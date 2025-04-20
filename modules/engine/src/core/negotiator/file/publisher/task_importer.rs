use std::{
    collections::HashMap,
    io::Cursor,
    sync::{Arc, atomic::AtomicBool},
};

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use omnius_core_rocketpack::RocketMessage;
use parking_lot::Mutex;
use rand::{SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha20Rng;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, BufReader},
    sync::{Mutex as TokioMutex, Notify, RwLock as TokioRwLock, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};

use crate::{
    core::{
        negotiator::file::model::PublishedUncommittedFile,
        storage::KeyValueFileStorage,
        util::{Terminable, VolatileHashSet},
    },
    prelude::*,
};

use super::{
    FilePublisherRepo, MerkleLayer, PublishedCommittedBlock, PublishedCommittedFile, PublishedUncommittedBlock, gen_committed_block_path,
    gen_uncommitted_block_path,
};

#[derive(Clone)]
pub struct TaskImporter {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,
    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    import_cancel_sender: tokio::sync::watch::Sender<()>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[async_trait]
impl Terminable for TaskImporter {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

impl TaskImporter {
    pub async fn new(
        file_publisher_repo: Arc<FilePublisherRepo>,
        blocks_storage: Arc<KeyValueFileStorage>,
        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let (import_cancel_sender, import_cancel_receiver) = tokio::sync::watch::channel(());

        let v = Arc::new(Self {
            file_publisher_repo,
            blocks_storage,
            tsid_provider,
            clock,
            sleeper,
            import_cancel_sender,
            join_handle: Arc::new(TokioMutex::new(None)),
        });
        let join_handle = v.clone().start(import_cancel_receiver).await?;
        *v.join_handle.lock().await = Some(join_handle);

        Ok(v)
    }

    async fn start(self: Arc<Self>, mut cancel_rx: tokio::sync::watch::Receiver<()>) -> Result<JoinHandle<()>> {
        Ok(tokio::spawn(async move {
            let mut current_file_id: Option<String> = None;

            loop {
                let callback = async {
                    self.sleeper.sleep(std::time::Duration::from_secs(1)).await;

                    let uncommitted_file = match self.file_publisher_repo.fetch_uncommitted_file_next().await {
                        Ok(uncommitted_file) => uncommitted_file,
                        Err(e) => {
                            warn!(error_message = e.to_string(), "fetch uncommitted file failed",);
                            return;
                        }
                    };

                    let Some(uncommitted_file) = uncommitted_file else {
                        return;
                    };

                    current_file_id = Some(uncommitted_file.id.clone());
                    let result = self.import(uncommitted_file).await;

                    if let Err(e) = result {
                        warn!(error_message = e.to_string(), "import failed",);
                    }
                };

                let exists = async |file_id: &str| match self.file_publisher_repo.fetch_uncommitted_file(file_id).await {
                    Ok(v) => v.is_some(),
                    Err(e) => {
                        warn!(error_message = e.to_string(), "fetch uncommitted file failed",);
                        false
                    }
                };

                tokio::select! {
                    _ = callback => {}
                    _ = cancel_rx.changed() => {
                        let Some(current_file_id) = current_file_id.as_ref() else {
                            continue;
                        };

                        if !exists(current_file_id.as_str()).await {
                            break;
                        }

                        continue;
                    }
                };
            }
        }))
    }

    async fn import(&self, uncommitted_file: PublishedUncommittedFile) -> Result<()> {
        let mut file = File::open(uncommitted_file.file_path.as_str()).await?;

        let mut all_uncommitted_blocks: Vec<PublishedUncommittedBlock> = Vec::new();
        let mut current_block_hashes: Vec<OmniHash> = Vec::new();

        let mut uncommitted_blocks = self.import_bytes(&uncommitted_file.id, &mut file, uncommitted_file.block_size, 0).await?;
        all_uncommitted_blocks.extend(uncommitted_blocks.iter().cloned());
        current_block_hashes.extend(uncommitted_blocks.iter().map(|block| block.block_hash.clone()));

        let mut depth = 1;
        loop {
            let merkle_layer = MerkleLayer {
                rank: depth,
                hashes: current_block_hashes.clone(),
            };

            let bytes = merkle_layer.export()?;
            let bytes_slice = bytes.as_ref();
            let cursor = Cursor::new(bytes_slice);
            let mut reader = BufReader::new(cursor);

            uncommitted_blocks = self
                .import_bytes(&uncommitted_file.id, &mut reader, uncommitted_file.block_size, depth)
                .await?;
            all_uncommitted_blocks.extend(uncommitted_blocks.iter().cloned());
            current_block_hashes = uncommitted_blocks.iter().map(|block| block.block_hash.clone()).collect();

            if uncommitted_blocks.len() == 1 {
                break;
            }

            depth += 1;
        }

        let root_hash = current_block_hashes.pop().unwrap();

        if let Some(committed_file) = self.file_publisher_repo.fetch_committed_file(&root_hash).await? {
            if committed_file.file_name == uncommitted_file.file_name {
                return Ok(());
            }

            let new_committed_file = PublishedCommittedFile {
                file_name: uncommitted_file.file_name.clone(),
                ..committed_file
            };

            self.file_publisher_repo.insert_committed_file(&new_committed_file).await?;

            for uncommitted_block in self.file_publisher_repo.fetch_uncommitted_blocks(&uncommitted_file.id).await? {
                let path = gen_uncommitted_block_path(&uncommitted_file.id, &uncommitted_block.block_hash);
                self.blocks_storage.delete_key(path.as_str()).await?;
            }

            self.file_publisher_repo
                .delete_uncommitted_blocks_by_file_id(&uncommitted_file.id)
                .await?;

            return Ok(());
        }

        let now = self.clock.now();

        let committed_file = PublishedCommittedFile {
            root_hash: root_hash.clone(),
            file_name: uncommitted_file.file_name.clone(),
            block_size: uncommitted_file.block_size,
            attrs: uncommitted_file.attrs.clone(),
            created_at: now,
            updated_at: now,
        };
        let committed_blocks = all_uncommitted_blocks
            .iter()
            .map(|block| PublishedCommittedBlock {
                root_hash: root_hash.clone(),
                block_hash: block.block_hash.clone(),
                rank: block.rank,
                index: block.index,
            })
            .collect::<Vec<_>>();

        for uncommitted_block in all_uncommitted_blocks {
            let old_key = gen_uncommitted_block_path(&uncommitted_file.id, &uncommitted_block.block_hash);
            let new_key = gen_committed_block_path(&root_hash, &uncommitted_block.block_hash);
            self.blocks_storage.rename_key(old_key.as_str(), new_key.as_str()).await?;
        }

        self.file_publisher_repo.insert_committed_file(&committed_file).await?;
        self.file_publisher_repo.insert_or_ignore_committed_blocks(&committed_blocks).await?;

        self.file_publisher_repo.delete_uncommitted_file(&uncommitted_file.id).await?;
        self.file_publisher_repo
            .delete_uncommitted_blocks_by_file_id(&uncommitted_file.id)
            .await?;

        Ok(())
    }

    async fn import_bytes<R>(&self, file_id: &str, reader: &mut R, max_block_size: u32, rank: u32) -> Result<Vec<PublishedUncommittedBlock>>
    where
        R: AsyncRead + Unpin,
    {
        let mut uncommitted_blocks: Vec<PublishedUncommittedBlock> = Vec::new();
        let mut index = 0;

        let mut buf = vec![0; max_block_size as usize];
        loop {
            let size = reader.read_exact(&mut buf).await?;
            if size == 0 {
                break;
            }

            let block = &buf[..size];
            let block_hash = OmniHash::compute_hash(OmniHashAlgorithmType::Sha3_256, block);

            let uncommitted_block = PublishedUncommittedBlock {
                file_id: file_id.to_string(),
                block_hash: block_hash.clone(),
                rank,
                index,
            };
            self.file_publisher_repo.insert_or_ignore_uncommitted_block(&uncommitted_block).await?;
            uncommitted_blocks.push(uncommitted_block);

            let path = gen_uncommitted_block_path(file_id, &block_hash);
            self.blocks_storage.put_value(path.as_str(), block).await?;

            index += 1;
        }

        Ok(uncommitted_blocks)
    }
}
