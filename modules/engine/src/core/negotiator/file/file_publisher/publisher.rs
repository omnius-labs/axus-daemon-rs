use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt as _;
use parking_lot::Mutex;
use tokio::{
    fs::File,
    io::{AsyncRead, AsyncReadExt, BufReader},
    sync::Mutex as TokioMutex,
    task::JoinHandle,
};
use tokio_stream::{StreamExt, wrappers::UnboundedReceiverStream};
use tracing::warn;

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};
use omnius_core_rocketpack::RocketMessage;

use crate::{
    core::{negotiator::file::model::PublishedUncommittedFile, storage::KeyValueFileStorage, util::Terminable},
    prelude::*,
};

use super::{FilePublisherRepo, MerkleLayer, PublishedCommittedBlock, PublishedCommittedFile, PublishedUncommittedBlock};

#[allow(unused)]
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    event_sender: tokio::sync::mpsc::UnboundedSender<FilePublisherEvent>,
    current_import_file_id: Arc<Mutex<Option<String>>>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

enum FilePublisherEvent {
    UncommittedFileDeleted { file_id: String },
}

#[async_trait]
impl Terminable for FilePublisher {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[allow(unused)]
impl FilePublisher {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        file_publisher_repo: Arc<FilePublisherRepo>,
        blocks_storage: Arc<KeyValueFileStorage>,

        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel::<FilePublisherEvent>();

        let v = Arc::new(Self {
            file_publisher_repo,
            blocks_storage,

            current_import_file_id: Arc::new(Mutex::new(None)),
            event_sender,

            tsid_provider,
            clock,
            sleeper,

            join_handle: Arc::new(TokioMutex::new(None)),
        });
        v.clone().start(event_receiver).await?;

        Ok(v)
    }

    async fn start(self: Arc<Self>, mut events: tokio::sync::mpsc::UnboundedReceiver<FilePublisherEvent>) -> Result<()> {
        let current_import_file_id = self.current_import_file_id.clone();
        let mut events = UnboundedReceiverStream::new(events).filter(move |v| match v {
            FilePublisherEvent::UncommittedFileDeleted { file_id } => {
                let current_file_id = current_import_file_id.lock();
                current_file_id.as_ref() == Some(file_id)
            }
        });

        let join_handle = self.join_handle.clone();

        *join_handle.lock().await = Some(tokio::spawn(async move {
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

                    *self.current_import_file_id.lock() = Some(uncommitted_file.id.clone());

                    // importing_file_idにセットする前に削除される可能性を考慮して、再度取得する
                    let uncommitted_file = match self.file_publisher_repo.fetch_uncommitted_file(&uncommitted_file.id).await {
                        Ok(uncommitted_file) => uncommitted_file,
                        Err(e) => {
                            warn!(error_message = e.to_string(), "fetch uncommitted file failed",);
                            return;
                        }
                    };
                    let Some(uncommitted_file) = uncommitted_file else {
                        return;
                    };

                    let result = self.import(uncommitted_file).await;
                    if let Err(e) = result {
                        warn!(error_message = e.to_string(), "import failed",);
                    }
                };

                tokio::select! {
                    _ = callback => {}
                    Some(_) = events.next() => {
                        warn!("Import task interrupted: Uncommitted file deleted while potentially being imported.");
                        *self.current_import_file_id.lock() = None;
                    }
                };
            }
        }));

        Ok(())
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
                let path = Self::gen_uncommitted_block_path(&uncommitted_file.id, &uncommitted_block.block_hash);
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
            let old_key = Self::gen_uncommitted_block_path(&uncommitted_file.id, &uncommitted_block.block_hash);
            let new_key = Self::gen_committed_block_path(&root_hash, &uncommitted_block.block_hash);
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

            let path = Self::gen_uncommitted_block_path(file_id, &block_hash);
            self.blocks_storage.put_value(path.as_str(), block).await?;

            index += 1;
        }

        Ok(uncommitted_blocks)
    }

    fn gen_uncommitted_block_path(id: &str, block_hash: &OmniHash) -> String {
        format!("U/{}/{}", id, block_hash)
    }

    fn gen_committed_block_path(root_hash: &OmniHash, block_hash: &OmniHash) -> String {
        format!("C/{}/{}", root_hash, block_hash)
    }
}
