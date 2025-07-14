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
use tracing::warn;

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};
use omnius_core_rocketpack::RocketMessage;

use crate::{
    core::{
        negotiator::file::model::PublishedUncommittedFile,
        storage::KeyValueFileStorage,
        util::{EventListener, Terminable},
    },
    prelude::*,
};

use super::*;

#[allow(unused)]
pub struct TaskEncoder {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    current_encoding_file_id: Arc<Mutex<Option<String>>>,
    publish_event_listener: Arc<EventListener>,
    cancel_event_listener: Arc<EventListener>,

    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[async_trait]
impl Terminable for TaskEncoder {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[allow(unused)]
impl TaskEncoder {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        file_publisher_repo: Arc<FilePublisherRepo>,
        blocks_storage: Arc<KeyValueFileStorage>,

        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let v = Arc::new(Self {
            file_publisher_repo,
            blocks_storage,

            tsid_provider,
            clock,
            sleeper,

            current_encoding_file_id: Arc::new(Mutex::new(None)),
            publish_event_listener: Arc::new(EventListener::new()),
            cancel_event_listener: Arc::new(EventListener::new()),

            join_handle: Arc::new(TokioMutex::new(None)),
        });
        v.clone().start().await?;

        Ok(v)
    }

    pub async fn publish(&self, file_path: &str, file_name: &str, block_size: u32, attrs: Option<&str>, priority: i64) -> Result<()> {
        let id = self.tsid_provider.lock().create().to_string();
        let now = self.clock.now();

        let file = PublishedUncommittedFile {
            id,
            file_path: file_path.to_string(),
            file_name: file_name.to_string(),
            block_size,
            attrs: attrs.map(|n| n.to_string()),
            priority,
            status: PublishedUncommittedFileStatus::Pending,
            failed_reason: None,
            created_at: now,
            updated_at: now,
        };

        self.file_publisher_repo.insert_uncommitted_file(&file).await?;

        Ok(())
    }

    pub async fn cancel(&self, file_id: &str) -> Result<()> {
        let guard = self.current_encoding_file_id.lock();

        let Some(current_encoding_file_id) = guard.as_ref() else {
            return Ok(());
        };

        if current_encoding_file_id != file_id {
            return Ok(());
        }

        self.cancel_event_listener.notify();

        Ok(())
    }

    async fn start(self: Arc<Self>) -> Result<()> {
        let cancel_event_listener = self.cancel_event_listener.clone();

        let join_handle = self.join_handle.clone();
        *join_handle.lock().await = Some(tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok(next) = self.encode() => {
                        if next {
                            continue;
                        }
                    }
                    _ = cancel_event_listener.wait() => {
                        info!("import task canceled");
                    }
                };

                self.publish_event_listener.wait().await;
            }
        }));

        Ok(())
    }

    async fn encode(&self) -> Result<bool> {
        let Some(uncommitted_file) = self.pickup().await else {
            warn!("no uncommitted file to import.");
            return Ok(false);
        };

        let mut file = File::open(uncommitted_file.file_path.as_str()).await?;

        let mut all_uncommitted_blocks: Vec<PublishedUncommittedBlock> = Vec::new();
        let mut current_block_hashes: Vec<OmniHash> = Vec::new();

        let mut uncommitted_blocks = self.encode_bytes(&uncommitted_file.id, &mut file, uncommitted_file.block_size, 0).await?;
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
                .encode_bytes(&uncommitted_file.id, &mut reader, uncommitted_file.block_size, depth)
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
                self.file_publisher_repo.delete_uncommitted_file(&uncommitted_file.id).await?;

                return Ok(true);
            }

            let new_committed_file = PublishedCommittedFile {
                file_name: uncommitted_file.file_name.clone(),
                ..committed_file
            };

            for uncommitted_block in self.file_publisher_repo.fetch_uncommitted_blocks(&uncommitted_file.id).await? {
                let path = gen_uncommitted_block_path(&uncommitted_file.id, &uncommitted_block.block_hash);
                self.blocks_storage.delete_key(path.as_str()).await?;
            }

            self.file_publisher_repo
                .commit_file_without_blocks(&new_committed_file, &uncommitted_file.id)
                .await?;

            return Ok(true);
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

        self.file_publisher_repo
            .commit_file_with_blocks(&committed_file, &committed_blocks, &uncommitted_file.id)
            .await?;

        Ok(true)
    }

    async fn pickup(&self) -> Option<PublishedUncommittedFile> {
        let uncommitted_file = match self.file_publisher_repo.fetch_uncommitted_file_next().await {
            Ok(uncommitted_file) => uncommitted_file,
            Err(e) => {
                warn!(error_message = e.to_string(), "fetch uncommitted file failed",);
                return None;
            }
        };
        let Some(uncommitted_file) = uncommitted_file else {
            *self.current_encoding_file_id.lock() = None;
            return None;
        };

        *self.current_encoding_file_id.lock() = Some(uncommitted_file.id.clone());

        // current_import_file_idがセットされる前に状態が変わる可能性があるため、再度取得する
        let uncommitted_file = match self.file_publisher_repo.fetch_uncommitted_file(&uncommitted_file.id).await {
            Ok(uncommitted_file) => uncommitted_file,
            Err(e) => {
                warn!(error_message = e.to_string(), "fetch uncommitted file failed",);
                return None;
            }
        };
        let Some(uncommitted_file) = uncommitted_file else {
            *self.current_encoding_file_id.lock() = None;
            return None;
        };

        Some(uncommitted_file)
    }

    async fn encode_bytes<R>(&self, file_id: &str, reader: &mut R, max_block_size: u32, rank: u32) -> Result<Vec<PublishedUncommittedBlock>>
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
