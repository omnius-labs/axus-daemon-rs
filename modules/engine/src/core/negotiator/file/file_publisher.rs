use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt as _;
use omnius_core_rocketpack::RocketMessage;
use parking_lot::Mutex;
use std::io::Cursor;
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    sync::Mutex as TokioMutex,
    task::JoinHandle,
};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, terminable::Terminable, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};

use crate::core::storage::KeyValueFileStorage;

use super::{FilePublisherRepo, MerkleLayer, PublishedCommittedBlock, PublishedUncommittedBlock};

#[allow(unused)]
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<TokioMutex<KeyValueFileStorage>>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[allow(unused)]
impl FilePublisher {
    async fn import_file<R>(&self, reader: &mut R, file_name: &str, block_size: u64) -> anyhow::Result<()>
    where
        R: AsyncRead + Unpin,
    {
        let file_id = self.tsid_provider.lock().create().to_string();

        let mut all_blocks: Vec<PublishedUncommittedBlock> = Vec::new();
        let mut current_block_hashes: Vec<OmniHash> = Vec::new();

        let mut blocks = self.import_bytes(&file_id, reader, block_size, 0).await?;
        all_blocks.extend(blocks.iter().cloned());
        current_block_hashes.extend(blocks.iter().map(|block| block.block_hash.clone()));

        let mut depth = 0;
        loop {
            let merkle_layer = MerkleLayer {
                rank: depth,
                hashes: current_block_hashes.clone(),
            };

            let bytes = merkle_layer.export()?;
            let bytes_slice = bytes.as_ref();
            let cursor = Cursor::new(bytes_slice);
            let mut reader = BufReader::new(cursor);

            blocks = self.import_bytes(&file_id, &mut reader, block_size, depth + 1).await?;
            all_blocks.extend(blocks.iter().cloned());
            current_block_hashes = blocks.iter().map(|block| block.block_hash.clone()).collect();

            if blocks.len() == 1 {
                break;
            }

            depth += 1;
        }

        let root_hash = current_block_hashes.pop().unwrap();

        Ok(())
    }

    async fn import_bytes<R>(&self, file_id: &str, reader: &mut R, max_block_size: u64, depth: u32) -> anyhow::Result<Vec<PublishedUncommittedBlock>>
    where
        R: AsyncRead + Unpin,
    {
        let mut blocks: Vec<PublishedUncommittedBlock> = Vec::new();
        let mut index = 0;

        let mut buf = vec![0; max_block_size as usize];
        loop {
            let size = reader.read_exact(&mut buf).await?;
            if size == 0 {
                break;
            }

            let block = &buf[..size];
            let block_hash = OmniHash::compute_hash(OmniHashAlgorithmType::Sha3_256, block);

            let published_block = PublishedUncommittedBlock {
                file_id: file_id.to_string(),
                block_hash: block_hash.clone(),
                depth,
                index,
            };
            blocks.push(published_block);

            self.file_publisher_repo.put_uncommitted_block(file_id, &block_hash, depth, index).await?;

            let path = Self::gen_uncommitted_block_path(file_id, &block_hash);
            self.blocks_storage.lock().await.put_value(path.as_str(), block).await?;

            index += 1;
        }

        Ok(blocks)
    }

    fn gen_uncommitted_block_path(id: &str, block_hash: &OmniHash) -> String {
        format!("U/{}/{}", id, block_hash)
    }

    fn gen_committed_block_path(root_hash: &OmniHash, block_hash: &OmniHash) -> String {
        format!("C/{}/{}", root_hash, block_hash)
    }
}

#[async_trait]
impl Terminable for FilePublisher {
    async fn terminate(&self) -> anyhow::Result<()> {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }

        Ok(())
    }
}
