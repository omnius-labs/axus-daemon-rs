use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt as _;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Mutex as TokioMutex,
    task::JoinHandle,
};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, terminable::Terminable};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};

use crate::service::storage::BlobStorage;

use super::{file_publisher_repo::FilePublisherRepo, PublishedBlock};

#[allow(unused)]
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blob_storage: Arc<TokioMutex<BlobStorage>>,

    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[allow(unused)]
impl FilePublisher {
    pub async fn publish_file<R>(&self, reader: &mut R, file_name: &str, block_size: u64) -> anyhow::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut buf = vec![0; block_size as usize];
        loop {
            let n = reader.read_exact(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.blob_storage.lock().await.put(file_name.as_bytes(), &buf[..n])?;
        }
        todo!()
    }

    async fn import_bytes<R>(&self, id: &str, reader: &mut R, max_block_size: u64, depth: u32) -> anyhow::Result<Vec<PublishedBlock>>
    where
        R: AsyncRead + Unpin,
    {
        let mut blocks: Vec<PublishedBlock> = Vec::new();
        let mut index = 0;

        let mut buf = vec![0; max_block_size as usize];
        loop {
            let size = reader.read_exact(&mut buf).await?;
            if size == 0 {
                break;
            }

            let block = &buf[..size];
            let block_hash = OmniHash::compute_hash(OmniHashAlgorithmType::Sha3_256, block);

            let published_block = PublishedBlock {
                root_hash: OmniHash::default(),
                block_hash: block_hash.clone(),
                depth,
                index,
            };
            blocks.push(published_block);

            index += 1;

            self.write_uncommitted_block(id, &block_hash, block).await?;
        }

        Ok(blocks)
    }

    async fn write_uncommitted_block(&self, id: &str, block_hash: &OmniHash, value: &[u8]) -> anyhow::Result<()> {
        let path = Self::gen_uncommitted_block_path(id, block_hash);
        self.blob_storage.lock().await.put(path.as_bytes(), value)?;
        Ok(())
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
    type Error = anyhow::Error;
    async fn terminate(&self) -> anyhow::Result<()> {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }

        Ok(())
    }
}
