use std::sync::Arc;

use chrono::Utc;
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    sync::Mutex as TokioMutex,
};

use omnius_core_base::{clock::Clock, sleeper::Sleeper};
use omnius_core_omnikit::model::OmniHash;

use crate::service::storage::BlobStorage;

use super::file_publisher_repo::FilePublisherRepo;

#[allow(unused)]
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blob_storage: Arc<TokioMutex<BlobStorage>>,

    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
}

#[allow(unused)]
impl FilePublisher {
    pub async fn publish_file<R>(
        &self,
        reader: &mut R,
        file_name: &str,
        block_size: u64,
    ) -> anyhow::Result<Self>
    where
        R: AsyncRead + Unpin,
    {
        let mut buf = vec![0; block_size as usize];
        loop {
            let n = reader.read_exact(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.blob_storage
                .lock()
                .await
                .put(file_name.as_bytes(), &buf[..n])?;
        }
        todo!()
    }

    async fn write_uncommitted_block(id: &str) {}

    fn gen_uncommitted_block_path(id: &str, block_hash: &OmniHash) -> String {
        format!("U/{}/{}", id, block_hash)
    }
}
