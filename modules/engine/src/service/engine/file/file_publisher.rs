use std::sync::Arc;

use tokio::{io::AsyncReadExt, sync::Mutex as TokioMutex};

use crate::storage::BlobStorage;

#[allow(unused)]
pub struct FilePublisher {
    blob_storage: Arc<TokioMutex<BlobStorage>>,
}

#[allow(unused)]
impl FilePublisher {
    pub async fn publish_file(self, mut reader: &mut (dyn tokio::io::AsyncRead + Unpin), file_name: &str, block_size: u64) -> anyhow::Result<Self> {
        let mut buf = vec![0; block_size as usize];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            self.blob_storage.lock().await.put(file_name.as_bytes(), &buf[..n])?;
        }
        todo!()
    }

    // pub fn gen_key_by_tmp
}
