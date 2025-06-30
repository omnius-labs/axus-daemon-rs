use core::task;
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
use tokio_util::bytes::Bytes;
use tracing::warn;

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};
use omnius_core_rocketpack::RocketMessage;

use crate::{
    core::{negotiator::file::model::PublishedUncommittedFile, storage::KeyValueFileStorage, util::Terminable},
    prelude::*,
};

use super::*;

#[allow(unused)]
pub struct FileSubscriber {
    file_subscriber_repo: Arc<FileSubscriberRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    event_sender: tokio::sync::mpsc::UnboundedSender<TaskDecoderEvent>,

    task_encoder: Arc<TokioMutex<Option<Arc<TaskDecoder>>>>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[async_trait]
impl Terminable for FileSubscriber {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[allow(unused)]
impl FileSubscriber {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        file_publisher_repo: Arc<FileSubscriberRepo>,
        blocks_storage: Arc<KeyValueFileStorage>,

        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Result<Arc<Self>> {
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel::<TaskDecoderEvent>();

        let task_encoder = TaskDecoder::new(
            file_publisher_repo.clone(),
            blocks_storage.clone(),
            event_receiver,
            tsid_provider.clone(),
            clock.clone(),
            sleeper.clone(),
        )
        .await?;

        let v = Arc::new(Self {
            file_subscriber_repo: file_publisher_repo,
            blocks_storage,

            event_sender,

            task_encoder: Arc::new(TokioMutex::new(Some(task_encoder))),

            tsid_provider,
            clock,
            sleeper,

            join_handle: Arc::new(TokioMutex::new(None)),
        });

        Ok(v)
    }

    pub async fn write_block(&self, root_hash: &OmniHash, block_hash: &OmniHash, value: &Bytes) -> Result<()> {
        let blocks = self.file_subscriber_repo.fetch_blocks(root_hash, block_hash).await?;
        if blocks.is_empty() {
            return Ok(());
        }

        let key = gen_block_path(root_hash, block_hash);
        self.blocks_storage.put_value(&key, value).await?;

        let new_blocks: Vec<SubscribedBlock> = blocks.into_iter().map(|n| SubscribedBlock { downloaded: true, ..n }).collect();
        self.file_subscriber_repo

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    #[tokio::test]
    pub async fn simple_test() -> TestResult {
        Ok(())
    }
}
