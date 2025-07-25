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
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    event_sender: tokio::sync::mpsc::UnboundedSender<TaskEncoderEvent>,

    task_encoder: Arc<TokioMutex<Option<Arc<TaskEncoder>>>>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
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
        let (event_sender, event_receiver) = tokio::sync::mpsc::unbounded_channel::<TaskEncoderEvent>();

        let task_encoder = TaskEncoder::new(
            file_publisher_repo.clone(),
            blocks_storage.clone(),
            event_receiver,
            tsid_provider.clone(),
            clock.clone(),
            sleeper.clone(),
        )
        .await?;

        let v = Arc::new(Self {
            file_publisher_repo,
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
}

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    #[tokio::test]
    pub async fn simple_test() -> TestResult {
        Ok(())
    }
}
