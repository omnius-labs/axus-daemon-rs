use std::io::Cursor;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt as _;
use parking_lot::Mutex;
use tokio::{
    io::{AsyncRead, AsyncReadExt, BufReader},
    sync::Mutex as TokioMutex,
    task::JoinHandle,
};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};
use omnius_core_omnikit::model::{OmniHash, OmniHashAlgorithmType};
use omnius_core_rocketpack::RocketMessage;

use crate::{
    core::{storage::KeyValueFileStorage, util::Terminable},
    prelude::*,
};

use super::{FilePublisherRepo, MerkleLayer, PublishedCommittedBlock, PublishedCommittedFile, PublishedUncommittedBlock, TaskImporter};

#[allow(unused)]
pub struct FilePublisher {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<KeyValueFileStorage>,

    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    task_importer: Arc<TokioMutex<Option<Arc<TaskImporter>>>>,
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
    ) -> Result<Self> {
        let result = Self {
            file_publisher_repo,
            blocks_storage,
            tsid_provider,
            clock,
            sleeper,
            task_importer: Arc::new(TokioMutex::new(None)),
        };
        result.run().await?;

        Ok(result)
    }
    async fn run(&self) -> Result<()> {
        let task = TaskImporter::new(
            self.file_publisher_repo.clone(),
            self.blocks_storage.clone(),
            self.tsid_provider.clone(),
            self.clock.clone(),
            self.sleeper.clone(),
        )
        .await?;
        self.task_importer.lock().await.replace(task);

        Ok(())
    }
}

#[async_trait]
impl Terminable for FilePublisher {
    async fn terminate(&self) {
        {
            let mut task_importer = self.task_importer.lock().await;
            if let Some(task_importer) = task_importer.take() {
                task_importer.terminate().await;
            }
        }
    }
}

pub fn gen_uncommitted_block_path(id: &str, block_hash: &OmniHash) -> String {
    format!("U/{}/{}", id, block_hash)
}

pub fn gen_committed_block_path(root_hash: &OmniHash, block_hash: &OmniHash) -> String {
    format!("C/{}/{}", root_hash, block_hash)
}
