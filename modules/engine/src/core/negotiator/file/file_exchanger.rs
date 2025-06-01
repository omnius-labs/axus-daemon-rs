use std::{path::PathBuf, sync::Arc};

use chrono::Utc;
use parking_lot::Mutex;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};

use crate::{core::storage::KeyValueFileStorage, prelude::*};

use super::*;

#[allow(dead_code)]
pub struct FileExchanger {
    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    option: FileExchangerOption,

    file_publisher: Arc<TokioMutex<Option<Arc<FilePublisher>>>>,
}

#[derive(Debug, Clone)]
pub struct FileExchangerOption {
    pub state_dir_path: PathBuf,
}

impl FileExchanger {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: FileExchangerOption,
    ) -> Result<Self> {
        let v = Self {
            tsid_provider,
            clock,
            sleeper,
            option,

            file_publisher: Arc::new(TokioMutex::new(None)),
        };
        v.start().await;

        Ok(v)
    }

    async fn start(&self) -> Result<()> {
        {
            let file_publisher_dir = self.option.state_dir_path.join("file_publisher");
            let file_publisher_repo = Arc::new(FilePublisherRepo::new(file_publisher_dir.join("repo"), self.clock.clone()).await?);
            let blocks_storage = Arc::new(KeyValueFileStorage::new(file_publisher_dir.join("blocks")).await?);
            let file_publisher = FilePublisher::new(
                file_publisher_repo,
                blocks_storage,
                self.tsid_provider.clone(),
                self.clock.clone(),
                self.sleeper.clone(),
            )
            .await?;
            self.file_publisher.lock().await.replace(file_publisher);
        }

        Ok(())
    }
}
