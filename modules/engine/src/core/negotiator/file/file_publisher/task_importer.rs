use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::Utc;
use futures::FutureExt;
use parking_lot::Mutex;
use rand::{SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha20Rng;
use tokio::{
    sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc},
    task::JoinHandle,
};
use tracing::warn;

use omnius_core_base::{clock::Clock, sleeper::Sleeper, terminable::Terminable, tsid::TsidProvider};

use crate::core::{storage::KeyValueFileStorage, util::VolatileHashSet};

use super::FilePublisherRepo;

#[derive(Clone)]
pub struct TaskImporter {
    inner: Inner,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

impl TaskImporter {
    pub fn new(
        file_publisher_repo: Arc<FilePublisherRepo>,
        blocks_storage: Arc<TokioMutex<KeyValueFileStorage>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Self {
        let inner = Inner {
            file_publisher_repo,
            blocks_storage,
            clock,
        };
        Self {
            inner,
            sleeper,
            join_handle: Arc::new(TokioMutex::new(None)),
        }
    }

    pub async fn run(&self) {
        let sleeper = self.sleeper.clone();
        let inner = self.inner.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                sleeper.sleep(std::time::Duration::from_secs(1)).await;
                let res = inner.import().await;
                if let Err(e) = res {
                    warn!(error_message = e.to_string(), "connect failed");
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }
}

#[async_trait]
impl Terminable for TaskImporter {
    async fn terminate(&self) -> anyhow::Result<()> {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct Inner {
    file_publisher_repo: Arc<FilePublisherRepo>,
    blocks_storage: Arc<TokioMutex<KeyValueFileStorage>>,

    clock: Arc<dyn Clock<Utc> + Send + Sync>,
}

impl Inner {}
