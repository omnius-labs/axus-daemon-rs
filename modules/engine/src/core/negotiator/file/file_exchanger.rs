use std::{collections::HashMap, path::PathBuf, sync::Arc};

use chrono::{Duration, Utc};
use parking_lot::Mutex;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, tsid::TsidProvider};

use crate::{
    core::{
        negotiator::NodeFinder,
        session::{SessionAccepter, SessionConnector},
        util::VolatileHashSet,
    },
    model::{AssetKey, NodeProfile},
    prelude::*,
};

use super::*;

#[allow(dead_code)]
pub struct FileExchanger {
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_finder: Arc<NodeFinder>,
    tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    option: FileExchangerOption,

    session_receiver: Arc<TokioMutex<mpsc::Receiver<SessionStatus>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<SessionStatus>>>,
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
    connected_node_profiles: Arc<Mutex<VolatileHashSet<Arc<NodeProfile>>>>,
    push_asset_keys: Arc<Mutex<Vec<AssetKey>>>,
    want_asset_keys: Arc<Mutex<Vec<AssetKey>>>,

    file_publisher: Arc<TokioMutex<Option<Arc<FilePublisher>>>>,
    file_subscriber: Arc<TokioMutex<Option<Arc<FileSubscriber>>>>,

    task_connectors: Arc<TokioMutex<Vec<Arc<TaskConnector>>>>,
}

#[derive(Debug, Clone)]
pub struct FileExchangerOption {
    #[allow(unused)]
    pub state_dir_path: PathBuf,
    pub max_connected_session_for_publish: usize,
    pub max_connected_session_for_subscribe: usize,
}

impl FileExchanger {
    #[allow(clippy::too_many_arguments)]
    #[allow(unused)]
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_finder: Arc<NodeFinder>,
        tsid_provider: Arc<Mutex<dyn TsidProvider + Send + Sync>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: FileExchangerOption,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(20);

        let v = Self {
            session_connector,
            session_accepter,
            node_finder,
            tsid_provider,
            clock: clock.clone(),
            sleeper,
            option,

            session_receiver: Arc::new(TokioMutex::new(rx)),
            session_sender: Arc::new(TokioMutex::new(tx)),
            sessions: Arc::new(TokioRwLock::new(HashMap::new())),
            connected_node_profiles: Arc::new(Mutex::new(VolatileHashSet::new(Duration::seconds(180), clock))),
            push_asset_keys: Arc::new(Mutex::new(vec![])),
            want_asset_keys: Arc::new(Mutex::new(vec![])),

            file_publisher: Arc::new(TokioMutex::new(None)),
            file_subscriber: Arc::new(TokioMutex::new(None)),

            task_connectors: Arc::new(TokioMutex::new(Vec::new())),
        };
        v.start().await;

        Ok(v)
    }

    async fn start(&self) -> Result<()> {
        {
            let state_dir_path = self.option.state_dir_path.join("file_publisher");
            let file_publisher = FilePublisher::new(&state_dir_path, self.tsid_provider.clone(), self.clock.clone(), self.sleeper.clone()).await?;
            self.file_publisher.lock().await.replace(file_publisher);
        }

        {
            let state_dir_path = self.option.state_dir_path.join("file_subscriber");
            let file_subscriber = FileSubscriber::new(&state_dir_path, self.tsid_provider.clone(), self.clock.clone(), self.sleeper.clone()).await?;
            self.file_subscriber.lock().await.replace(file_subscriber);
        }

        for _ in 0..3 {
            let task = TaskConnector::new(
                self.sessions.clone(),
                self.session_sender.clone(),
                self.session_connector.clone(),
                self.node_finder.clone(),
                self.file_publisher.clone(),
                self.file_subscriber.clone(),
                self.connected_node_profiles.clone(),
                self.clock.clone(),
                self.sleeper.clone(),
                self.option.clone(),
            )
            .await?;
            self.task_connectors.lock().await.push(task);
        }

        Ok(())
    }
}
