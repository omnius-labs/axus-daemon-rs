use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

use core_base::sleeper::Sleeper;
use futures::FutureExt;
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tokio::{
    sync::{mpsc, Mutex as TokioMutex, RwLock as TokioRwLock},
    task::JoinHandle,
};
use tracing::warn;

use crate::{
    model::NodeProfile,
    service::{
        session::{
            model::{Session, SessionType},
            SessionConnector,
        },
        util::VolatileHashSet,
    },
};

use super::{HandshakeType, NodeFinderOptions, NodeProfileRepo, SessionStatus};

#[derive(Clone)]
pub struct TaskConnector {
    inner: TaskConnectorInner,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

impl TaskConnector {
    pub fn new(
        sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
        session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
        session_connector: Arc<SessionConnector>,
        connected_node_profiles: Arc<StdMutex<VolatileHashSet<NodeProfile>>>,
        node_profile_repo: Arc<NodeProfileRepo>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let inner = TaskConnectorInner {
            sessions,
            session_sender,
            session_connector,
            connected_node_profiles,
            node_profile_repo,
            option,
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
                let res = inner.connect().await;
                if let Err(e) = res {
                    warn!("{:?}", e);
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }

    pub async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[derive(Clone)]
struct TaskConnectorInner {
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    session_connector: Arc<SessionConnector>,
    connected_node_profiles: Arc<StdMutex<VolatileHashSet<NodeProfile>>>,
    node_profile_repo: Arc<NodeProfileRepo>,
    option: NodeFinderOptions,
}

impl TaskConnectorInner {
    async fn connect(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .read()
            .await
            .iter()
            .filter(|(_, status)| status.handshake_type == HandshakeType::Connected)
            .count();
        if session_count >= self.option.max_connected_session_count {
            return Ok(());
        }

        self.connected_node_profiles.lock().unwrap().refresh();

        let mut rng = ChaCha20Rng::from_entropy();
        let node_profiles = self.node_profile_repo.get_node_profiles().await?;
        let node_profile = node_profiles.choose(&mut rng).ok_or(anyhow::anyhow!("Not found node_profile"))?;

        if self
            .sessions
            .read()
            .await
            .iter()
            .any(|(_, status)| status.node_profile.id == node_profile.id)
        {
            anyhow::bail!("Already connected 1");
        }

        if self.connected_node_profiles.lock().unwrap().contains(node_profile) {
            anyhow::bail!("Already connected 2");
        }

        for addr in node_profile.addrs.iter() {
            if let Ok(session) = self.session_connector.connect(addr, &SessionType::NodeFinder).await {
                self.session_sender.lock().await.send((HandshakeType::Connected, session)).await?;
                self.connected_node_profiles.lock().unwrap().insert(node_profile.clone());
            }
        }

        Ok(())
    }
}
