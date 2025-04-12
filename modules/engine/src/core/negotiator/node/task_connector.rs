use std::{
    collections::{HashMap, HashSet},
    io::Read,
    sync::Arc,
};

use async_trait::async_trait;
use futures::FutureExt;
use parking_lot::Mutex;
use rand::{SeedableRng, seq::SliceRandom};
use rand_chacha::ChaCha20Rng;
use tokio::{
    sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc},
    task::JoinHandle,
};
use tracing::warn;

use omnius_core_base::sleeper::Sleeper;

use crate::{
    core::{
        session::{
            SessionConnector,
            model::{Session, SessionType},
        },
        util::{Terminable, VolatileHashSet},
    },
    model::NodeProfile,
    prelude::*,
};

use super::{HandshakeType, NodeFinderOption, NodeFinderRepo, SessionStatus};

#[derive(Clone)]
pub struct TaskConnector {
    inner: Inner,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

impl TaskConnector {
    pub fn new(
        sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
        session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
        session_connector: Arc<SessionConnector>,
        connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
        node_profile_repo: Arc<NodeFinderRepo>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOption,
    ) -> Self {
        let inner = Inner {
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
                    warn!(error_message = e.to_string(), "connect failed");
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }
}

#[async_trait]
impl Terminable for TaskConnector {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[derive(Clone)]
struct Inner {
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    session_connector: Arc<SessionConnector>,
    connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
    node_profile_repo: Arc<NodeFinderRepo>,
    option: NodeFinderOption,
}

impl Inner {
    async fn connect(&self) -> Result<()> {
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

        self.connected_node_profiles.lock().refresh();

        let mut connected_ids: HashSet<Vec<u8>> = {
            let v1: Vec<Vec<u8>> = self.connected_node_profiles.lock().iter().map(|n| n.id.to_owned()).collect();
            let v2: Vec<Vec<u8>> = self.sessions.read().await.iter().map(|n| n.0.to_owned()).collect();
            v1.into_iter().chain(v2.into_iter()).collect()
        };

        let node_profiles: Vec<NodeProfile> = self
            .node_profile_repo
            .fetch_node_profiles()
            .await?
            .into_iter()
            .filter(|n| !connected_ids.contains(&n.id))
            .collect();

        let mut rng = ChaCha20Rng::from_entropy();
        let node_profile = node_profiles
            .choose(&mut rng)
            .ok_or_else(|| Error::new(ErrorKind::NotFound).message("node profile is not found"))?;

        for addr in node_profile.addrs.iter() {
            if let Ok(session) = self.session_connector.connect(addr, &SessionType::NodeFinder).await {
                self.session_sender
                    .lock()
                    .await
                    .send((HandshakeType::Connected, session))
                    .await
                    .map_err(|e| Error::new(ErrorKind::UnexpectedError).source(e))?;
                self.connected_node_profiles.lock().insert(node_profile.clone());
            }
        }

        Ok(())
    }
}
