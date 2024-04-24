use std::sync::{Arc, Mutex as StdMutex};

use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tokio::{
    select,
    sync::{mpsc, Mutex as TokioMutex, RwLock as TokioRwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
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
    pub sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
    pub session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    pub session_connector: Arc<SessionConnector>,
    pub connected_node_profiles: Arc<StdMutex<VolatileHashSet<NodeProfile>>>,
    pub node_profile_repo: Arc<NodeProfileRepo>,
    pub option: NodeFinderOptions,
}

impl TaskConnector {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let res = self.connect().await;
                        if let Err(e) = res {
                            warn!("{:?}", e);
                        }
                    }
                } => {}
            }
        })
    }

    async fn connect(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .read()
            .await
            .iter()
            .filter(|n| n.handshake_type == HandshakeType::Connected)
            .count();
        if session_count >= self.option.max_connected_session_count {
            return Ok(());
        }

        self.connected_node_profiles.lock().unwrap().refresh();

        let mut rng = ChaCha20Rng::from_entropy();
        let node_profiles = self.node_profile_repo.get_node_profiles().await?;
        let node_profile = node_profiles.choose(&mut rng).ok_or(anyhow::anyhow!("Not found node_profile"))?;

        if self.sessions.read().await.iter().any(|n| n.node_profile.id == node_profile.id) {
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
