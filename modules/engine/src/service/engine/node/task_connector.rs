use std::sync::Arc;

use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tokio::{
    select,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeProfile,
    service::{
        session::{model::SessionType, SessionConnector},
        util::VolatileHashSet,
    },
};

use super::{Communicator, HandshakeType, NodeFinderOptions, NodeRefRepo, SessionStatus};

#[derive(Clone)]
pub struct TaskConnector {
    pub sessions: Arc<Mutex<Vec<SessionStatus>>>,
    pub session_sender: Arc<Mutex<mpsc::Sender<SessionStatus>>>,
    pub session_connector: Arc<SessionConnector>,
    pub connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
    pub node_profile_repo: Arc<NodeRefRepo>,
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
                        let _ = self.connect().await;
                    }
                } => {}
            }
        })
    }

    async fn connect(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .lock()
            .await
            .iter()
            .filter(|n| n.handshake_type == HandshakeType::Connected)
            .count();
        if session_count >= self.option.max_connected_session_count {
            return Ok(());
        }

        self.connected_node_profiles.lock().await.refresh();

        let mut rng = ChaCha20Rng::from_entropy();
        let node_profiles = self.node_profile_repo.get_node_profiles().await?;
        let node_profile = node_profiles.choose(&mut rng).ok_or(anyhow::anyhow!("Not found node_profile"))?;

        if self.sessions.lock().await.iter().any(|n| n.node_profile == *node_profile) {
            anyhow::bail!("Already connected");
        }

        if self.connected_node_profiles.lock().await.contains(node_profile) {
            anyhow::bail!("Already connected");
        }

        for addr in node_profile.addrs.iter() {
            let session = self.session_connector.connect(addr, &SessionType::NodeFinder).await?;
            let (id, node_profile) = Communicator::handshake(&session).await?;
            self.session_sender
                .lock()
                .await
                .send(SessionStatus {
                    id,
                    handshake_type: HandshakeType::Connected,
                    node_profile,
                    session,
                })
                .await?;
        }

        self.connected_node_profiles.lock().await.insert(node_profile.clone());

        Ok(())
    }
}
