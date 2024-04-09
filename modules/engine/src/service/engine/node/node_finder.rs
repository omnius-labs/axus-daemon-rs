use std::sync::Arc;

use chrono::Duration;
use core_base::clock::SystemClock;
use futures::future::{join_all, JoinAll};
use rand::{seq::SliceRandom, RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sqlx::types::chrono::Utc;
use tokio::{select, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeProfile,
    service::{
        session::{
            model::{Session, SessionType},
            SessionAccepter, SessionConnector,
        },
        util::VolatileHashSet,
    },
};

use super::{Communicator, NodeRefFetcher, NodeRefRepo};

#[allow(dead_code)]
pub struct NodeFinder {
    id: Arc<Vec<u8>>,
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_profile_repo: Arc<NodeRefRepo>,
    node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeFinderOptions,
    cancellation_token: CancellationToken,
    sessions: Arc<Mutex<Vec<SessionStatus>>>,
    connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
    join_handles: Arc<Mutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

#[derive(Debug, Clone)]
pub struct NodeFinderOptions {
    pub state_dir_path: String,
    pub max_connected_session_count: usize,
    pub max_accepted_session_count: usize,
}
#[allow(dead_code)]
struct SessionStatus {
    id: Vec<u8>,
    handshake_type: HandshakeType,
    node_profile: NodeProfile,
    session: Session,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
enum HandshakeType {
    Unknown,
    Connected,
    Accepted,
}

impl NodeFinder {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_profile_repo: Arc<NodeRefRepo>,
        node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        let result = Self {
            id: Arc::new(Self::gen_id()),
            session_connector,
            session_accepter,
            node_profile_repo,
            node_fetcher,
            system_clock: system_clock.clone(),
            option,
            cancellation_token,
            sessions: Arc::new(Mutex::new(Vec::new())),
            connected_node_profiles: Arc::new(Mutex::new(VolatileHashSet::new(Duration::seconds(180), system_clock))),
            join_handles: Arc::new(Mutex::new(None)),
        };
        result.create_tasks().await;

        result
    }

    fn gen_id() -> Vec<u8> {
        let mut rng = ChaCha20Rng::from_entropy();
        let mut id = [0_u8, 32];
        rng.fill_bytes(&mut id);
        id.to_vec()
    }

    async fn create_tasks(&self) {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        join_handles.extend(self.create_connect_task().await);

        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn create_connect_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = ConnectorTask {
            sessions: self.sessions.clone(),
            session_connector: self.session_connector.clone(),
            connected_node_profiles: self.connected_node_profiles.clone(),
            node_profile_repo: self.node_profile_repo.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    pub async fn terminate(&self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();

        if let Some(join_handles) = self.join_handles.lock().await.take() {
            join_handles.await;
        }

        Ok(())
    }
}

#[derive(Clone)]
struct ConnectorTask {
    sessions: Arc<Mutex<Vec<SessionStatus>>>,
    session_connector: Arc<SessionConnector>,
    connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
    node_profile_repo: Arc<NodeRefRepo>,
    option: NodeFinderOptions,
}

impl ConnectorTask {
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

        if self.connected_node_profiles.lock().await.contains(node_profile) {
            anyhow::bail!("Already connected");
        }

        for addr in node_profile.addrs.iter() {
            let session = self.session_connector.connect(addr, &SessionType::NodeFinder).await?;
            let (id, node_profile) = Communicator::handshake(&session).await?;
            self.sessions.lock().await.push(SessionStatus {
                id,
                handshake_type: HandshakeType::Connected,
                node_profile,
                session,
            });
        }

        self.connected_node_profiles.lock().await.insert(node_profile.clone());

        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct AccepterTask {
    sessions: Arc<Mutex<Vec<SessionStatus>>>,
    session_accepter: Arc<SessionAccepter>,
    option: NodeFinderOptions,
}

#[allow(dead_code)]
impl AccepterTask {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let _ = self.accept().await;
                    }
                } => {}
            }
        })
    }

    async fn accept(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .lock()
            .await
            .iter()
            .filter(|n| n.handshake_type == HandshakeType::Accepted)
            .count();
        if session_count >= self.option.max_accepted_session_count {
            return Ok(());
        }

        let session = self.session_accepter.accept(&SessionType::NodeFinder).await?;

        let (id, node_profile) = Communicator::handshake(&session).await?;
        self.sessions.lock().await.push(SessionStatus {
            id,
            handshake_type: HandshakeType::Connected,
            node_profile,
            session,
        });

        Ok(())
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct SessionTask {}

#[allow(dead_code)]
#[derive(Clone)]
struct ComputeTask {}
