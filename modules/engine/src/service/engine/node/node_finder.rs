use std::sync::Arc;

use chrono::Duration;
use core_base::clock::SystemClock;
use futures::future::{join_all, JoinAll};
use rand::{seq::SliceRandom, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sqlx::types::chrono::Utc;
use tokio::{select, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeRef,
    service::{
        session::{
            model::{Session, SessionType},
            SessionAccepter, SessionConnector,
        },
        util::VolatileHashSet,
    },
};

use super::{NodeRefFetcher, NodeRefRepo};

#[allow(dead_code)]
pub struct NodeFinder {
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_ref_repo: Arc<NodeRefRepo>,
    node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeFinderOptions,
    cancellation_token: CancellationToken,
    sessions: Arc<Mutex<Vec<SessionStatus>>>,
    connected_node_refs: Arc<Mutex<VolatileHashSet<NodeRef>>>,
    join_handles: Arc<Mutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

pub struct NodeFinderOptions {
    pub state_directory_path: String,
    pub max_connected_session_count: u32,
    pub max_accepted_session_count: u32,
}
#[allow(dead_code)]
struct SessionStatus {
    id: Vec<u8>,
    node_ref: NodeRef,
    session: Session,
}

impl NodeFinder {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_ref_repo: Arc<NodeRefRepo>,
        node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        let result = Self {
            session_connector,
            session_accepter,
            node_ref_repo,
            node_fetcher,
            system_clock: system_clock.clone(),
            option,
            cancellation_token,
            sessions: Arc::new(Mutex::new(Vec::new())),
            connected_node_refs: Arc::new(Mutex::new(VolatileHashSet::new(
                Duration::seconds(180),
                Duration::seconds(30),
                system_clock,
            ))),
            join_handles: Arc::new(Mutex::new(None)),
        };
        result.create_tasks().await;

        result
    }

    async fn create_tasks(&self) {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

        for _ in 0..3 {
            let token = self.cancellation_token.clone();
            let sessions = self.sessions.clone();
            let session_connector = self.session_connector.clone();
            let connected_node_refs = self.connected_node_refs.clone();
            let node_ref_repo = self.node_ref_repo.clone();
            let join_handle = tokio::spawn(async move {
                select! {
                    _ = token.cancelled() => {}
                    _ = async {
                        loop {
                             let _ = Self::internal_connect(sessions.clone(), session_connector.clone(), connected_node_refs.clone(), node_ref_repo.clone()).await;
                        }
                    } => {}
                }
            });

            join_handles.push(join_handle);
        }

        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn internal_connect(
        sessions: Arc<Mutex<Vec<SessionStatus>>>,
        session_connector: Arc<SessionConnector>,
        connected_node_refs: Arc<Mutex<VolatileHashSet<NodeRef>>>,
        node_ref_repo: Arc<NodeRefRepo>,
    ) -> anyhow::Result<()> {
        connected_node_refs.lock().await.refresh();

        let mut rng = ChaCha20Rng::from_entropy();
        let node_refs = node_ref_repo.get_node_refs().await?;
        let node_ref = node_refs.choose(&mut rng).ok_or(anyhow::anyhow!("No node refs"))?;

        if connected_node_refs.lock().await.contains(node_ref) {
            anyhow::bail!("Already connected");
        }

        for addr in node_ref.addrs.iter() {
            let session = session_connector.connect(addr, &SessionType::NodeFinder).await?;
            let id = Self::handshake(&session).await?;
            sessions.lock().await.push(SessionStatus {
                id,
                node_ref: node_ref.clone(),
                session,
            });
        }

        connected_node_refs.lock().await.insert(node_ref.clone());

        Ok(())
    }

    async fn handshake(_session: &Session) -> anyhow::Result<Vec<u8>> {
        todo!()
    }
}
