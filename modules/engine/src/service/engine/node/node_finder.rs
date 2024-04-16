use std::sync::Arc;

use chrono::Duration;
use core_base::clock::SystemClock;
use futures::future::{join_all, JoinAll};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sqlx::types::chrono::Utc;
use tokio::{
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeProfile,
    service::{
        session::{SessionAccepter, SessionConnector},
        util::VolatileHashSet,
    },
};

use super::{NodeRefFetcher, NodeRefRepo, SessionStatus, TaskAccepter, TaskCommunicator, TaskComputer, TaskConnector};

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

    session_receiver: Arc<Mutex<mpsc::Receiver<SessionStatus>>>,
    session_sender: Arc<Mutex<mpsc::Sender<SessionStatus>>>,
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
        let (tx, rx) = mpsc::channel(20);

        let result = Self {
            id: Arc::new(Self::gen_id()),
            session_connector,
            session_accepter,
            node_profile_repo,
            node_fetcher,
            system_clock: system_clock.clone(),
            option,
            cancellation_token,

            session_receiver: Arc::new(Mutex::new(rx)),
            session_sender: Arc::new(Mutex::new(tx)),
            sessions: Arc::new(Mutex::new(Vec::new())),
            connected_node_profiles: Arc::new(Mutex::new(VolatileHashSet::new(Duration::seconds(180), system_clock))),
            join_handles: Arc::new(Mutex::new(None)),
        };
        result.run().await;

        result
    }

    fn gen_id() -> Vec<u8> {
        let mut rng = ChaCha20Rng::from_entropy();
        let mut id = [0_u8, 32];
        rng.fill_bytes(&mut id);
        id.to_vec()
    }

    async fn run(&self) {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        join_handles.extend(self.create_connect_task().await);
        join_handles.extend(self.create_accept_task().await);
        join_handles.extend(self.create_communicator_task().await);
        join_handles.extend(self.create_computer_task().await);

        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn create_connect_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskConnector {
            sessions: self.sessions.clone(),
            session_sender: self.session_sender.clone(),
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

    async fn create_accept_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskAccepter {
            sessions: self.sessions.clone(),
            session_sender: self.session_sender.clone(),
            session_accepter: self.session_accepter.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    async fn create_communicator_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskCommunicator {
            sessions: self.sessions.clone(),
            session_receiver: self.session_receiver.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    async fn create_computer_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskComputer {
            sessions: self.sessions.clone(),
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
