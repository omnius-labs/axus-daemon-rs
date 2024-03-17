use std::sync::Arc;

use core_base::clock::SystemClock;
use futures::future::JoinAll;
use sqlx::types::chrono::Utc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeRef,
    service::session::{model::Session, SessionAccepter, SessionConnector},
};

use super::NodeRefFetcher;
#[allow(dead_code)]
pub struct NodeFinder {
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeFinderOptions,
    cancellation_token: CancellationToken,
    join_handles: Arc<Mutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

pub struct NodeFinderOptions {
    pub state_directory_path: String,
    pub max_session_count: u32,
}
#[allow(dead_code)]
struct SessionStatus {
    session: Session,
    id: Vec<u8>,
    node_profile: NodeRef,
}

impl NodeFinder {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_fetcher: Arc<dyn NodeRefFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let cancellation_token = CancellationToken::new();

        let result = Self {
            session_connector,
            session_accepter,
            node_fetcher,
            system_clock,
            option,
            cancellation_token,
            join_handles: Arc::new(Mutex::new(None)),
        };
        result.create_tasks().await;

        result
    }

    async fn create_tasks(&self) {
        todo!()
    }
}
