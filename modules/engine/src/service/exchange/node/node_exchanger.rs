use std::sync::Arc;

use core_base::clock::SystemClock;
use sqlx::types::chrono::Utc;
use tokio_util::sync::CancellationToken;

use crate::service::session::{SessionAccepter, SessionConnector};

use super::NodeFetcher;

pub struct NodeExchanger {
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_fetcher: Arc<dyn NodeFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeExchangerOptions,
}

pub struct NodeExchangerOptions {
    pub state_directory_path: String,
    pub max_session_count: u32,
}

impl NodeExchanger {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_fetcher: Arc<dyn NodeFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeExchangerOptions,
    ) -> Self {
        let token = CancellationToken::new();

        Self {
            session_connector,
            session_accepter,
            node_fetcher,
            system_clock,
            option,
        }
    }

    async fn internal_connect() -> anyhow::Result<()> {}
}
