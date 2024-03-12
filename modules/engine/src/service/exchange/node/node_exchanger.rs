use std::sync::Arc;

use core_base::clock::SystemClock;
use futures::future::{join_all, JoinAll};
use sqlx::types::chrono::Utc;
use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use crate::{
    model::NodeProfile,
    service::session::{model::Session, SessionAccepter, SessionConnector},
};

use super::NodeFetcher;

pub struct NodeExchanger {
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_fetcher: Arc<dyn NodeFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeExchangerOptions,
    cancellation_token: CancellationToken,
    join_handles: Arc<Mutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

pub struct NodeExchangerOptions {
    pub state_directory_path: String,
    pub max_session_count: u32,
}

struct SessionStatus {
    session: Session,
    id: Vec<u8>,
    node_profile: NodeProfile,
}

impl NodeExchanger {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_fetcher: Arc<dyn NodeFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeExchangerOptions,
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
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

        for _ in 0..3 {
            let token = self.cancellation_token.clone();
            let sender = self.senders.clone();
            let tcp_connector = self.tcp_connector.clone();
            let signer = self.signer.clone();
            let random_bytes_provider = self.random_bytes_provider.clone();
            let join_handle = tokio::spawn(async move {
                select! {
                    _ = token.cancelled() => {}
                    _ = async {
                        loop {
                             let _ = Self::internal_accept(sender.clone(), tcp_connector.clone(), signer.clone(), random_bytes_provider.clone()).await;
                        }
                    } => {}
                }
            });

            join_handles.push(join_handle);
        }
        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn internal_connect(session_connector: Arc<SessionConnector>) -> anyhow::Result<()> {
        session_connector.connect(address, typ)
    }
}
