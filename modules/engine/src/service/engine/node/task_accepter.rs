use std::sync::Arc;

use tokio::{
    select,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::service::session::{model::SessionType, SessionAccepter};

use super::{Communicator, HandshakeType, NodeFinderOptions, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskAccepter {
    pub sessions: Arc<Mutex<Vec<SessionStatus>>>,
    pub session_sender: Arc<Mutex<mpsc::Sender<SessionStatus>>>,
    pub session_accepter: Arc<SessionAccepter>,
    pub option: NodeFinderOptions,
}

#[allow(dead_code)]
impl TaskAccepter {
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
        self.session_sender
            .lock()
            .await
            .send(SessionStatus {
                id,
                handshake_type: HandshakeType::Accepted,
                node_profile,
                session,
            })
            .await?;

        Ok(())
    }
}
