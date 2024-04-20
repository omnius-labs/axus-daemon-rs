use std::sync::{Arc, Mutex as StdMutex};

use tokio::{
    select,
    sync::{mpsc, Mutex as TokioMutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::service::session::{
    model::{Session, SessionType},
    SessionAccepter,
};

use super::{HandshakeType, NodeFinderOptions, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskAccepter {
    pub sessions: Arc<StdMutex<Vec<SessionStatus>>>,
    pub session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
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
                        let res = self.accept().await;
                        if let Err(e) = res {
                            warn!("{:?}", e);
                        }
                    }
                } => {}
            }
        })
    }

    async fn accept(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .lock()
            .unwrap()
            .iter()
            .filter(|n| n.handshake_type == HandshakeType::Accepted)
            .count();
        if session_count >= self.option.max_accepted_session_count {
            return Ok(());
        }

        let session = self.session_accepter.accept(&SessionType::NodeFinder).await?;

        self.session_sender.lock().await.send((HandshakeType::Accepted, session)).await?;

        Ok(())
    }
}
