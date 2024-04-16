use std::sync::Arc;

use tokio::{
    select,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::{NodeFinderOptions, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskCommunicator {
    pub sessions: Arc<Mutex<Vec<SessionStatus>>>,
    pub session_receiver: Arc<Mutex<mpsc::Receiver<SessionStatus>>>,
    pub option: NodeFinderOptions,
}

#[allow(dead_code)]
impl TaskCommunicator {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        let session = self.session_receiver.lock().await.recv().await;
                        if session.is_none() {
                            continue;
                        }
                        let session = session.unwrap();
                        let _ = self.communicate(session).await;
                    }
                } => {}
            }
        })
    }

    async fn communicate(&self, _session: SessionStatus) -> anyhow::Result<()> {
        Ok(())
    }
}
