use std::sync::Arc;

use tokio::{select, sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{NodeFinderOptions, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskComputer {
    pub sessions: Arc<Mutex<Vec<SessionStatus>>>,
    pub option: NodeFinderOptions,
}

#[allow(dead_code)]
impl TaskComputer {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let _ = self.compute().await;
                    }
                } => {}
            }
        })
    }

    async fn compute(&self) -> anyhow::Result<()> {
        Ok(())
    }
}
