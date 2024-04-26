use std::{collections::HashMap, sync::Arc};

use core_base::sleeper::Sleeper;
use futures::FutureExt;
use tokio::{
    sync::{mpsc, Mutex as TokioMutex, RwLock as TokioRwLock},
    task::JoinHandle,
};
use tracing::warn;

use crate::service::session::{
    model::{Session, SessionType},
    SessionAccepter,
};

use super::{HandshakeType, NodeFinderOptions, SessionStatus};

#[derive(Clone)]
pub struct TaskAccepter {
    inner: Inner,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

impl TaskAccepter {
    pub fn new(
        sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
        session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
        session_accepter: Arc<SessionAccepter>,
        option: NodeFinderOptions,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Self {
        let inner = Inner {
            sessions,
            session_sender,
            session_accepter,
            option,
        };
        Self {
            inner,
            sleeper,
            join_handle: Arc::new(TokioMutex::new(None)),
        }
    }

    pub async fn run(&self) {
        let sleeper = self.sleeper.clone();
        let inner = self.inner.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                sleeper.sleep(std::time::Duration::from_secs(1)).await;
                let res = inner.accept().await;
                if let Err(e) = res {
                    warn!("{:?}", e);
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }

    pub async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct Inner {
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    session_accepter: Arc<SessionAccepter>,
    option: NodeFinderOptions,
}

#[allow(dead_code)]
impl Inner {
    async fn accept(&self) -> anyhow::Result<()> {
        let session_count = self
            .sessions
            .read()
            .await
            .iter()
            .filter(|(_, status)| status.handshake_type == HandshakeType::Accepted)
            .count();
        if session_count >= self.option.max_accepted_session_count {
            return Ok(());
        }

        let session = self.session_accepter.accept(&SessionType::NodeFinder).await?;

        self.session_sender.lock().await.send((HandshakeType::Accepted, session)).await?;

        Ok(())
    }
}
