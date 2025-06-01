use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use futures::FutureExt;
use tokio::{
    sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc},
    task::JoinHandle,
};
use tracing::warn;

use omnius_core_base::sleeper::Sleeper;

use crate::{
    core::{
        session::{
            SessionAccepter,
            model::{Session, SessionType},
        },
        util::Terminable,
    },
    prelude::*,
};

use super::*;

#[derive(Clone)]
pub struct TaskAccepter {
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    session_accepter: Arc<SessionAccepter>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    option: NodeFinderOption,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

#[async_trait]
impl Terminable for TaskAccepter {
    async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
    }
}

impl TaskAccepter {
    pub async fn new(
        sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
        session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
        session_accepter: Arc<SessionAccepter>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOption,
    ) -> Result<Arc<Self>> {
        let v = Arc::new(Self {
            sessions,
            session_sender,
            session_accepter,
            sleeper,
            option,
            join_handle: Arc::new(TokioMutex::new(None)),
        });

        v.clone().start().await?;

        Ok(v)
    }

    async fn start(self: Arc<Self>) -> Result<()> {
        let sleeper = self.sleeper.clone();
        let join_handle = self.join_handle.clone();
        *join_handle.lock().await = Some(tokio::spawn(async move {
            loop {
                sleeper.sleep(std::time::Duration::from_secs(1)).await;
                let res = self.accept().await;
                if let Err(e) = res {
                    warn!("{:?}", e);
                }
            }
        }));

        Ok(())
    }

    async fn accept(&self) -> Result<()> {
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

        self.session_sender
            .lock()
            .await
            .send((HandshakeType::Accepted, session))
            .await
            .map_err(|e| Error::new(ErrorKind::UnexpectedError).source(e))?;

        Ok(())
    }
}
