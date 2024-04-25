use std::sync::{Arc, Mutex as StdMutex};

use chrono::Utc;
use core_base::{clock::Clock, sleeper::Sleeper};
use futures::FutureExt;
use tokio::{
    select,
    sync::{mpsc, Mutex as TokioMutex, RwLock as TokioRwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    model::NodeProfile,
    service::{
        engine::node::{ReceivedDataMessage, SendingDataMessage},
        session::model::Session,
        util::WaitGroup,
    },
};

use super::{Communicator, HandshakeType, NodeFinderOptions, SessionStatus};

#[derive(Clone)]
pub struct TaskCommunicator {
    session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    inner: TaskCommunicatorInner,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    wait_group: Arc<WaitGroup>,
    cancellation_token: CancellationToken,
}

impl TaskCommunicator {
    pub fn new(
        my_node_profile: Arc<StdMutex<NodeProfile>>,
        sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
        session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let wait_group = Arc::new(WaitGroup::new());
        let cancellation_token = CancellationToken::new();
        let inner = TaskCommunicatorInner {
            my_node_profile,
            sessions,
            clock,
            option,
            sleeper,
            wait_group: wait_group.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        Self {
            session_receiver,
            inner,
            join_handle: Arc::new(TokioMutex::new(None)),
            wait_group,
            cancellation_token,
        }
    }

    pub async fn run(&self) {
        let session_receiver = self.session_receiver.clone();
        let inner = self.inner.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                if let Some((handshake_type, session)) = session_receiver.lock().await.recv().await {
                    let res = inner.clone().communicate(handshake_type, session);
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }

    pub async fn terminate(&self) {
        self.cancellation_token.cancel();
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
        self.wait_group.wait().await;
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct TaskCommunicatorInner {
    my_node_profile: Arc<StdMutex<NodeProfile>>,
    sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    option: NodeFinderOptions,

    wait_group: Arc<WaitGroup>,
    cancellation_token: CancellationToken,
}

#[allow(dead_code)]
impl TaskCommunicatorInner {
    pub fn new(
        my_node_profile: Arc<StdMutex<NodeProfile>>,
        sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        Self {
            my_node_profile,
            sessions,
            clock,
            sleeper,
            option,

            wait_group: Arc::new(WaitGroup::new()),
            cancellation_token: CancellationToken::new(),
        }
    }

    pub fn communicate(self, handshake_type: HandshakeType, session: Session) -> anyhow::Result<()> {
        self.wait_group.add(1);
        tokio::spawn(async move {
            select! {
                _ = self.cancellation_token.cancelled() => {}
                _ = async {
                    let res = self.communicate_sub(handshake_type, session).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
            self.wait_group.done().await;
        });
        Ok(())
    }

    async fn communicate_sub(&self, handshake_type: HandshakeType, session: Session) -> anyhow::Result<()> {
        let my_node_profile = self.my_node_profile.lock().unwrap().clone();

        let node_profile = Communicator::handshake(&session, &my_node_profile).await?;

        let status = SessionStatus {
            handshake_type,
            session,
            node_profile: node_profile.clone(),
            sending_data_message: Arc::new(SendingDataMessage::default()),
            received_data_message: Arc::new(ReceivedDataMessage::new(self.clock.clone())),
        };

        {
            let mut sessions = self.sessions.write().await;
            if sessions.iter().any(|n| n.node_profile.id == node_profile.id) {
                return Err(anyhow::anyhow!("Session already exists"));
            }
            sessions.push(status.clone());
        }

        info!("Session established: {}", status.node_profile);

        let send = self.send(&status);
        let receive = self.receive(&status);

        let _ = tokio::join!(send, receive);

        Ok(())
    }

    fn send(&self, status: &SessionStatus) -> JoinHandle<()> {
        let status = status.clone();
        let sleeper = self.sleeper.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = Self::send_sub(status, sleeper).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
        })
    }

    async fn send_sub(_status: SessionStatus, sleeper: Arc<dyn Sleeper + Send + Sync>) -> anyhow::Result<()> {
        loop {
            sleeper.sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    fn receive(&self, status: &SessionStatus) -> JoinHandle<()> {
        let status = status.clone();
        let clock = self.clock.clone();
        let sleeper = self.sleeper.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = Self::receive_sub(status, sleeper).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
        })
    }

    async fn receive_sub(
        status: SessionStatus,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> anyhow::Result<()> {
        loop {
            sleeper.sleep(std::time::Duration::from_secs(1)).await;
            status.received_data_message = Arc::new(ReceivedDataMessage::new(clock.clone()));
        }
    }
}
