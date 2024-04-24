use std::sync::{Arc, Mutex as StdMutex};

use chrono::{Duration, Utc};
use core_base::{clock::Clock, sleeper::Sleeper};
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

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskCommunicator {
    pub my_node_profile: Arc<StdMutex<NodeProfile>>,
    pub sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
    pub session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    pub clock: Arc<dyn Clock<Utc> + Send + Sync>,
    pub sleeper: Arc<dyn Sleeper + Send + Sync>,
    pub option: NodeFinderOptions,
}

#[allow(dead_code)]
impl TaskCommunicator {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        let wait_group = Arc::new(WaitGroup::new());
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        if let Some((handshake_type, session)) = self.session_receiver.lock().await.recv().await {
                            let my_node_profile = self.my_node_profile.clone();
                            let sessions = self.sessions.clone();
                            let clock = self.clock.clone();
                            let sleeper = self.sleeper.clone();
                            let cancellation_token = cancellation_token.clone();
                            let wait_group = wait_group.clone();
                            Self::create_session(my_node_profile, sessions, handshake_type, session, clock, sleeper, cancellation_token, wait_group).await;
                        }
                    }
                } => {}
            }
            wait_group.wait().await;
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_session(
        my_node_profile: Arc<StdMutex<NodeProfile>>,
        sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
        handshake_type: HandshakeType,
        session: Session,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        cancellation_token: CancellationToken,
        wait_group: Arc<WaitGroup>,
    ) -> JoinHandle<()> {
        wait_group.add(1);
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = Self::create_session_sub(my_node_profile, sessions, handshake_type, session, clock, sleeper, cancellation_token.clone()).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
            wait_group.done().await;
        })
    }

    async fn create_session_sub(
        my_node_profile: Arc<StdMutex<NodeProfile>>,
        sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
        handshake_type: HandshakeType,
        session: Session,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<()> {
        let my_node_profile = my_node_profile.lock().unwrap().clone();
        let node_profile = Communicator::handshake(&session, &my_node_profile).await?;
        let status = SessionStatus {
            handshake_type,
            session,
            node_profile: node_profile.clone(),
            sending_data_message: Arc::new(SendingDataMessage::default()),
            received_data_message: Arc::new(ReceivedDataMessage::new(clock)),
        };

        {
            let mut sessions = sessions.write().await;
            if sessions.iter().any(|n| n.node_profile.id == node_profile.id) {
                return Err(anyhow::anyhow!("Session already exists"));
            }
            sessions.push(status.clone());
        }

        info!("Session established: {}", status.node_profile);

        let send = Self::send(status.clone(), sleeper.clone(), cancellation_token.clone());
        let receive = Self::receive(status.clone(), sleeper.clone(), cancellation_token.clone());

        let _ = tokio::join!(send, receive);

        Ok(())
    }

    async fn send(status: SessionStatus, sleeper: Arc<dyn Sleeper + Send + Sync>, cancellation_token: CancellationToken) -> JoinHandle<()> {
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
            sleeper.sleep(Duration::seconds(1).to_std().unwrap()).await;
        }
    }

    async fn receive(status: SessionStatus, sleeper: Arc<dyn Sleeper + Send + Sync>, cancellation_token: CancellationToken) -> JoinHandle<()> {
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

    async fn receive_sub(_status: SessionStatus, sleeper: Arc<dyn Sleeper + Send + Sync>) -> anyhow::Result<()> {
        loop {
            sleeper.sleep(Duration::seconds(1).to_std().unwrap()).await;
        }
    }
}
