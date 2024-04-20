use std::sync::{Arc, Mutex as StdMutex};

use tokio::{
    select,
    sync::{mpsc, Mutex as TokioMutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    model::NodeProfile,
    service::{
        engine::node::{ReceivedDataMessage, SendingDataMessage},
        session::model::Session,
    },
};

use super::{Communicator, HandshakeType, NodeFinderOptions, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskCommunicator {
    pub my_node_profile: Arc<StdMutex<NodeProfile>>,
    pub sessions: Arc<StdMutex<Vec<SessionStatus>>>,
    pub session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    pub option: NodeFinderOptions,
    // pub _received_push_ContentLocationMap;
}

#[allow(dead_code)]
impl TaskCommunicator {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    loop {
                        if let Some((handshake_type, session)) = self.session_receiver.lock().await.recv().await {
                            let res = self.communicate(handshake_type, session).await;
                            if let Err(e) = res {
                                warn!("{:?}", e);
                            }
                        }
                    }
                } => {}
            }
        })
    }

    async fn communicate(&self, handshake_type: HandshakeType, session: Session) -> anyhow::Result<()> {
        let my_node_profile = self.my_node_profile.as_ref().lock().unwrap().clone();
        let node_profile = Communicator::handshake(&session, &my_node_profile).await?;
        let state = SessionStatus {
            handshake_type,
            session,
            node_profile: node_profile.clone(),

            sending_data_message: Arc::new(StdMutex::new(SendingDataMessage::default())),
            received_data_message: Arc::new(StdMutex::new(ReceivedDataMessage::default())),
        };

        let mut sessions = self.sessions.lock().unwrap();
        if sessions.iter().any(|n| n.node_profile.id == node_profile.id) {
            return Err(anyhow::anyhow!("Session already exists"));
        }

        info!("Session established: {}", state.node_profile);

        sessions.push(state);

        Ok(())
    }
}
