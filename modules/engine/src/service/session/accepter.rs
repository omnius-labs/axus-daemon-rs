use std::{collections::HashMap, sync::Arc};

use omnius_core_base::{random_bytes::RandomBytesProvider, sleeper::Sleeper};
use futures::{future::join_all, FutureExt};
use omnius_core_omnikit::{OmniAddr, OmniSigner};
use tokio::{
    sync::{mpsc, Mutex as TokioMutex},
    task::JoinHandle,
};
use tracing::warn;

use crate::{
    connection::{FramedRecvExt as _, FramedSendExt as _},
    service::{
        connection::ConnectionTcpAccepter,
        session::message::{HelloMessage, SessionVersion, V1ChallengeMessage, V1RequestMessage, V1SignatureMessage},
    },
};

use super::{
    message::{V1RequestType, V1ResultMessage, V1ResultType},
    model::{Session, SessionHandshakeType, SessionType},
};

pub struct SessionAccepter {
    tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
    signer: Arc<OmniSigner>,
    random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    receivers: Arc<TokioMutex<HashMap<SessionType, mpsc::Receiver<Session>>>>,
    senders: Arc<TokioMutex<HashMap<SessionType, mpsc::Sender<Session>>>>,
    task_acceptors: Arc<TokioMutex<Vec<TaskAccepter>>>,
}

impl SessionAccepter {
    pub async fn new(
        tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
        signer: Arc<OmniSigner>,
        random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Self {
        let senders = Arc::new(TokioMutex::new(HashMap::<SessionType, mpsc::Sender<Session>>::new()));
        let receivers = Arc::new(TokioMutex::new(HashMap::<SessionType, mpsc::Receiver<Session>>::new()));

        for typ in [SessionType::NodeFinder].iter() {
            let (tx, rx) = mpsc::channel(20);
            senders.lock().await.insert(typ.clone(), tx);
            receivers.lock().await.insert(typ.clone(), rx);
        }

        let result = Self {
            tcp_connector,
            signer,
            random_bytes_provider,
            sleeper,
            receivers,
            senders,
            task_acceptors: Arc::new(TokioMutex::new(Vec::new())),
        };
        result.run().await;

        result
    }

    async fn run(&self) {
        for _ in 0..3 {
            let task = TaskAccepter::new(
                self.senders.clone(),
                self.tcp_connector.clone(),
                self.signer.clone(),
                self.random_bytes_provider.clone(),
                self.sleeper.clone(),
            );
            task.run().await;
            self.task_acceptors.lock().await.push(task);
        }
    }

    pub async fn terminate(&self) -> anyhow::Result<()> {
        let mut task_acceptors = self.task_acceptors.lock().await;
        let task_acceptors: Vec<TaskAccepter> = task_acceptors.drain(..).collect();
        join_all(task_acceptors.iter().map(|task| task.terminate())).await;

        Ok(())
    }

    pub async fn accept(&self, typ: &SessionType) -> anyhow::Result<Session> {
        let mut receivers = self.receivers.lock().await;
        let receiver = receivers.get_mut(typ).unwrap();

        receiver.recv().await.ok_or_else(|| anyhow::anyhow!("Session not found"))
    }
}

#[derive(Clone)]
struct TaskAccepter {
    inner: Inner,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
}

impl TaskAccepter {
    pub fn new(
        senders: Arc<TokioMutex<HashMap<SessionType, mpsc::Sender<Session>>>>,
        tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
        signer: Arc<OmniSigner>,
        random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Self {
        let inner = Inner {
            senders,
            tcp_connector,
            signer,
            random_bytes_provider,
        };
        Self {
            inner,
            sleeper,
            join_handle: Arc::new(TokioMutex::new(None)),
        }
    }

    pub async fn terminate(&self) {
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
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
        self.join_handle.lock().await.replace(join_handle);
    }
}

#[derive(Clone)]
struct Inner {
    senders: Arc<TokioMutex<HashMap<SessionType, mpsc::Sender<Session>>>>,
    tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
    signer: Arc<OmniSigner>,
    random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
}

impl Inner {
    async fn accept(&self) -> anyhow::Result<()> {
        let (stream, addr) = self.tcp_connector.accept().await?;

        let send_hello_message = HelloMessage { version: SessionVersion::V1 };
        stream.sender.lock().await.send_message(&send_hello_message).await?;
        let received_hello_message: HelloMessage = stream.receiver.lock().await.recv_message().await?;

        let version = send_hello_message.version | received_hello_message.version;

        if version.contains(SessionVersion::V1) {
            let send_nonce: [u8; 32] = self
                .random_bytes_provider
                .get_bytes(32)
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid nonce length"))?;
            let send_challenge_message = V1ChallengeMessage { nonce: send_nonce };
            stream.sender.lock().await.send_message(&send_challenge_message).await?;
            let receive_challenge_message: V1ChallengeMessage = stream.receiver.lock().await.recv_message().await?;

            let send_signature = self.signer.sign(&receive_challenge_message.nonce)?;
            let send_signature_message = V1SignatureMessage { cert: send_signature };
            stream.sender.lock().await.send_message(&send_signature_message).await?;
            let received_signature_message: V1SignatureMessage = stream.receiver.lock().await.recv_message().await?;

            if received_signature_message.cert.verify(send_nonce.as_slice()).is_err() {
                anyhow::bail!("Invalid signature")
            }

            let received_session_request_message: V1RequestMessage = stream.receiver.lock().await.recv_message().await?;
            let typ = match received_session_request_message.request_type {
                V1RequestType::NodeExchanger => SessionType::NodeFinder,
            };
            if let Ok(permit) = self.senders.lock().await.get(&typ).unwrap().try_reserve() {
                let send_session_result_message = V1ResultMessage {
                    result_type: V1ResultType::Accept,
                };
                stream.sender.lock().await.send_message(&send_session_result_message).await?;

                let session = Session {
                    typ: typ.clone(),
                    address: OmniAddr::new(format!("tcp({})", addr).as_str()),
                    handshake_type: SessionHandshakeType::Accepted,
                    cert: received_signature_message.cert,
                    stream,
                };
                permit.send(session);
            } else {
                let send_session_result_message = V1ResultMessage {
                    result_type: V1ResultType::Reject,
                };
                stream.sender.lock().await.send_message(&send_session_result_message).await?;
            }

            Ok(())
        } else {
            anyhow::bail!("Unsupported session version: {:?}", version)
        }
    }
}
