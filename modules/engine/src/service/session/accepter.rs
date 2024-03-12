use std::{collections::HashMap, sync::Arc};

use core_base::random_bytes::RandomBytesProvider;
use futures_util::future::{join_all, JoinAll};
use tokio::{
    select,
    sync::{mpsc, Mutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    model::{OmniAddress, OmniSigner},
    service::{
        connection::{AsyncSendRecv, AsyncSendRecvExt, ConnectionTcpAccepter},
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
    receivers: Arc<Mutex<HashMap<SessionType, mpsc::Receiver<Session>>>>,
    senders: Arc<Mutex<HashMap<SessionType, mpsc::Sender<Session>>>>,
    cancellation_token: CancellationToken,
    join_handles: Arc<Mutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

impl SessionAccepter {
    pub async fn new(
        tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
        signer: Arc<OmniSigner>,
        random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let senders = Arc::new(Mutex::new(HashMap::<SessionType, mpsc::Sender<Session>>::new()));
        let receivers = Arc::new(Mutex::new(HashMap::<SessionType, mpsc::Receiver<Session>>::new()));

        for typ in [SessionType::NodeExchanger].iter() {
            let (tx, rx) = mpsc::channel(20);
            senders.lock().await.insert(typ.clone(), tx);
            receivers.lock().await.insert(typ.clone(), rx);
        }

        let result = Self {
            tcp_connector,
            signer,
            random_bytes_provider,
            receivers,
            senders,
            cancellation_token,
            join_handles: Arc::new(Mutex::new(None)),
        };
        result.create_tasks().await;

        result
    }

    async fn create_tasks(&self) {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();

        for _ in 0..3 {
            let token = self.cancellation_token.clone();
            let sender = self.senders.clone();
            let tcp_connector = self.tcp_connector.clone();
            let signer = self.signer.clone();
            let random_bytes_provider = self.random_bytes_provider.clone();
            let join_handle = tokio::spawn(async move {
                select! {
                    _ = token.cancelled() => {}
                    _ = async {
                        loop {
                             let _ = Self::internal_accept(sender.clone(), tcp_connector.clone(), signer.clone(), random_bytes_provider.clone()).await;
                        }
                    } => {}
                }
            });

            join_handles.push(join_handle);
        }

        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn internal_accept(
        senders: Arc<Mutex<HashMap<SessionType, mpsc::Sender<Session>>>>,
        tcp_connector: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
        signer: Arc<OmniSigner>,
        random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
    ) -> anyhow::Result<()> {
        let (stream, addr) = tcp_connector.accept().await?;
        let stream: Arc<Mutex<dyn AsyncSendRecv + Send + Sync + Unpin>> = Arc::new(Mutex::new(stream));

        let send_hello_message = HelloMessage { version: SessionVersion::V1 };
        stream.lock().await.send_message(&send_hello_message).await?;
        let received_hello_message: HelloMessage = stream.lock().await.recv_message().await?;

        let version = send_hello_message.version | received_hello_message.version;

        if version.contains(SessionVersion::V1) {
            let send_nonce: [u8; 32] = random_bytes_provider
                .get_bytes(32)
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid nonce length"))?;
            let send_challenge_message = V1ChallengeMessage { nonce: send_nonce };
            stream.lock().await.send_message(&send_challenge_message).await?;
            let receive_challenge_message: V1ChallengeMessage = stream.lock().await.recv_message().await?;

            let send_signature = signer.sign(&receive_challenge_message.nonce)?;
            let send_signature_message = V1SignatureMessage { signature: send_signature };
            stream.lock().await.send_message(&send_signature_message).await?;
            let received_signature_message: V1SignatureMessage = stream.lock().await.recv_message().await?;

            if received_signature_message.signature.verify(send_nonce.as_slice()).is_err() {
                anyhow::bail!("Invalid signature")
            }

            let received_session_request_message: V1RequestMessage = stream.lock().await.recv_message().await?;
            let typ = match received_session_request_message.request_type {
                V1RequestType::NodeExchanger => SessionType::NodeExchanger,
            };
            if let Ok(permit) = senders.lock().await.get(&typ).unwrap().try_reserve() {
                let send_session_result_message = V1ResultMessage {
                    result_type: V1ResultType::Accept,
                };
                stream.lock().await.send_message(&send_session_result_message).await?;

                let session = Session {
                    typ: typ.clone(),
                    address: OmniAddress::new(format!("tcp({})", addr).as_str()),
                    handshake_type: SessionHandshakeType::Accepted,
                    signature: received_signature_message.signature,
                    stream,
                };
                permit.send(session);
            } else {
                let send_session_result_message = V1ResultMessage {
                    result_type: V1ResultType::Reject,
                };
                stream.lock().await.send_message(&send_session_result_message).await?;
            }

            Ok(())
        } else {
            anyhow::bail!("Unsupported session version: {:?}", version)
        }
    }

    pub async fn accept(&self, typ: &SessionType) -> anyhow::Result<Session> {
        let mut receivers = self.receivers.lock().await;
        let receiver = receivers.get_mut(typ).unwrap();

        receiver.recv().await.ok_or_else(|| anyhow::anyhow!("Session not found"))
    }

    pub async fn terminate(&self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();

        if let Some(join_handle) = self.join_handles.lock().await.take() {
            join_handle.await;
        }

        Ok(())
    }
}
