use std::sync::Arc;

use core_base::random_bytes::RandomBytesProvider;
use tokio::sync::Mutex;

use crate::{
    model::{OmniAddress, OmniSigner},
    service::{
        connection::{AsyncSendRecv, AsyncSendRecvExt, ConnectionTcpConnector},
        session::message::{V1ChallengeMessage, V1SignatureMessage},
    },
};

use super::{
    message::{HelloMessage, SessionVersion, V1RequestMessage, V1RequestType, V1ResultMessage, V1ResultType},
    model::{Session, SessionHandshakeType, SessionType},
};

pub struct SessionConnector {
    tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync>,
    signer: Arc<OmniSigner>,
    random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
}

impl SessionConnector {
    pub fn new(
        tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync>,
        signer: Arc<OmniSigner>,
        random_bytes_provider: Arc<dyn RandomBytesProvider + Send + Sync>,
    ) -> Self {
        Self {
            tcp_connector,
            signer,
            random_bytes_provider,
        }
    }

    pub async fn connect(&self, address: &OmniAddress, typ: &SessionType) -> anyhow::Result<Session> {
        let stream = self.tcp_connector.connect(address.parse_tcp()?.as_str()).await?;
        let stream: Arc<Mutex<dyn AsyncSendRecv + Send + Sync + Unpin>> = Arc::new(Mutex::new(stream));

        let send_hello_message = HelloMessage { version: SessionVersion::V1 };
        stream.lock().await.send_message(&send_hello_message).await?;
        let received_hello_message: HelloMessage = stream.lock().await.recv_message().await?;

        let version = send_hello_message.version | received_hello_message.version;

        if version.contains(SessionVersion::V1) {
            let send_nonce: [u8; 32] = self
                .random_bytes_provider
                .get_bytes(32)
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid nonce length"))?;
            let send_challenge_message = V1ChallengeMessage { nonce: send_nonce };
            stream.lock().await.send_message(&send_challenge_message).await?;
            let receive_challenge_message: V1ChallengeMessage = stream.lock().await.recv_message().await?;

            let send_signature = self.signer.sign(&receive_challenge_message.nonce)?;
            let send_signature_message = V1SignatureMessage { signature: send_signature };
            stream.lock().await.send_message(&send_signature_message).await?;
            let received_signature_message: V1SignatureMessage = stream.lock().await.recv_message().await?;

            if received_signature_message.signature.verify(send_nonce.as_slice()).is_err() {
                anyhow::bail!("Invalid signature")
            }

            let send_session_request_message = V1RequestMessage {
                request_type: match typ {
                    SessionType::NodeExchanger => V1RequestType::NodeExchanger,
                },
            };
            stream.lock().await.send_message(&send_session_request_message).await?;
            let received_session_result_message: V1ResultMessage = stream.lock().await.recv_message().await?;

            if received_session_result_message.result_type == V1ResultType::Reject {
                anyhow::bail!("Session rejected")
            }

            let session = Session {
                typ: typ.clone(),
                address: address.clone(),
                handshake_type: SessionHandshakeType::Connected,
                signature: received_signature_message.signature,
                stream,
            };

            Ok(session)
        } else {
            anyhow::bail!("Unsupported session version: {:?}", version)
        }
    }
}
