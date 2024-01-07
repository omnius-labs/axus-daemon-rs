use crate::{
    model::{OmniAddress, OmniSignature, OmniSigner},
    service::connection::{AsyncSendRecv, AsyncSendRecvExt, ConnectionTcpConnector},
};

use super::{
    message::HelloMessage,
    model::{SessionHandshakeType, SessionTcp, SessionType},
};

pub struct SessionConnector {
    pub tcp_connector: ConnectionTcpConnector,
    pub signer: OmniSigner,
}

impl SessionConnector {
    pub fn new(tcp_connector: ConnectionTcpConnector, signer: OmniSigner) -> Self {
        Self { tcp_connector, signer }
    }

    pub async fn connect(&self, address: &OmniAddress, typ: &SessionType) -> anyhow::Result<SessionTcp> {
        let mut stream: Box<dyn AsyncSendRecv + Send + Sync + Unpin> = Box::new(self.tcp_connector.connect(address.parse_tcp()?.as_str()).await?);

        let signature = self.handshake(&mut stream, typ).await?;

        let session = SessionTcp {
            typ: typ.clone(),
            address: address.clone(),
            handshake_type: SessionHandshakeType::Connected,
            signature,
            stream,
        };
        Ok(session)
    }

    async fn handshake(&self, stream: &mut Box<dyn AsyncSendRecv + Send + Sync + Unpin>, _typ: &SessionType) -> anyhow::Result<OmniSignature> {
        let send_hello_message = HelloMessage {
            version: super::message::SessionVersion::V1,
        };
        stream.send_message(send_hello_message).await?;

        let _: HelloMessage = stream.recv_message().await?;

        todo!()
    }
}
