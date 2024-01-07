use crate::{
    model::{OmniAddress, OmniSignature},
    service::connection::AsyncSendRecv,
};

#[derive(Debug, Clone)]
pub enum SessionType {
    NodeExchanger,
}

pub enum SessionHandshakeType {
    Connected,
    Accepted,
}

pub struct SessionTcp {
    pub typ: SessionType,
    pub address: OmniAddress,
    pub handshake_type: SessionHandshakeType,
    pub signature: OmniSignature,
    pub stream: Box<dyn AsyncSendRecv + Send + Sync + Unpin>,
}
