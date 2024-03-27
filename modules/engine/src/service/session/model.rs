use std::sync::Arc;

use tokio::sync::Mutex;

use crate::{
    model::{OmniAddress, OmniSignature},
    service::connection::AsyncSendRecv,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionType {
    NodeFinder,
}

pub enum SessionHandshakeType {
    Connected,
    Accepted,
}

pub struct Session {
    pub typ: SessionType,
    pub address: OmniAddress,
    pub handshake_type: SessionHandshakeType,
    pub signature: OmniSignature,
    pub stream: Arc<Mutex<dyn AsyncSendRecv + Send + Sync + Unpin>>,
}
