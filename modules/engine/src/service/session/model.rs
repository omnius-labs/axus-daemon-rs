use std::sync::Arc;

use tokio::sync::Mutex as TokioMutex;

use crate::{
    model::{OmniAddress, OmniSignature},
    service::connection::{AsyncRecv, AsyncSend},
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionType {
    NodeFinder,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SessionHandshakeType {
    Connected,
    Accepted,
}

#[derive(Clone)]
pub struct Session {
    pub typ: SessionType,
    pub address: OmniAddress,
    pub handshake_type: SessionHandshakeType,
    pub signature: OmniSignature,
    pub reader: Arc<TokioMutex<dyn AsyncRecv + Send + Sync + Unpin>>,
    pub writer: Arc<TokioMutex<dyn AsyncSend + Send + Sync + Unpin>>,
}
