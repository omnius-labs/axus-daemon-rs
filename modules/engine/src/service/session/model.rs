use omnius_core_omnikit::{OmniAddr, OmniCert};

use crate::service::connection::FramedStream;

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
    pub address: OmniAddr,
    pub handshake_type: SessionHandshakeType,
    pub cert: OmniCert,
    pub stream: FramedStream,
}
