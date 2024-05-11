use core_omnius::{FramedStream, OmniAddress, OmniSignature};

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
    pub stream: FramedStream,
}
