use crate::{model::NodeProfile, service::session::model::Session};

#[allow(dead_code)]
pub struct SessionStatus {
    pub id: Vec<u8>,
    pub handshake_type: HandshakeType,
    pub node_profile: NodeProfile,
    pub session: Session,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeType {
    Unknown,
    Connected,
    Accepted,
}
