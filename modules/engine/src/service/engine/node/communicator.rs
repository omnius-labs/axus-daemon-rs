use crate::{model::NodeProfile, service::session::model::Session};

#[allow(dead_code)]
pub struct Communicator {}

impl Communicator {
    pub async fn handshake(_session: &Session) -> anyhow::Result<(Vec<u8>, NodeProfile)> {
        todo!()
    }
}
