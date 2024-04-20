use std::collections::HashMap;

use crate::{
    model::{AssetKey, NodeProfile},
    service::{connection::AsyncSendRecvExt, session::model::Session},
};

#[allow(dead_code)]
pub struct Communicator {}

impl Communicator {
    pub async fn handshake(session: &Session, node_profile: &NodeProfile) -> anyhow::Result<NodeProfile> {
        let stream = session.stream.clone();

        let send_hello_message = HelloMessage {
            version: NodeFinderVersion::V1,
        };
        stream.lock().await.send_message(&send_hello_message).await?;
        let received_hello_message: HelloMessage = stream.lock().await.recv_message().await?;

        let version = send_hello_message.version | received_hello_message.version;

        if version.contains(NodeFinderVersion::V1) {
            let send_profile_message = ProfileMessage {
                node_profile: node_profile.clone(),
            };
            stream.lock().await.send_message(&send_profile_message).await?;
            let received_profile_message: ProfileMessage = stream.lock().await.recv_message().await?;

            Ok(received_profile_message.node_profile)
        } else {
            anyhow::bail!("Invalid version")
        }
    }
}

use bitflags::bitflags;
use serde::{Deserialize, Serialize};

bitflags! {
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
      struct NodeFinderVersion: u32 {
        const V1 = 1;
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct HelloMessage {
    pub version: NodeFinderVersion,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct ProfileMessage {
    pub node_profile: NodeProfile,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
struct DataMessage {
    pub push_node_profiles: Vec<NodeProfile>,
    pub want_asset_keys: Vec<AssetKey>,
    pub give_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
    pub push_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
}

impl DataMessage {
    pub fn new() -> Self {
        Self {
            push_node_profiles: vec![],
            want_asset_keys: vec![],
            give_asset_key_locations: HashMap::new(),
            push_asset_key_locations: HashMap::new(),
        }
    }
}

impl Default for DataMessage {
    fn default() -> Self {
        Self::new()
    }
}
