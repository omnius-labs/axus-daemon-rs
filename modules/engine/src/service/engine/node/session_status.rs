use std::{
    collections::HashMap,
    sync::{Arc, Mutex as StdMutex},
};

use serde::{Deserialize, Serialize};

use crate::{
    model::{AssetKey, NodeProfile},
    service::session::model::Session,
};

#[allow(dead_code)]
pub struct SessionStatus {
    pub handshake_type: HandshakeType,
    pub session: Session,
    pub node_profile: NodeProfile,

    pub sending_data_message: Arc<StdMutex<SendingDataMessage>>,
    pub received_data_message: Arc<StdMutex<ReceivedDataMessage>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeType {
    Unknown,
    Connected,
    Accepted,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendingDataMessage {
    pub push_node_profiles: Vec<NodeProfile>,
    pub want_asset_keys: Vec<AssetKey>,
    pub give_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
    pub push_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
}

impl SendingDataMessage {
    pub fn new() -> Self {
        Self {
            push_node_profiles: vec![],
            want_asset_keys: vec![],
            give_asset_key_locations: HashMap::new(),
            push_asset_key_locations: HashMap::new(),
        }
    }
}

impl Default for SendingDataMessage {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReceivedDataMessage {
    pub push_node_profiles: Vec<NodeProfile>,
    pub want_asset_keys: Vec<AssetKey>,
    pub give_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
    pub push_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>>,
}

impl ReceivedDataMessage {
    pub fn new() -> Self {
        Self {
            push_node_profiles: vec![],
            want_asset_keys: vec![],
            give_asset_key_locations: HashMap::new(),
            push_asset_key_locations: HashMap::new(),
        }
    }
}

impl Default for ReceivedDataMessage {
    fn default() -> Self {
        Self::new()
    }
}
