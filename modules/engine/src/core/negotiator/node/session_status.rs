use std::{collections::HashMap, sync::Arc};

use chrono::{Duration, Utc};
use parking_lot::Mutex;

use omnius_core_base::clock::Clock;

use crate::{
    core::{
        session::model::Session,
        util::{VolatileHashMap, VolatileHashSet},
    },
    model::{AssetKey, NodeProfile},
};

#[derive(Clone)]
pub struct SessionStatus {
    pub handshake_type: HandshakeType,
    pub session: Session,
    pub node_profile: NodeProfile,

    pub sending_data_message: Arc<Mutex<SendingDataMessage>>,
    pub received_data_message: Arc<Mutex<ReceivedDataMessage>>,
}

impl SessionStatus {
    pub fn new(handshake_type: HandshakeType, session: Session, node_profile: NodeProfile, clock: Arc<dyn Clock<Utc> + Send + Sync>) -> Self {
        Self {
            handshake_type,
            session,
            node_profile,
            sending_data_message: Arc::new(Mutex::new(SendingDataMessage::new())),
            received_data_message: Arc::new(Mutex::new(ReceivedDataMessage::new(clock))),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandshakeType {
    Unknown,
    Connected,
    Accepted,
}

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

pub struct ReceivedDataMessage {
    pub want_asset_keys: VolatileHashSet<Arc<AssetKey>>,
    pub give_asset_key_locations: VolatileHashMap<Arc<AssetKey>, Vec<Arc<NodeProfile>>>,
    pub push_asset_key_locations: VolatileHashMap<Arc<AssetKey>, Vec<Arc<NodeProfile>>>,
}

impl ReceivedDataMessage {
    pub fn new(clock: Arc<dyn Clock<Utc> + Send + Sync>) -> Self {
        Self {
            want_asset_keys: VolatileHashSet::new(Duration::minutes(30), clock.clone()),
            give_asset_key_locations: VolatileHashMap::new(Duration::minutes(30), clock.clone()),
            push_asset_key_locations: VolatileHashMap::new(Duration::minutes(30), clock),
        }
    }
}
