use std::{collections::HashMap, sync::Arc};

use chrono::{Duration, Utc};
use core_base::clock::Clock;

use crate::{
    model::{AssetKey, NodeProfile},
    service::{
        session::model::Session,
        util::{VolatileHashMap, VolatileHashSet},
    },
};

#[derive(Clone)]
pub struct SessionStatus {
    pub handshake_type: HandshakeType,
    pub session: Session,
    pub node_profile: NodeProfile,

    pub sending_data_message: Arc<SendingDataMessage>,
    pub received_data_message: Arc<ReceivedDataMessage>,
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
    pub push_node_profiles: VolatileHashSet<NodeProfile>,
    pub want_asset_keys: VolatileHashSet<AssetKey>,
    pub give_asset_key_locations: VolatileHashMap<AssetKey, Vec<NodeProfile>>,
    pub push_asset_key_locations: VolatileHashMap<AssetKey, Vec<NodeProfile>>,
}

impl ReceivedDataMessage {
    pub fn new(clock: Arc<dyn Clock<Utc> + Send + Sync>) -> Self {
        Self {
            push_node_profiles: VolatileHashSet::new(Duration::seconds(60), clock.clone()),
            want_asset_keys: VolatileHashSet::new(Duration::seconds(60), clock.clone()),
            give_asset_key_locations: VolatileHashMap::new(Duration::seconds(60), clock.clone()),
            push_asset_key_locations: VolatileHashMap::new(Duration::seconds(60), clock),
        }
    }
}
