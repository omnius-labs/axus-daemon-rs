use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
};

use tokio::{select, sync::RwLock as TokioRwLock, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    model::{AssetKey, NodeProfile},
    service::util::{FnExecutor, Kadex},
};

use super::{NodeFinderOptions, NodeProfileFetcher, NodeProfileRepo, ReceivedDataMessage, SendingDataMessage, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskComputer {
    pub my_node_profile: Arc<StdMutex<NodeProfile>>,
    pub node_profile_repo: Arc<NodeProfileRepo>,
    pub node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
    pub sessions: Arc<TokioRwLock<Vec<SessionStatus>>>,
    pub get_want_asset_keys_fn: Arc<FnExecutor<Vec<AssetKey>, ()>>,
    pub get_push_asset_keys_fn: Arc<FnExecutor<Vec<AssetKey>, ()>>,
    pub option: NodeFinderOptions,
}

#[allow(dead_code)]
impl TaskComputer {
    pub async fn run(self, cancellation_token: CancellationToken) -> JoinHandle<()> {
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = self.set_initial_node_profile().await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }

                    loop {
                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        let res = self.compute().await;
                        if let Err(e) = res {
                            warn!("{:?}", e);
                        }
                    }
                } => {}
            }
        })
    }

    async fn set_initial_node_profile(&self) -> anyhow::Result<()> {
        let node_profile = self.node_profile_fetcher.fetch().await?;
        self.node_profile_repo.insert_bulk_node_profile(&node_profile, 0).await?;

        Ok(())
    }

    async fn compute(&self) -> anyhow::Result<()> {
        // self.compute_sending_data_message().await?;

        Ok(())
    }

    async fn compute_sending_data_message(&self) -> anyhow::Result<()> {
        let my_node_profile = self.my_node_profile.lock().unwrap().clone();
        let cloud_node_profile = self.node_profile_repo.get_node_profiles().await?;

        let my_get_want_asset_keys: HashSet<AssetKey> = self.get_want_asset_keys_fn.execute(&()).into_iter().flatten().collect();
        let my_get_push_asset_keys: HashSet<AssetKey> = self.get_push_asset_keys_fn.execute(&()).into_iter().flatten().collect();

        let mut session_map: HashMap<Vec<u8>, Arc<ReceivedDataMessage>> = HashMap::new();
        {
            let sessions = self.sessions.read().await;
            for session in sessions.iter() {
                session_map.insert(session.node_profile.id.clone(), session.received_data_message.clone());
            }
        }

        // 全ノードに配布する情報
        let mut push_node_profiles: HashSet<&NodeProfile> = HashSet::new();
        push_node_profiles.insert(&my_node_profile);
        push_node_profiles.extend(cloud_node_profile.iter());

        // Kadexの距離が近いノードに配布する情報
        let mut want_asset_keys: HashSet<&AssetKey> = HashSet::new();
        want_asset_keys.extend(my_get_want_asset_keys.iter());
        for data in session_map.values() {
            want_asset_keys.extend(data.want_asset_keys.iter());
        }

        // Wantリクエストを受けたノードに配布する情報
        let mut give_asset_key_locations: HashMap<&AssetKey, HashSet<&NodeProfile>> = HashMap::new();
        for asset_key in my_get_push_asset_keys.iter() {
            give_asset_key_locations.entry(asset_key).or_default().insert(&my_node_profile);
        }
        for data in session_map.values() {
            let iter1 = data.push_asset_key_locations.iter();
            let iter2 = data.give_asset_key_locations.iter();
            for (asset_key, node_profiles) in iter1.chain(iter2) {
                give_asset_key_locations.entry(asset_key).or_default().extend(node_profiles.iter());
            }
        }

        // Kadexの距離が近いノードに配布する情報
        let mut push_asset_key_locations: HashMap<&AssetKey, HashSet<&NodeProfile>> = HashMap::new();
        for asset_key in my_get_push_asset_keys.iter() {
            push_asset_key_locations.entry(asset_key).or_default().insert(&my_node_profile);
        }
        for data in session_map.values() {
            for (asset_key, node_profiles) in data.push_asset_key_locations.iter() {
                give_asset_key_locations.entry(asset_key).or_default().extend(node_profiles.iter());
            }
        }

        // Kadexの距離が近いノードにwant_asset_keyを配布する
        let mut sending_want_asset_key_map: HashMap<&[u8], Vec<&AssetKey>> = HashMap::new();
        let ids: Vec<&[u8]> = session_map.keys().map(|n| n.as_slice()).collect();
        for target_key in want_asset_keys {
            for id in Kadex::find(&my_node_profile.id, &target_key.hash.value, &ids, 1) {
                sending_want_asset_key_map.entry(id).or_default().push(target_key);
            }
        }

        // want_asset_keyを受け取ったノードにgive_asset_key_locationsを配布する
        let mut sending_give_asset_key_location_map: HashMap<&[u8], HashMap<&AssetKey, &HashSet<&NodeProfile>>> = HashMap::new();
        for (id, data) in session_map.iter() {
            for target_key in data.want_asset_keys.iter() {
                if let Some((target_key, node_profiles)) = give_asset_key_locations.get_key_value(target_key) {
                    sending_give_asset_key_location_map
                        .entry(id)
                        .or_default()
                        .insert(target_key, node_profiles);
                }
            }
        }

        // Kadexの距離が近いノードにpush_asset_key_locationsを配布する
        let mut sending_push_asset_key_location_map: HashMap<&[u8], HashMap<&AssetKey, &HashSet<&NodeProfile>>> = HashMap::new();
        let ids: Vec<&[u8]> = session_map.keys().map(|n| n.as_slice()).collect();
        for (target_key, node_profiles) in push_asset_key_locations.iter() {
            for id in Kadex::find(&my_node_profile.id, &target_key.hash.value, &ids, 1) {
                sending_push_asset_key_location_map
                    .entry(id)
                    .or_default()
                    .insert(target_key, node_profiles);
            }
        }

        // Session毎にデータを実体化する
        let mut data_map: HashMap<Vec<u8>, Arc<SendingDataMessage>> = HashMap::new();

        let push_node_profiles: Vec<NodeProfile> = push_node_profiles.into_iter().cloned().collect();

        for id in session_map.keys() {
            let want_asset_keys = sending_want_asset_key_map
                .get(id.as_slice())
                .unwrap_or(&Vec::new())
                .iter()
                .map(|n| (*n).clone())
                .collect();
            let give_asset_key_locations = sending_give_asset_key_location_map
                .get(id.as_slice())
                .unwrap_or(&HashMap::new())
                .iter()
                .map(|(k, v)| ((*k).clone(), v.iter().map(|n| (*n).clone()).collect()))
                .collect();
            let push_asset_key_locations = sending_push_asset_key_location_map
                .get(id.as_slice())
                .unwrap_or(&HashMap::new())
                .iter()
                .map(|(k, v)| ((*k).clone(), v.iter().map(|n| (*n).clone()).collect()))
                .collect();
            let data_message = SendingDataMessage {
                push_node_profiles: push_node_profiles.clone(),
                want_asset_keys,
                give_asset_key_locations,
                push_asset_key_locations,
            };
            data_map.insert(id.clone(), Arc::new(data_message));
        }

        // Session毎に送信用データを格納する
        {
            let mut sessions = self.sessions.write().await;
            for session in sessions.iter_mut() {
                if let Some(data_message) = data_map.get(&session.node_profile.id) {
                    session.sending_data_message = data_message.clone();
                }
            }
        }

        Ok(())
    }
}
