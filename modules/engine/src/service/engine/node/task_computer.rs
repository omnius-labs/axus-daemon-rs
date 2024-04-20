use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex as StdMutex},
};

use tokio::{select, task::JoinHandle};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::{
    model::{AssetKey, NodeProfile},
    service::util::{FnCaller, Kadex},
};

use super::{NodeFinderOptions, NodeProfileFetcher, NodeProfileRepo, SessionStatus};

#[allow(dead_code)]
#[derive(Clone)]
pub struct TaskComputer {
    pub my_node_profile: Arc<StdMutex<NodeProfile>>,
    pub node_profile_repo: Arc<NodeProfileRepo>,
    pub node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
    pub sessions: Arc<StdMutex<Vec<SessionStatus>>>,
    pub get_want_asset_keys_fn: Arc<FnCaller<Vec<AssetKey>>>,
    pub get_push_asset_keys_fn: Arc<FnCaller<Vec<AssetKey>>>,
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
        self.compute_sending_data_message().await?;

        Ok(())
    }

    async fn compute_sending_data_message(&self) -> anyhow::Result<()> {
        let my_node_profile = self.my_node_profile.lock().unwrap().clone();
        let cloud_node_profile = self.node_profile_repo.get_node_profiles().await?;

        let my_get_want_asset_keys: HashSet<AssetKey> = self.get_want_asset_keys_fn.call().into_iter().flatten().collect();
        let my_get_push_asset_keys: HashSet<AssetKey> = self.get_push_asset_keys_fn.call().into_iter().flatten().collect();

        let sessions = self.sessions.lock().unwrap();

        // 全ノードに配布する情報
        let mut push_node_profiles: HashSet<NodeProfile> = HashSet::new();
        push_node_profiles.insert(my_node_profile.clone());
        push_node_profiles.extend(cloud_node_profile);

        // Kadexの距離が近いノードに配布する情報
        let mut want_asset_keys: HashSet<AssetKey> = HashSet::new();
        want_asset_keys.extend(my_get_want_asset_keys);
        for session in sessions.iter() {
            want_asset_keys.extend(session.received_data_message.lock().unwrap().want_asset_keys.clone());
        }

        // Wantリクエストを受けたノードに配布する情報
        let mut give_asset_key_locations: HashMap<AssetKey, HashSet<NodeProfile>> = HashMap::new();
        for asset_key in my_get_push_asset_keys.iter() {
            give_asset_key_locations
                .entry(asset_key.clone())
                .or_default()
                .insert(my_node_profile.clone());
        }
        for session in sessions.iter() {
            let received_data_message = session.received_data_message.lock().unwrap();
            let iter1 = received_data_message.push_asset_key_locations.iter();
            let iter2 = received_data_message.give_asset_key_locations.iter();
            for (asset_key, node_profiles) in iter1.chain(iter2) {
                give_asset_key_locations
                    .entry(asset_key.clone())
                    .or_default()
                    .extend(node_profiles.clone());
            }
        }

        // Kadexの距離が近いノードに配布する情報
        let mut push_asset_key_locations: HashMap<AssetKey, HashSet<NodeProfile>> = HashMap::new();
        for asset_key in my_get_push_asset_keys {
            push_asset_key_locations
                .entry(asset_key.clone())
                .or_default()
                .insert(my_node_profile.clone());
        }
        for session in sessions.iter() {
            let received_data_message = session.received_data_message.lock().unwrap();
            for (asset_key, node_profiles) in &received_data_message.push_asset_key_locations {
                give_asset_key_locations
                    .entry(asset_key.clone())
                    .or_default()
                    .extend(node_profiles.clone());
            }
        }

        // Kadexの距離が近いノードにwant_asset_keyを配布する
        let mut sending_want_asset_key_map: HashMap<&Vec<u8>, Vec<&AssetKey>> = HashMap::new();
        let ids: &[&Vec<u8>] = &sessions.iter().map(|n| &n.node_profile.id).collect::<Vec<&Vec<u8>>>();
        for target_key in &want_asset_keys {
            for id in Kadex::find(&my_node_profile.id, &target_key.hash.value, ids, 1) {
                sending_want_asset_key_map.entry(id).or_default().push(target_key);
            }
        }

        // want_asset_keyを受け取ったノードにgive_asset_key_locationsを配布する
        let mut sending_give_asset_key_location_map: HashMap<&Vec<u8>, HashMap<&AssetKey, Vec<&NodeProfile>>> = HashMap::new();
        for session in sessions.iter() {
            let received_data_message = session.received_data_message.lock().unwrap();
            for target_key in &received_data_message.want_asset_keys {
                if let Some((target_key, node_profiles)) = give_asset_key_locations.get_key_value(target_key) {
                    sending_give_asset_key_location_map
                        .entry(&session.node_profile.id)
                        .or_default()
                        .insert(target_key, node_profiles.iter().collect());
                }
            }
        }

        // Kadexの距離が近いノードにpush_asset_key_locationsを配布する
        let mut sending_push_asset_key_location_map: HashMap<&Vec<u8>, HashMap<&AssetKey, Vec<&NodeProfile>>> = HashMap::new();
        let ids: &[&Vec<u8>] = &sessions.iter().map(|n| &n.node_profile.id).collect::<Vec<&Vec<u8>>>();
        for (target_key, node_profiles) in push_asset_key_locations.iter() {
            for id in Kadex::find(&my_node_profile.id, &target_key.hash.value, ids, 1) {
                sending_push_asset_key_location_map
                    .entry(id)
                    .or_default()
                    .insert(target_key, node_profiles.iter().collect());
            }
        }

        for session in sessions.iter() {
            let mut data_message = session.sending_data_message.lock().unwrap();
            data_message.push_node_profiles = push_node_profiles.clone().into_iter().collect();
            data_message.want_asset_keys = sending_want_asset_key_map
                .get(&session.node_profile.id)
                .unwrap_or(&Vec::new())
                .iter()
                .map(|n| (*n).clone())
                .collect();
            data_message.give_asset_key_locations = sending_give_asset_key_location_map
                .get(&session.node_profile.id)
                .unwrap_or(&HashMap::new())
                .iter()
                .map(|(k, v)| ((*k).clone(), v.iter().map(|n| (*n).clone()).collect()))
                .collect();
            data_message.push_asset_key_locations = sending_push_asset_key_location_map
                .get(&session.node_profile.id)
                .unwrap_or(&HashMap::new())
                .iter()
                .map(|(k, v)| ((*k).clone(), v.iter().map(|n| (*n).clone()).collect()))
                .collect();
        }

        Ok(())
    }
}
