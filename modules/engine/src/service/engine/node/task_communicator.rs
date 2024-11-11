use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use bitflags::bitflags;
use chrono::Utc;
use futures::FutureExt;
use omnius_core_rocketpack::{RocketMessage, RocketMessageReader, RocketMessageWriter};
use parking_lot::Mutex;
use tokio::{
    select,
    sync::{mpsc, Mutex as TokioMutex, RwLock as TokioRwLock},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use omnius_core_base::{clock::Clock, sleeper::Sleeper, terminable::Terminable};

use crate::{
    model::{AssetKey, NodeProfile},
    service::{
        connection::{FramedRecvExt as _, FramedSendExt as _},
        session::model::Session,
        util::WaitGroup,
    },
};

use super::{HandshakeType, NodeProfileRepo, SessionStatus};

#[derive(Clone)]
pub struct TaskCommunicator {
    session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    inner: Inner,
    join_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    wait_group: Arc<WaitGroup>,
    cancellation_token: CancellationToken,
}

#[async_trait]
impl Terminable for TaskCommunicator {
    async fn terminate(&self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        if let Some(join_handle) = self.join_handle.lock().await.take() {
            join_handle.abort();
            let _ = join_handle.fuse().await;
        }
        self.wait_group.wait().await;

        Ok(())
    }
}

impl TaskCommunicator {
    pub fn new(
        my_node_profile: Arc<Mutex<NodeProfile>>,
        sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
        node_profile_repo: Arc<NodeProfileRepo>,
        session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> Self {
        let wait_group = Arc::new(WaitGroup::new());
        let cancellation_token = CancellationToken::new();
        let inner = Inner {
            my_node_profile,
            sessions,
            node_profile_repo,
            clock,
            sleeper,
            wait_group: wait_group.clone(),
            cancellation_token: cancellation_token.clone(),
        };
        Self {
            session_receiver,
            inner,
            join_handle: Arc::new(TokioMutex::new(None)),
            wait_group,
            cancellation_token,
        }
    }

    pub async fn run(&self) {
        let session_receiver = self.session_receiver.clone();
        let inner = self.inner.clone();
        let join_handle = tokio::spawn(async move {
            loop {
                if let Some((handshake_type, session)) = session_receiver.lock().await.recv().await {
                    let res = inner.clone().communicate(handshake_type, session);
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                }
            }
        });
        *self.join_handle.lock().await = Some(join_handle);
    }
}

#[derive(Clone)]
struct Inner {
    my_node_profile: Arc<Mutex<NodeProfile>>,
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, SessionStatus>>>,
    node_profile_repo: Arc<NodeProfileRepo>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,

    wait_group: Arc<WaitGroup>,
    cancellation_token: CancellationToken,
}

impl Inner {
    pub fn communicate(self, handshake_type: HandshakeType, session: Session) -> anyhow::Result<()> {
        let w = self.wait_group.worker();
        tokio::spawn(async move {
            select! {
                _ = self.cancellation_token.cancelled() => {}
                _ = async {
                    let res = self.communicate_sub(handshake_type, session).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
            drop(w);
        });
        Ok(())
    }

    async fn communicate_sub(&self, handshake_type: HandshakeType, session: Session) -> anyhow::Result<()> {
        let my_node_profile = self.my_node_profile.lock().clone();

        let node_profile = Self::handshake(&session, &my_node_profile).await?;
        let status = SessionStatus::new(handshake_type, session, node_profile, self.clock.clone());

        {
            let mut sessions = self.sessions.write().await;
            if sessions.contains_key(&status.node_profile.id) {
                return Err(anyhow::anyhow!("Session already exists"));
            }
            sessions.insert(status.node_profile.id.clone(), status.clone());
        }

        info!("Session established: {}", status.node_profile);

        let send = self.send(&status);
        let receive = self.receive(&status);

        let _ = tokio::join!(send, receive);

        {
            let mut sessions = self.sessions.write().await;
            sessions.remove(&status.node_profile.id);
        }

        Ok(())
    }

    pub async fn handshake(session: &Session, node_profile: &NodeProfile) -> anyhow::Result<NodeProfile> {
        let send_hello_message = HelloMessage {
            version: NodeFinderVersion::V1,
        };
        session.stream.sender.lock().await.send_message(&send_hello_message).await?;
        let received_hello_message: HelloMessage = session.stream.receiver.lock().await.recv_message().await?;

        let version = send_hello_message.version | received_hello_message.version;

        if version.contains(NodeFinderVersion::V1) {
            let send_profile_message = ProfileMessage {
                node_profile: node_profile.clone(),
            };
            session.stream.sender.lock().await.send_message(&send_profile_message).await?;
            let received_profile_message: ProfileMessage = session.stream.receiver.lock().await.recv_message().await?;

            Ok(received_profile_message.node_profile)
        } else {
            anyhow::bail!("Invalid version")
        }
    }

    fn send(&self, status: &SessionStatus) -> JoinHandle<()> {
        let status = status.clone();
        let sleeper = self.sleeper.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = Self::send_sub(status, sleeper).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
        })
    }

    async fn send_sub(status: SessionStatus, sleeper: Arc<dyn Sleeper + Send + Sync>) -> anyhow::Result<()> {
        loop {
            sleeper.sleep(std::time::Duration::from_secs(30)).await;

            let data_message = {
                let mut sending_data_message = status.sending_data_message.lock();
                DataMessage {
                    push_node_profiles: sending_data_message.push_node_profiles.drain(..).collect(),
                    want_asset_keys: sending_data_message.want_asset_keys.drain(..).collect(),
                    give_asset_key_locations: sending_data_message.give_asset_key_locations.drain().collect(),
                    push_asset_key_locations: sending_data_message.push_asset_key_locations.drain().collect(),
                }
            };

            status.session.stream.sender.lock().await.send_message(&data_message).await?;
        }
    }

    fn receive(&self, status: &SessionStatus) -> JoinHandle<()> {
        let status = status.clone();
        let node_profile_repo = self.node_profile_repo.clone();
        let sleeper = self.sleeper.clone();
        let cancellation_token = self.cancellation_token.clone();
        tokio::spawn(async move {
            select! {
                _ = cancellation_token.cancelled() => {}
                _ = async {
                    let res = Self::receive_sub(status, node_profile_repo, sleeper).await;
                    if let Err(e) = res {
                        warn!("{:?}", e);
                    }
                } => {}
            }
        })
    }

    async fn receive_sub(
        status: SessionStatus,
        node_profile_repo: Arc<NodeProfileRepo>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
    ) -> anyhow::Result<()> {
        loop {
            sleeper.sleep(std::time::Duration::from_secs(20)).await;

            let data_message = status.session.stream.receiver.lock().await.recv_message::<DataMessage>().await?;

            let push_node_profiles: Vec<&NodeProfile> = data_message.push_node_profiles.iter().take(32).collect();
            node_profile_repo.insert_bulk_node_profile(&push_node_profiles, 0).await?;
            node_profile_repo.shrink(1024).await?;

            {
                let mut received_data_message = status.received_data_message.lock();
                received_data_message
                    .want_asset_keys
                    .extend(data_message.want_asset_keys.into_iter().map(Arc::new));
                received_data_message.give_asset_key_locations.extend(
                    data_message
                        .give_asset_key_locations
                        .into_iter()
                        .map(|(k, v)| (Arc::new(k), v.into_iter().map(Arc::new).collect())),
                );
                received_data_message.push_asset_key_locations.extend(
                    data_message
                        .push_asset_key_locations
                        .into_iter()
                        .map(|(k, v)| (Arc::new(k), v.into_iter().map(Arc::new).collect())),
                );

                received_data_message.want_asset_keys.shrink(1024 * 256);
                received_data_message.give_asset_key_locations.shrink(1024 * 256);
                received_data_message.push_asset_key_locations.shrink(1024 * 256);
            }
        }
    }
}

bitflags! {
    #[derive(Debug, PartialEq, Eq )]
      struct NodeFinderVersion: u32 {
        const V1 = 1;
    }
}

#[derive(Debug, PartialEq, Eq)]
struct HelloMessage {
    pub version: NodeFinderVersion,
}

impl RocketMessage for HelloMessage {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, _depth: u32) -> anyhow::Result<()> {
        writer.write_u32(value.version.bits());

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, _depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let version = NodeFinderVersion::from_bits(reader.get_u32()?).ok_or_else(|| anyhow::anyhow!("invalid version"))?;

        Ok(Self { version })
    }
}

#[derive(Debug, PartialEq, Eq)]
struct ProfileMessage {
    pub node_profile: NodeProfile,
}

impl RocketMessage for ProfileMessage {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, depth: u32) -> anyhow::Result<()> {
        NodeProfile::pack(writer, &value.node_profile, depth + 1)?;

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let node_profile = NodeProfile::unpack(reader, depth + 1)?;

        Ok(Self { node_profile })
    }
}

#[derive(Debug, PartialEq, Eq)]
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

impl RocketMessage for DataMessage {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, depth: u32) -> anyhow::Result<()> {
        writer.write_u32(value.push_node_profiles.len().try_into()?);
        for v in &value.push_node_profiles {
            NodeProfile::pack(writer, v, depth + 1)?;
        }

        writer.write_u32(value.want_asset_keys.len().try_into()?);
        for v in &value.want_asset_keys {
            AssetKey::pack(writer, v, depth + 1)?;
        }

        writer.write_u32(value.give_asset_key_locations.len().try_into()?);
        for (key, vs) in &value.give_asset_key_locations {
            AssetKey::pack(writer, key, depth + 1)?;
            writer.write_u32(vs.len().try_into()?);
            for v in vs {
                NodeProfile::pack(writer, v, depth + 1)?;
            }
        }

        writer.write_u32(value.push_asset_key_locations.len().try_into()?);
        for (key, vs) in &value.push_asset_key_locations {
            AssetKey::pack(writer, key, depth + 1)?;
            writer.write_u32(vs.len().try_into()?);
            for v in vs {
                NodeProfile::pack(writer, v, depth + 1)?;
            }
        }

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let len = reader.get_u32()?.try_into()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut push_node_profiles = Vec::with_capacity(len);
        for _ in 0..len {
            push_node_profiles.push(NodeProfile::unpack(reader, depth + 1)?);
        }

        let len = reader.get_u32()?.try_into()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut want_asset_keys = Vec::with_capacity(len);
        for _ in 0..len {
            want_asset_keys.push(AssetKey::unpack(reader, depth + 1)?);
        }

        let len = reader.get_u32()?.try_into()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut give_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>> = HashMap::new();
        for _ in 0..len {
            let key = AssetKey::unpack(reader, depth + 1)?;
            let len = reader.get_u32()?.try_into()?;
            if len > 128 {
                anyhow::bail!("len too large");
            }
            let mut vs = Vec::with_capacity(len);
            for _ in 0..len {
                vs.push(NodeProfile::unpack(reader, depth + 1)?);
            }
            give_asset_key_locations.entry(key).or_default().extend(vs);
        }

        let len = reader.get_u32()?.try_into()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut push_asset_key_locations: HashMap<AssetKey, Vec<NodeProfile>> = HashMap::new();
        for _ in 0..len {
            let key = AssetKey::unpack(reader, depth + 1)?;
            let len = reader.get_u32()?.try_into()?;
            if len > 128 {
                anyhow::bail!("len too large");
            }
            let mut vs = Vec::with_capacity(len);
            for _ in 0..len {
                vs.push(NodeProfile::unpack(reader, depth + 1)?);
            }
            push_asset_key_locations.entry(key).or_default().extend(vs);
        }

        Ok(Self {
            push_node_profiles,
            want_asset_keys,
            give_asset_key_locations,
            push_asset_key_locations,
        })
    }
}
