use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use chrono::{Duration, Utc};
use futures::future::join_all;
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use tokio::sync::{Mutex as TokioMutex, RwLock as TokioRwLock, mpsc};

use omnius_core_base::{clock::Clock, sleeper::Sleeper};

use crate::{
    core::{
        connection::{ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl},
        session::{SessionAccepter, SessionConnector, model::Session},
        util::{FnHub, Terminable, VolatileHashSet},
    },
    model::{AssetKey, NodeProfile},
    prelude::*,
};

use super::*;

#[allow(dead_code)]
pub struct NodeFinder {
    my_node_profile: Arc<Mutex<NodeProfile>>,
    tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync>,
    tcp_accepter: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_profile_repo: Arc<NodeFinderRepo>,
    node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
    clock: Arc<dyn Clock<Utc> + Send + Sync>,
    sleeper: Arc<dyn Sleeper + Send + Sync>,
    option: NodeFinderOption,

    session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    sessions: Arc<TokioRwLock<HashMap<Vec<u8>, Arc<SessionStatus>>>>,
    connected_node_profiles: Arc<Mutex<VolatileHashSet<NodeProfile>>>,
    get_want_asset_keys_fn: Arc<FnHub<Vec<AssetKey>, ()>>,
    get_push_asset_keys_fn: Arc<FnHub<Vec<AssetKey>, ()>>,

    task_connectors: Arc<TokioMutex<Vec<Arc<TaskConnector>>>>,
    task_acceptors: Arc<TokioMutex<Vec<Arc<TaskAccepter>>>>,
    task_computer: Arc<TokioMutex<Option<Arc<TaskComputer>>>>,
    task_communicator: Arc<TokioMutex<Option<Arc<TaskCommunicator>>>>,
}

#[derive(Debug, Clone)]
pub struct NodeFinderOption {
    pub state_dir_path: String,
    pub max_connected_session_count: usize,
    pub max_accepted_session_count: usize,
}

impl NodeFinder {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync>,
        tcp_accepter: Arc<dyn ConnectionTcpAccepter + Send + Sync>,
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_profile_repo: Arc<NodeFinderRepo>,
        node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
        clock: Arc<dyn Clock<Utc> + Send + Sync>,
        sleeper: Arc<dyn Sleeper + Send + Sync>,
        option: NodeFinderOption,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(20);

        let v = Self {
            my_node_profile: Arc::new(Mutex::new(NodeProfile {
                id: Self::gen_id(),
                addrs: Vec::new(),
            })),
            tcp_connector,
            tcp_accepter,
            session_connector,
            session_accepter,
            node_profile_repo,
            node_profile_fetcher,
            clock: clock.clone(),
            sleeper,
            option,

            session_receiver: Arc::new(TokioMutex::new(rx)),
            session_sender: Arc::new(TokioMutex::new(tx)),
            sessions: Arc::new(TokioRwLock::new(HashMap::new())),
            connected_node_profiles: Arc::new(Mutex::new(VolatileHashSet::new(Duration::seconds(180), clock))),
            get_want_asset_keys_fn: Arc::new(FnHub::new()),
            get_push_asset_keys_fn: Arc::new(FnHub::new()),

            task_connectors: Arc::new(TokioMutex::new(Vec::new())),
            task_acceptors: Arc::new(TokioMutex::new(Vec::new())),
            task_computer: Arc::new(TokioMutex::new(None)),
            task_communicator: Arc::new(TokioMutex::new(None)),
        };
        v.start().await?;

        Ok(v)
    }

    pub async fn get_session_count(&self) -> usize {
        self.sessions.read().await.len()
    }

    fn gen_id() -> Vec<u8> {
        let mut rng = ChaCha20Rng::from_entropy();
        let mut id = [0_u8, 32];
        rng.fill_bytes(&mut id);
        id.to_vec()
    }

    async fn start(&self) -> Result<()> {
        for _ in 0..3 {
            let task = TaskConnector::new(
                self.sessions.clone(),
                self.session_sender.clone(),
                self.session_connector.clone(),
                self.connected_node_profiles.clone(),
                self.node_profile_repo.clone(),
                self.sleeper.clone(),
                self.option.clone(),
            )
            .await?;
            self.task_connectors.lock().await.push(task);
        }

        for _ in 0..3 {
            let task = TaskAccepter::new(
                self.sessions.clone(),
                self.session_sender.clone(),
                self.session_accepter.clone(),
                self.sleeper.clone(),
                self.option.clone(),
            )
            .await?;
            self.task_acceptors.lock().await.push(task);
        }

        let task = TaskComputer::new(
            self.my_node_profile.clone(),
            self.node_profile_repo.clone(),
            self.node_profile_fetcher.clone(),
            self.sessions.clone(),
            self.get_want_asset_keys_fn.caller(),
            self.get_push_asset_keys_fn.caller(),
            self.sleeper.clone(),
            self.option.clone(),
        )
        .await?;
        self.task_computer.lock().await.replace(task);

        let task = TaskCommunicator::new(
            self.my_node_profile.clone(),
            self.sessions.clone(),
            self.node_profile_repo.clone(),
            self.session_receiver.clone(),
            self.clock.clone(),
            self.sleeper.clone(),
            self.option.clone(),
        )
        .await?;
        self.task_communicator.lock().await.replace(task);

        Ok(())
    }
}

#[async_trait]
impl Terminable for NodeFinder {
    async fn terminate(&self) {
        {
            let mut task_connectors = self.task_connectors.lock().await;
            let task_connectors: Vec<Arc<TaskConnector>> = task_connectors.drain(..).collect();
            join_all(task_connectors.iter().map(|task| task.terminate())).await;
        }

        {
            let mut task_acceptors = self.task_acceptors.lock().await;
            let task_acceptors: Vec<Arc<TaskAccepter>> = task_acceptors.drain(..).collect();
            join_all(task_acceptors.iter().map(|task| task.terminate())).await;
        }

        {
            let mut task_computer = self.task_computer.lock().await;
            if let Some(task_computer) = task_computer.take() {
                task_computer.terminate().await;
            }
        }

        {
            let mut task_communicator = self.task_communicator.lock().await;
            if let Some(task_communicator) = task_communicator.take() {
                task_communicator.terminate().await;
            }
        }

        self.session_accepter.terminate().await;
        self.tcp_accepter.terminate().await;
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc};

    use chrono::Utc;
    use omnius_core_base::{
        clock::{Clock, ClockUtc},
        random_bytes::RandomBytesProviderImpl,
        sleeper::{Sleeper, SleeperImpl},
    };
    use parking_lot::Mutex;
    use testresult::TestResult;
    use tracing::info;

    use omnius_core_omnikit::model::{OmniAddr, OmniSignType, OmniSigner};

    use crate::core::{
        connection::{TcpProxyOption, TcpProxyType},
        negotiator::NodeProfileFetcherMock,
    };

    use super::*;

    #[ignore]
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn simple_test() -> TestResult {
        tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).with_target(false).init();

        let dir = tempfile::tempdir()?;

        let np1 = NodeProfile {
            id: "1".as_bytes().to_vec(),
            addrs: vec![OmniAddr::new("tcp(ip4(127.0.0.1),60001)")],
        };
        let np2 = NodeProfile {
            id: "2".as_bytes().to_vec(),
            addrs: vec![OmniAddr::new("tcp(ip4(127.0.0.1),60002)")],
        };

        let nf1_path = dir.path().join("1");
        fs::create_dir_all(&nf1_path)?;

        let nf1 = create_node_finder(&nf1_path, "1", 60001, np2).await?;

        let nf2_path = dir.path().join("2");
        fs::create_dir_all(&nf2_path)?;

        let nf2 = create_node_finder(&nf2_path, "2", 60002, np1).await?;

        loop {
            let nf1_session_count = nf1.get_session_count().await;
            let nf2_session_count = nf2.get_session_count().await;

            if nf1_session_count == 1 && nf2_session_count == 1 {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            info!(nf1_session_count, nf2_session_count, "wait");
        }
        info!("done");

        nf1.terminate().await;
        nf2.terminate().await;

        Ok(())
    }

    async fn create_node_finder(dir_path: &Path, name: &str, port: u16, other_node_profile: NodeProfile) -> Result<NodeFinder> {
        let tcp_accepter: Arc<dyn ConnectionTcpAccepter + Send + Sync> =
            Arc::new(ConnectionTcpAccepterImpl::new(&OmniAddr::create_tcp("127.0.0.1".parse()?, port), false).await?);
        let tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync> = Arc::new(
            ConnectionTcpConnectorImpl::new(TcpProxyOption {
                typ: TcpProxyType::None,
                addr: None,
            })
            .await?,
        );

        let clock: Arc<dyn Clock<Utc> + Send + Sync> = Arc::new(ClockUtc);
        let sleeper: Arc<dyn Sleeper + Send + Sync> = Arc::new(SleeperImpl);
        let signer = Arc::new(OmniSigner::new(OmniSignType::Ed25519_Sha3_256_Base64Url, name)?);
        let random_bytes_provider = Arc::new(Mutex::new(RandomBytesProviderImpl::new()));

        let session_accepter =
            Arc::new(SessionAccepter::new(tcp_accepter.clone(), signer.clone(), random_bytes_provider.clone(), sleeper.clone()).await);
        let session_connector = Arc::new(SessionConnector::new(tcp_connector.clone(), signer, random_bytes_provider));

        let node_ref_repo_dir = dir_path.join(name).join("repo");
        fs::create_dir_all(&node_ref_repo_dir)?;

        let node_profile_repo = Arc::new(NodeFinderRepo::new(node_ref_repo_dir.as_os_str().to_str().unwrap(), clock.clone()).await?);

        let node_profile_fetcher = Arc::new(NodeProfileFetcherMock {
            node_profiles: vec![other_node_profile],
        });

        let node_finder_dir = dir_path.join(name).join("finder");
        fs::create_dir_all(&node_finder_dir)?;

        let result = NodeFinder::new(
            tcp_connector,
            tcp_accepter,
            session_connector,
            session_accepter,
            node_profile_repo,
            node_profile_fetcher,
            clock,
            sleeper,
            NodeFinderOption {
                state_dir_path: node_finder_dir.as_os_str().to_str().unwrap().to_string(),
                max_connected_session_count: 3,
                max_accepted_session_count: 3,
            },
        )
        .await?;

        Ok(result)
    }
}
