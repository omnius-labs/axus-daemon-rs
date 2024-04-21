use std::sync::{Arc, Mutex as StdMutex};

use chrono::Duration;
use core_base::clock::SystemClock;
use futures::future::{join_all, JoinAll};
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use sqlx::types::chrono::Utc;
use tokio::{
    sync::{mpsc, Mutex as TokioMutex},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{
    model::{AssetKey, NodeProfile},
    service::{
        session::{model::Session, SessionAccepter, SessionConnector},
        util::{FnHub, VolatileHashSet},
    },
};

use super::{HandshakeType, NodeProfileFetcher, NodeProfileRepo, SessionStatus, TaskAccepter, TaskCommunicator, TaskComputer, TaskConnector};

#[allow(dead_code)]
pub struct NodeFinder {
    my_node_profile: Arc<StdMutex<NodeProfile>>,
    session_connector: Arc<SessionConnector>,
    session_accepter: Arc<SessionAccepter>,
    node_profile_repo: Arc<NodeProfileRepo>,
    node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
    system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
    option: NodeFinderOptions,
    cancellation_token: CancellationToken,

    session_receiver: Arc<TokioMutex<mpsc::Receiver<(HandshakeType, Session)>>>,
    session_sender: Arc<TokioMutex<mpsc::Sender<(HandshakeType, Session)>>>,
    sessions: Arc<StdMutex<Vec<SessionStatus>>>,
    connected_node_profiles: Arc<StdMutex<VolatileHashSet<NodeProfile>>>,
    get_want_asset_keys_fn: Arc<FnHub<Vec<AssetKey>, ()>>,
    get_push_asset_keys_fn: Arc<FnHub<Vec<AssetKey>, ()>>,
    join_handles: Arc<TokioMutex<Option<JoinAll<tokio::task::JoinHandle<()>>>>>,
}

#[derive(Debug, Clone)]
pub struct NodeFinderOptions {
    pub state_dir_path: String,
    pub max_connected_session_count: usize,
    pub max_accepted_session_count: usize,
}
impl NodeFinder {
    pub async fn new(
        session_connector: Arc<SessionConnector>,
        session_accepter: Arc<SessionAccepter>,
        node_profile_repo: Arc<NodeProfileRepo>,
        node_profile_fetcher: Arc<dyn NodeProfileFetcher + Send + Sync>,
        system_clock: Arc<dyn SystemClock<Utc> + Send + Sync>,
        option: NodeFinderOptions,
    ) -> Self {
        let cancellation_token = CancellationToken::new();
        let (tx, rx) = mpsc::channel(20);

        let result = Self {
            my_node_profile: Arc::new(StdMutex::new(NodeProfile {
                id: Self::gen_id(),
                addrs: Vec::new(),
            })),
            session_connector,
            session_accepter,
            node_profile_repo,
            node_profile_fetcher,
            system_clock: system_clock.clone(),
            option,
            cancellation_token,

            session_receiver: Arc::new(TokioMutex::new(rx)),
            session_sender: Arc::new(TokioMutex::new(tx)),
            sessions: Arc::new(StdMutex::new(Vec::new())),
            connected_node_profiles: Arc::new(StdMutex::new(VolatileHashSet::new(Duration::seconds(180), system_clock))),
            get_want_asset_keys_fn: Arc::new(FnHub::new()),
            get_push_asset_keys_fn: Arc::new(FnHub::new()),
            join_handles: Arc::new(TokioMutex::new(None)),
        };
        result.run().await;

        result
    }

    pub async fn get_session_count(&self) -> usize {
        self.sessions.lock().unwrap().len()
    }

    fn gen_id() -> Vec<u8> {
        let mut rng = ChaCha20Rng::from_entropy();
        let mut id = [0_u8, 32];
        rng.fill_bytes(&mut id);
        id.to_vec()
    }

    async fn run(&self) {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        join_handles.extend(self.create_connect_task().await);
        join_handles.extend(self.create_accept_task().await);
        join_handles.extend(self.create_communicator_task().await);
        join_handles.extend(self.create_computer_task().await);

        *self.join_handles.as_ref().lock().await = Some(join_all(join_handles));
    }

    async fn create_connect_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskConnector {
            sessions: self.sessions.clone(),
            session_sender: self.session_sender.clone(),
            session_connector: self.session_connector.clone(),
            connected_node_profiles: self.connected_node_profiles.clone(),
            node_profile_repo: self.node_profile_repo.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    async fn create_accept_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskAccepter {
            sessions: self.sessions.clone(),
            session_sender: self.session_sender.clone(),
            session_accepter: self.session_accepter.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    async fn create_communicator_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskCommunicator {
            my_node_profile: self.my_node_profile.clone(),
            sessions: self.sessions.clone(),
            session_receiver: self.session_receiver.clone(),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    async fn create_computer_task(&self) -> Vec<JoinHandle<()>> {
        let mut join_handles: Vec<JoinHandle<()>> = Vec::new();
        let task = TaskComputer {
            my_node_profile: self.my_node_profile.clone(),
            node_profile_repo: self.node_profile_repo.clone(),
            node_profile_fetcher: self.node_profile_fetcher.clone(),
            sessions: self.sessions.clone(),
            get_want_asset_keys_fn: Arc::new(self.get_want_asset_keys_fn.executor()),
            get_push_asset_keys_fn: Arc::new(self.get_push_asset_keys_fn.executor()),
            option: self.option.clone(),
        };

        for _ in 0..3 {
            let join_handle = task.clone().run(self.cancellation_token.clone()).await;
            join_handles.push(join_handle);
        }

        join_handles
    }

    pub async fn terminate(&self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();

        if let Some(join_handles) = self.join_handles.lock().await.take() {
            join_handles.await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::Path, sync::Arc};

    use chrono::Utc;
    use core_base::{
        clock::{SystemClock, SystemClockUtc},
        random_bytes::RandomBytesProviderImpl,
    };
    use testresult::TestResult;

    use crate::{
        model::{NodeProfile, OmniAddress, OmniSignType, OmniSigner},
        service::{
            connection::{
                ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl, TcpProxyOption, TcpProxyType,
            },
            engine::{node::NodeProfileRepo, NodeFinder, NodeProfileFetcherMock},
            session::{SessionAccepter, SessionConnector},
        },
    };

    use super::NodeFinderOptions;

    #[tokio::test]
    #[ignore]
    async fn simple_test() -> TestResult {
        tracing_subscriber::fmt().with_max_level(tracing::Level::TRACE).with_target(false).init();

        let dir = tempfile::tempdir()?;

        let np1 = NodeProfile {
            id: "1".as_bytes().to_vec(),
            addrs: vec![OmniAddress::new("tcp(127.0.0.1:60001)")],
        };
        let np2 = NodeProfile {
            id: "2".as_bytes().to_vec(),
            addrs: vec![OmniAddress::new("tcp(127.0.0.1:60002)")],
        };

        let nf1_path = dir.path().join("1");
        fs::create_dir_all(&nf1_path)?;

        let nf1 = create_node_finder(&nf1_path, "1", 60001, np2).await?;

        let nf2_path = dir.path().join("2");
        fs::create_dir_all(&nf2_path)?;

        let nf2 = create_node_finder(&nf2_path, "2", 60002, np1).await?;

        loop {
            if nf1.get_session_count().await == 1 && nf2.get_session_count().await == 1 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            println!("wait");
        }
        println!("done");

        nf1.terminate().await?;
        nf2.terminate().await?;

        Ok(())
    }

    async fn create_node_finder(dir_path: &Path, name: &str, port: u16, other_node_profile: NodeProfile) -> anyhow::Result<NodeFinder> {
        let tcp_accepter: Arc<dyn ConnectionTcpAccepter + Send + Sync> =
            Arc::new(ConnectionTcpAccepterImpl::new(format!("127.0.0.1:{}", port).as_str(), false).await?);
        let tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync> = Arc::new(
            ConnectionTcpConnectorImpl::new(TcpProxyOption {
                typ: TcpProxyType::None,
                addr: None,
            })
            .await?,
        );

        let signer = Arc::new(OmniSigner::new(&OmniSignType::Ed25519, name));
        let random_bytes_provider = Arc::new(RandomBytesProviderImpl);
        let session_accepter = Arc::new(SessionAccepter::new(tcp_accepter.clone(), signer.clone(), random_bytes_provider.clone()).await);
        let session_connector = Arc::new(SessionConnector::new(tcp_connector, signer, random_bytes_provider));

        let system_clock: Arc<dyn SystemClock<Utc> + Send + Sync> = Arc::new(SystemClockUtc);
        let node_ref_repo_dir = dir_path.join(name).join("repo");
        fs::create_dir_all(&node_ref_repo_dir)?;

        let node_ref_repo = Arc::new(NodeProfileRepo::new(node_ref_repo_dir.as_os_str().to_str().unwrap(), system_clock.clone()).await?);

        let node_ref_fetcher = Arc::new(NodeProfileFetcherMock {
            node_profiles: vec![other_node_profile],
        });

        let node_finder_dir = dir_path.join(name).join("finder");
        fs::create_dir_all(&node_finder_dir)?;

        let result = NodeFinder::new(
            session_connector,
            session_accepter,
            node_ref_repo,
            node_ref_fetcher,
            system_clock,
            NodeFinderOptions {
                state_dir_path: node_finder_dir.as_os_str().to_str().unwrap().to_string(),
                max_connected_session_count: 3,
                max_accepted_session_count: 3,
            },
        )
        .await;

        Ok(result)
    }
}
