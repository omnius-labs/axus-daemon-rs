mod accepter;
mod connector;
pub mod message;
pub mod model;

pub use accepter::*;
pub use connector::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use core_base::{random_bytes::RandomBytesProviderImpl, sleeper::FakeSleeper};
    use core_omnius::{
        connection::framed::{FramedRecvExt as _, FramedSendExt as _},
        OmniAddr, OmniSignType, OmniSigner,
    };
    use testresult::TestResult;

    use crate::service::{
        connection::{
            ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl, TcpProxyOption, TcpProxyType,
        },
        session::{model::SessionType, SessionAccepter, SessionConnector},
    };

    #[tokio::test]
    #[ignore]
    async fn simple_test() -> TestResult {
        let tcp_accepter: Arc<dyn ConnectionTcpAccepter + Send + Sync> = Arc::new(ConnectionTcpAccepterImpl::new("127.0.0.1:60000", false).await?);
        let tcp_connector: Arc<dyn ConnectionTcpConnector + Send + Sync> = Arc::new(
            ConnectionTcpConnectorImpl::new(TcpProxyOption {
                typ: TcpProxyType::None,
                addr: None,
            })
            .await?,
        );

        let signer = Arc::new(OmniSigner::new(&OmniSignType::Ed25519, "test")?);
        let random_bytes_provider = Arc::new(RandomBytesProviderImpl);
        let sleeper = Arc::new(FakeSleeper);

        let session_accepter = SessionAccepter::new(tcp_accepter.clone(), signer.clone(), random_bytes_provider.clone(), sleeper.clone()).await;
        let session_connector = SessionConnector::new(tcp_connector, signer, random_bytes_provider);

        let client = Arc::new(
            session_connector
                .connect(&OmniAddr::new("tcp(127.0.0.1:60000)"), &SessionType::NodeFinder)
                .await?,
        );
        let server = Arc::new(session_accepter.accept(&SessionType::NodeFinder).await?);

        client.stream.sender.lock().await.send_message(b"Hello, World!").await?;
        let line: Vec<u8> = server.stream.receiver.lock().await.recv_message().await?;

        println!("{}", std::str::from_utf8(&line)?);

        session_accepter.terminate().await?;
        tcp_accepter.terminate().await?;

        Ok(())
    }
}
