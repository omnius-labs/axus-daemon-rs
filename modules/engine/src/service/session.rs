mod accepter;
mod connector;
pub mod message;
pub mod model;

pub use accepter::*;
pub use connector::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use core_base::random_bytes::RandomBytesProviderImpl;
    use tests::model::SessionType;

    use crate::{
        model::{OmniAddress, OmniSignType, OmniSigner},
        service::connection::{AsyncSendRecvExt, ConnectionTcpAccepter, ConnectionTcpConnector, TcpProxyOption, TcpProxyType},
    };

    use super::*;

    #[tokio::test]
    #[ignore]
    async fn simple_test() {
        let tcp_accepter = Arc::new(ConnectionTcpAccepter::new("127.0.0.1:60000", false).await.unwrap());
        let tcp_connector = Arc::new(
            ConnectionTcpConnector::new(TcpProxyOption {
                typ: TcpProxyType::None,
                addr: None,
            })
            .await
            .unwrap(),
        );

        let signer = Arc::new(OmniSigner::new(&OmniSignType::Ed25519, "test"));
        let random_bytes_provider = Arc::new(RandomBytesProviderImpl);
        let session_accepter = SessionAccepter::new(tcp_accepter.clone(), signer.clone(), random_bytes_provider.clone()).await;
        let session_connector = SessionConnector::new(tcp_connector, signer, random_bytes_provider);

        let client = Arc::new(
            session_connector
                .connect(&OmniAddress::new("tcp(127.0.0.1:60000)"), &SessionType::NodeExchanger)
                .await
                .unwrap(),
        );
        let server = Arc::new(session_accepter.accept(&SessionType::NodeExchanger).await.unwrap());

        client.stream.lock().await.send_message(b"Hello, World!").await.unwrap();
        let line: Vec<u8> = server.stream.lock().await.recv_message().await.unwrap();

        println!("{}", std::str::from_utf8(&line).unwrap());

        session_accepter.terminate().await.unwrap();
        tcp_accepter.terminate().await.unwrap();
    }
}
