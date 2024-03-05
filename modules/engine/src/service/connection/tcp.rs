mod accepter;
mod connector;
mod upnp_client;

pub use accepter::*;
pub use connector::*;
pub use upnp_client::*;

#[cfg(test)]
mod tests {
    use crate::service::connection::{AsyncSendRecv, AsyncSendRecvExt, ConnectionTcpAccepter, ConnectionTcpConnector, TcpProxyOption, TcpProxyType};

    #[tokio::test]
    #[ignore]
    async fn simple_test() {
        let accepter = ConnectionTcpAccepter::new("127.0.0.1:50000", false).await.unwrap();
        let connector = ConnectionTcpConnector::new(TcpProxyOption {
            typ: TcpProxyType::None,
            addr: None,
        })
        .await
        .unwrap();

        let mut client: Box<dyn AsyncSendRecv + Send + Sync + Unpin> = Box::new(connector.connect("127.0.0.1:50000").await.unwrap());
        let mut server: Box<dyn AsyncSendRecv + Send + Sync + Unpin> = Box::new(accepter.accept().await.unwrap().0);

        client.send_message(b"Hello, World!").await.unwrap();
        let line: Vec<u8> = server.recv_message().await.unwrap();

        println!("{}", std::str::from_utf8(&line).unwrap());
    }
}
