mod accepter;
mod connector;
mod upnp_client;

pub use accepter::*;
pub use connector::*;
pub use upnp_client::*;

#[cfg(test)]
mod tests {
    use crate::service::connection::{
        AsyncRecvExt as _, AsyncSendExt as _, ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl,
        TcpProxyOption, TcpProxyType,
    };

    #[tokio::test]
    #[ignore]
    async fn simple_test() {
        let accepter = ConnectionTcpAccepterImpl::new("127.0.0.1:50000", false).await.unwrap();
        let connector = ConnectionTcpConnectorImpl::new(TcpProxyOption {
            typ: TcpProxyType::None,
            addr: None,
        })
        .await
        .unwrap();

        let (_, mut writer) = connector.connect("127.0.0.1:50000").await.unwrap();
        let (mut reader, _, _) = accepter.accept().await.unwrap();

        writer.send_message(b"Hello, World!").await.unwrap();
        let line: Vec<u8> = reader.recv_message().await.unwrap();

        println!("{}", std::str::from_utf8(&line).unwrap());
    }
}
