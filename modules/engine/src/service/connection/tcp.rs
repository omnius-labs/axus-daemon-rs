mod accepter;
mod connector;
mod upnp_client;

pub use accepter::*;
pub use connector::*;
pub use upnp_client::*;

#[cfg(test)]
mod tests {
    use core_omnius::{AsyncRecvExt as _, AsyncSendExt as _};

    use crate::service::connection::{
        ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl, TcpProxyOption, TcpProxyType,
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

        let connected_stream = connector.connect("127.0.0.1:50000").await.unwrap();
        let (accepted_stream, _) = accepter.accept().await.unwrap();

        connected_stream.writer.lock().await.send_message(b"Hello, World!").await.unwrap();
        let line: Vec<u8> = accepted_stream.reader.lock().await.recv_message().await.unwrap();

        println!("{}", std::str::from_utf8(&line).unwrap());
    }
}
