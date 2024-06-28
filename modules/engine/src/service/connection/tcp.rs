mod accepter;
mod connector;
mod upnp_client;

pub use accepter::*;
pub use connector::*;
pub use upnp_client::*;

#[cfg(test)]
mod tests {
    use testresult::TestResult;

    use crate::{
        connection::{FramedRecvExt as _, FramedSendExt as _},
        service::connection::{
            ConnectionTcpAccepter, ConnectionTcpAccepterImpl, ConnectionTcpConnector, ConnectionTcpConnectorImpl, TcpProxyOption, TcpProxyType,
        },
    };

    #[tokio::test]
    #[ignore]
    async fn simple_test() -> TestResult {
        let accepter = ConnectionTcpAccepterImpl::new("127.0.0.1:50000", false).await?;
        let connector = ConnectionTcpConnectorImpl::new(TcpProxyOption {
            typ: TcpProxyType::None,
            addr: None,
        })
        .await?;

        let connected_stream = connector.connect("127.0.0.1:50000").await?;
        let (accepted_stream, _) = accepter.accept().await?;

        connected_stream.sender.lock().await.send_message(b"Hello, World!").await?;
        let line: Vec<u8> = accepted_stream.receiver.lock().await.recv_message().await?;

        println!("{}", std::str::from_utf8(&line)?);

        Ok(())
    }
}
