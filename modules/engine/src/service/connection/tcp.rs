mod accepter;
mod connector;
mod upnp_client;

pub use accepter::*;
pub use connector::*;
pub use upnp_client::*;

#[cfg(test)]
mod tests {
    use futures_util::SinkExt;
    use tokio_stream::StreamExt;
    use tokio_util::{
        bytes::Bytes,
        codec::{Framed, LengthDelimitedCodec},
    };

    use super::*;

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

        let client = connector.connect("127.0.0.1:50000").await.unwrap();
        let mut client = Framed::new(client, LengthDelimitedCodec::new());
        let (server, _) = accepter.accept().await.unwrap();
        let mut server = Framed::new(server, LengthDelimitedCodec::new());

        client.send(Bytes::from("Hello, World!")).await.unwrap();

        if let Some(Ok(line)) = server.next().await {
            println!("{}", std::str::from_utf8(&line).unwrap());
        }
    }
}
