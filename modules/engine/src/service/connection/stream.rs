use async_trait::async_trait;
use futures_util::SinkExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::Bytes,
    codec::{Framed, LengthDelimitedCodec},
};

use crate::service::util::Cbor;

#[async_trait]
pub trait AsyncSendRecv {
    async fn send(&mut self, buffer: Bytes) -> anyhow::Result<()>;
    async fn recv(&mut self) -> anyhow::Result<Bytes>;
}

#[async_trait]
pub trait AsyncSendRecvExt: AsyncSendRecv {
    async fn send_message<T: Serialize + Send>(&mut self, item: T) -> anyhow::Result<()>;
    async fn recv_message<T: for<'a> Deserialize<'a>>(&mut self) -> anyhow::Result<T>;
}

pub struct Stream<T>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    framed: Framed<T, LengthDelimitedCodec>,
}

impl<T> Stream<T>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    pub fn new(stream: T) -> Self {
        let framed = Framed::new(stream, LengthDelimitedCodec::new());
        Self { framed }
    }
}

#[async_trait]
impl<T> AsyncSendRecv for Stream<T>
where
    T: AsyncRead + AsyncWrite + Send + Sync + Unpin,
{
    async fn send(&mut self, buffer: Bytes) -> anyhow::Result<()> {
        self.framed.send(buffer).await?;
        Ok(())
    }

    async fn recv(&mut self) -> anyhow::Result<Bytes> {
        let buffer = self.framed.next().await.ok_or(anyhow::anyhow!("Stream ended"))??.freeze();
        Ok(buffer)
    }
}

#[async_trait]
impl<T: AsyncSendRecv> AsyncSendRecvExt for T
where
    T: ?Sized + Send + Sync + Unpin,
{
    async fn send_message<TItem: Serialize + Send>(&mut self, item: TItem) -> anyhow::Result<()> {
        let b = Cbor::serialize(item)?;
        self.send(b).await?;
        Ok(())
    }

    async fn recv_message<TItem: DeserializeOwned>(&mut self) -> anyhow::Result<TItem> {
        let b = self.recv().await?;
        let item = Cbor::deserialize(b)?;
        Ok(item)
    }
}
