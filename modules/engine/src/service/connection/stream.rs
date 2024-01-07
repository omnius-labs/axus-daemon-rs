use async_trait::async_trait;
use futures_util::SinkExt;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::{Framed, LengthDelimitedCodec},
};

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
        let buffer = BytesMut::new();
        let mut writer = buffer.writer();
        ciborium::ser::into_writer(&item, &mut writer)?;
        let buffer = writer.into_inner().freeze();
        self.send(buffer).await?;
        Ok(())
    }

    async fn recv_message<TItem: for<'a> Deserialize<'a>>(&mut self) -> anyhow::Result<TItem> {
        let buffer = self.recv().await?;
        let mut reader = buffer.reader();
        let item: TItem = ciborium::de::from_reader(&mut reader)?;
        Ok(item)
    }
}
