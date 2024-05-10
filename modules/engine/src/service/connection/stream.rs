use std::sync::Arc;

use anyhow::Context as _;
use async_trait::async_trait;
use futures_util::SinkExt;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::Mutex as TokioMutex,
};
use tokio_stream::StreamExt;
use tokio_util::bytes::Bytes;

use crate::service::util::Cbor;

#[derive(Clone)]
pub struct FramedStream {
    pub reader: Arc<TokioMutex<dyn AsyncRecv + Send + Sync + Unpin>>,
    pub writer: Arc<TokioMutex<dyn AsyncSend + Send + Sync + Unpin>>,
}

impl FramedStream {
    pub fn new<R, W>(reader: R, writer: W) -> Self
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let reader = Arc::new(TokioMutex::new(FramedReader::new(reader)));
        let writer = Arc::new(TokioMutex::new(FramedWriter::new(writer)));
        Self { reader, writer }
    }
}

#[async_trait]
pub trait AsyncSend {
    async fn send(&mut self, buffer: Bytes) -> anyhow::Result<()>;
}
#[async_trait]
pub trait AsyncRecv {
    async fn recv(&mut self) -> anyhow::Result<Bytes>;
}

#[async_trait]
pub trait AsyncSendExt: AsyncSend {
    async fn send_message<T: Serialize + Send>(&mut self, item: T) -> anyhow::Result<()>;
}

#[async_trait]
pub trait AsyncRecvExt: AsyncRecv {
    async fn recv_message<T: for<'a> Deserialize<'a>>(&mut self) -> anyhow::Result<T>;
}

struct FramedReader<T>
where
    T: AsyncRead + Send + Sync + Unpin,
{
    framed: tokio_util::codec::FramedRead<T, tokio_util::codec::LengthDelimitedCodec>,
}

impl<T> FramedReader<T>
where
    T: AsyncRead + Send + Sync + Unpin,
{
    pub fn new(stream: T) -> Self {
        let codec = tokio_util::codec::LengthDelimitedCodec::builder()
            .max_frame_length(1024 * 1024 * 64)
            .little_endian()
            .new_codec();
        let framed = tokio_util::codec::FramedRead::new(stream, codec);
        Self { framed }
    }
}

struct FramedWriter<T>
where
    T: AsyncWrite + Send + Sync + Unpin,
{
    framed: tokio_util::codec::FramedWrite<T, tokio_util::codec::LengthDelimitedCodec>,
}

impl<T> FramedWriter<T>
where
    T: AsyncWrite + Send + Sync + Unpin,
{
    pub fn new(stream: T) -> Self {
        let codec = tokio_util::codec::LengthDelimitedCodec::builder()
            .max_frame_length(1024 * 1024 * 64)
            .little_endian()
            .new_codec();
        let framed = tokio_util::codec::FramedWrite::new(stream, codec);
        Self { framed }
    }
}

#[async_trait]
impl<T> AsyncSend for FramedWriter<T>
where
    T: AsyncWrite + Send + Sync + Unpin,
{
    async fn send(&mut self, buffer: Bytes) -> anyhow::Result<()> {
        self.framed.send(buffer).await.with_context(|| "Failed to send")?;
        Ok(())
    }
}

#[async_trait]
impl<T: AsyncSend> AsyncSendExt for T
where
    T: ?Sized + Send + Sync + Unpin,
{
    async fn send_message<TItem: Serialize + Send>(&mut self, item: TItem) -> anyhow::Result<()> {
        let b = Cbor::serialize(item)?;
        self.send(b).await?;
        Ok(())
    }
}

#[async_trait]
impl<T> AsyncRecv for FramedReader<T>
where
    T: AsyncRead + Send + Sync + Unpin,
{
    async fn recv(&mut self) -> anyhow::Result<Bytes> {
        let buffer = self.framed.next().await.ok_or(anyhow::anyhow!("Stream ended"))??.freeze();
        Ok(buffer)
    }
}

#[async_trait]
impl<T: AsyncRecv> AsyncRecvExt for T
where
    T: ?Sized + Send + Sync + Unpin,
{
    async fn recv_message<TItem: DeserializeOwned>(&mut self) -> anyhow::Result<TItem> {
        let b = self.recv().await?;
        let item = Cbor::deserialize(b)?;
        Ok(item)
    }
}
