use std::io::Write as _;

use async_trait::async_trait;
use omnius_core_omnikit::connection::framed::{FramedRecv, FramedSend};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::bytes::{Buf, BufMut, Bytes, BytesMut};

#[async_trait]
pub trait FramedRecvExt: FramedRecv {
    async fn recv_message<T: DeserializeOwned>(&mut self) -> anyhow::Result<T>;
}

#[async_trait]
impl<T: FramedRecv> FramedRecvExt for T
where
    T: ?Sized + Send + Unpin,
{
    async fn recv_message<TItem: DeserializeOwned>(&mut self) -> anyhow::Result<TItem> {
        let b = self.recv().await?;
        let item = Packet::deserialize(b)?;
        Ok(item)
    }
}

#[async_trait]
pub trait FramedSendExt: FramedSend {
    async fn send_message<T: Serialize + Send>(&mut self, item: T) -> anyhow::Result<()>;
}

#[async_trait]
impl<T: FramedSend> FramedSendExt for T
where
    T: ?Sized + Send + Unpin,
{
    async fn send_message<TItem: Serialize + Send>(&mut self, item: TItem) -> anyhow::Result<()> {
        let b = Packet::serialize(item)?;
        self.send(b).await?;
        Ok(())
    }
}

enum PacketType {
    #[allow(unused)]
    Unknown = 0,
    Cbor = 1,
}

impl From<u8> for PacketType {
    fn from(value: u8) -> Self {
        match value {
            1 => PacketType::Cbor,
            _ => PacketType::Unknown,
        }
    }
}

pub struct Packet;

impl Packet {
    pub fn serialize<T: Serialize>(item: T) -> anyhow::Result<Bytes> {
        let buffer = BytesMut::new();
        let mut writer = buffer.writer();

        writer.write_all(&[PacketType::Cbor as u8])?;

        ciborium::ser::into_writer(&item, &mut writer)?;
        let buffer = writer.into_inner().freeze();
        Ok(buffer)
    }

    pub fn deserialize<T: DeserializeOwned>(buf: Bytes) -> anyhow::Result<T> {
        let mut buf = buf;

        if buf.is_empty() {
            return Err(anyhow::anyhow!("Invalid packet"));
        }

        match PacketType::from(buf.get_u8()) {
            PacketType::Cbor => {
                let reader = buf.reader();
                let item = ciborium::de::from_reader(reader)?;
                Ok(item)
            }
            _ => Err(anyhow::anyhow!("Invalid packet type")),
        }
    }
}
