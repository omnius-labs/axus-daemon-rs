use serde::{de::DeserializeOwned, Serialize};
use tokio_util::bytes::{Buf, BufMut, Bytes, BytesMut};

pub struct Cbor;

impl Cbor {
    pub fn serialize<T: Serialize>(item: T) -> anyhow::Result<Bytes> {
        let buffer = BytesMut::new();
        let mut writer = buffer.writer();
        ciborium::ser::into_writer(&item, &mut writer)?;
        let buffer = writer.into_inner().freeze();
        Ok(buffer)
    }

    pub fn deserialize<T: DeserializeOwned>(b: Bytes) -> anyhow::Result<T> {
        let mut reader = b.reader();
        let item = ciborium::de::from_reader(&mut reader)?;
        Ok(item)
    }
}
