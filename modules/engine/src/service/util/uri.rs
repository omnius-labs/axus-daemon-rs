use crate::model::NodeProfile;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine};
use crc::{Algorithm, Crc, CRC_16_IBM_SDLC, CRC_32_ISCSI};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::bytes::Bytes;

use super::Cbor;

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub struct UriConverter;

impl UriConverter {
    pub fn encode_node_profile(v: &NodeProfile) -> anyhow::Result<String> {
        let b = Cbor::serialize(v)?;
        let b = BASE64.encode(&b);

        let mut s = String::new();
        s.push_str("axus:node");
        s.push('/');
        s.push_str(b.as_str());
        Ok(s)
    }

    pub fn decode_node_profile(text: &str) -> anyhow::Result<NodeProfile> {
        let b = UriConverter::try_remove_schema_and_type("node", text)?;
        let b = Bytes::from(BASE64.decode(b.as_bytes())?);
        let v = Cbor::deserialize(b)?;
        Ok(v)
    }

    fn encode<T: Serialize>(v: &T) -> anyhow::Result<String> {
        let m = Cbor::serialize(v)?;
        let crc = BASE64.encode(CASTAGNOLI.checksum(&m).to_le_bytes());
        let body = BASE64.encode(&m);

        let mut s = String::new();
        s.push_str("axus:node");
        s.push('/');
        s.push_str(crc.as_str());
        s.push('.');
        s.push_str(body.as_str());
        Ok(s)
    }

    fn decode<T: DeserializeOwned>(typ: &str, text: &str) -> anyhow::Result<T> {
        let (crc, body) = Self::try_parse(typ, text)?;
        let b = Bytes::from(BASE64.decode(b.as_bytes())?);
        let v = Cbor::deserialize(b)?;
        Ok(v)
    }

    fn try_parse<'a>(typ: &str, text: &'a str) -> anyhow::Result<(&'a str, &'a str)> {
        if text.starts_with(format!("axus:{}/", typ).as_str()) {
            let v = text.split_once('/').unwrap().1;
            let vs = v.split_once('.').ok_or(anyhow::anyhow!("'.' not found"))?;
            return Ok(vs);
        }
        anyhow::bail!("invalid schema")
    }
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn node_profile_test() {}
}
