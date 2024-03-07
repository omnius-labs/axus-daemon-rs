use crate::model::NodeProfile;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine};
use crc::{Crc, CRC_32_ISCSI};
use serde::{de::DeserializeOwned, Serialize};
use tokio_util::bytes::Bytes;

use super::Cbor;

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub struct UriConverter;

impl UriConverter {
    pub fn encode_node_profile(v: &NodeProfile) -> anyhow::Result<String> {
        Self::encode("node", v)
    }

    pub fn decode_node_profile(text: &str) -> anyhow::Result<NodeProfile> {
        Self::decode("node", text)
    }

    fn encode<T: Serialize>(typ: &str, v: &T) -> anyhow::Result<String> {
        let b = Cbor::serialize(v)?;
        let c = CASTAGNOLI.checksum(&b).to_le_bytes();
        let crc = BASE64.encode(c);
        let body = BASE64.encode(&b);

        let mut s = String::new();
        s.push_str(format!("axus:{}", typ).as_str());
        s.push('/');
        s.push_str(crc.as_str());
        s.push('.');
        s.push_str(body.as_str());
        Ok(s)
    }

    fn decode<T: DeserializeOwned>(typ: &str, text: &str) -> anyhow::Result<T> {
        let (crc, body) = Self::try_parse(typ, text)?;
        let c = <[u8; 4]>::try_from(BASE64.decode(crc)?).map_err(|_| anyhow::anyhow!("invalid crc"))?;
        let b = Bytes::from(BASE64.decode(body.as_bytes())?);

        if c != CASTAGNOLI.checksum(b.as_ref()).to_le_bytes() {
            anyhow::bail!("invalid checksum")
        }
        let v = Cbor::deserialize(b)?;
        Ok(v)
    }

    fn try_parse<'a>(typ: &str, text: &'a str) -> anyhow::Result<(&'a str, &'a str)> {
        if text.starts_with(format!("axus:{}/", typ).as_str()) {
            let v = text.split_once('/').unwrap().1;
            let vs = v.split_once('.').ok_or(anyhow::anyhow!("separator not found"))?;
            return Ok(vs);
        }
        anyhow::bail!("invalid schema")
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        model::{NodeProfile, OmniAddress},
        service::util::UriConverter,
    };

    #[test]
    pub fn node_profile_test() {
        let v = NodeProfile {
            addrs: ["a", "b", "c"].iter().map(|s| OmniAddress::new(s)).collect(),
        };
        let s = UriConverter::encode_node_profile(&v).unwrap();
        println!("{}", s);
        let v2 = UriConverter::decode_node_profile(s.as_str()).unwrap();
        assert_eq!(v, v2);
    }
}
