use crate::model::NodeProfile;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine};
use crc::{Crc, CRC_32_ISCSI};
use tokio_util::bytes::Bytes;

use omnius_core_rocketpack::RocketMessage;

const CASTAGNOLI: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

pub struct UriConverter;

impl UriConverter {
    pub fn encode_node_profile(v: &NodeProfile) -> anyhow::Result<String> {
        Self::encode("node", v)
    }

    pub fn decode_node_profile(text: &str) -> anyhow::Result<NodeProfile> {
        Self::decode("node", text)
    }

    fn encode<T: RocketMessage>(typ: &str, v: &T) -> anyhow::Result<String> {
        let body = v.export()?;
        let crc = CASTAGNOLI.checksum(&body).to_le_bytes();

        let body = BASE64.encode(&body);
        let crc = BASE64.encode(crc);

        let mut s = String::new();
        s.push_str(format!("axus:{}", typ).as_str());
        s.push('/');
        s.push_str(crc.as_str());
        s.push('.');
        s.push_str(body.as_str());
        s.push_str(".1");
        Ok(s)
    }

    fn decode<T: RocketMessage>(typ: &str, text: &str) -> anyhow::Result<T> {
        let text = Self::try_parse_schema(typ, text)?;
        let (text, version) = Self::try_parse_version(text)?;

        match version {
            1 => Self::decode_v1(text),
            _ => anyhow::bail!("unsupported version"),
        }
    }

    fn decode_v1<T: RocketMessage>(text: &str) -> anyhow::Result<T> {
        let (crc, body) = Self::try_parse_body(text)?;

        let crc = <[u8; 4]>::try_from(BASE64.decode(crc)?).map_err(|_| anyhow::anyhow!("invalid crc"))?;
        let mut body = Bytes::from(BASE64.decode(body.as_bytes())?);

        if crc != CASTAGNOLI.checksum(body.as_ref()).to_le_bytes() {
            anyhow::bail!("invalid checksum")
        }

        let v = T::import(&mut body)?;
        Ok(v)
    }

    fn try_parse_schema<'a>(typ: &str, text: &'a str) -> anyhow::Result<&'a str> {
        if text.starts_with(format!("axus:{}/", typ).as_str()) {
            let text = text.split_once('/').unwrap().1;
            return Ok(text);
        }
        anyhow::bail!("invalid schema")
    }

    fn try_parse_version(text: &str) -> anyhow::Result<(&str, u32)> {
        let (text, version) = text.rsplit_once('.').ok_or(anyhow::anyhow!("separator not found"))?;
        let version: u32 = version.parse()?;
        Ok((text, version))
    }

    fn try_parse_body(text: &str) -> anyhow::Result<(&str, &str)> {
        let (crc, body) = text.split_once('.').ok_or(anyhow::anyhow!("separator not found"))?;
        Ok((crc, body))
    }
}

#[cfg(test)]
mod tests {
    use omnius_core_omnikit::model::OmniAddr;

    use crate::{model::NodeProfile, service::util::UriConverter};

    #[test]
    pub fn node_profile_test() {
        let v = NodeProfile {
            id: vec![1, 2, 3],
            addrs: ["a", "b", "c"].into_iter().map(OmniAddr::new).collect(),
        };
        let s = UriConverter::encode_node_profile(&v).unwrap();
        println!("{}", s);
        let v2 = UriConverter::decode_node_profile(s.as_str()).unwrap();
        assert_eq!(v, v2);
    }
}
