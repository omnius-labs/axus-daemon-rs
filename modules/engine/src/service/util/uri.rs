use crate::model::NodeProfile;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as BASE64, Engine};
use tokio_util::bytes::Bytes;

use super::Cbor;

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

    fn try_remove_schema_and_type<'a>(typ: &str, text: &'a str) -> anyhow::Result<&'a str> {
        if text.starts_with(format!("axus:{}/", typ).as_str()) {
            return Ok(text.split_once('/').unwrap().1);
        }
        anyhow::bail!("Invalid schema")
    }
}

#[cfg(test)]
mod tests {

    #[test]
    pub fn node_profile_test() {}
}
