use bitflags::bitflags;
use serde::{Deserialize, Serialize};

use crate::model::OmniSignature;

bitflags! {
    #[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
    pub struct SessionVersion: u32 {
        const V1 = 1;
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloMessage {
    pub version: SessionVersion,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct V1ChallengeMessage {
    pub nonce: [u8; 32],
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct V1SignatureMessage {
    pub signature: OmniSignature,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum V1RequestType {
    NodeExchanger,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct V1RequestMessage {
    pub request_type: V1RequestType,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum V1ResultType {
    Accept,
    Reject,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct V1ResultMessage {
    pub result_type: V1ResultType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[ignore]
    #[test]
    fn serialize_test() {
        let v = HelloMessage { version: SessionVersion::V1 };

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&v, &mut bytes).unwrap();

        let output = ciborium::de::from_reader(&bytes[..]).unwrap();
        assert_eq!(v, output);

        println!("{}", hex::encode(&bytes))
    }
}
