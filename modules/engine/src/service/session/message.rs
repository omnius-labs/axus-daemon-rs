use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SessionVersion {
    V1,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HelloMessage {
    pub version: SessionVersion,
}

pub enum V1RequestType {
    NodeExchanger,
}

pub struct V1RequestMessage {
    pub request_type: V1RequestType,
}

pub enum V1ResultType {
    Accept,
    Reject,
}

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
