use std::fmt;

use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{char, multispace0};
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OmniAddress(String);

impl OmniAddress {
    pub fn new(value: &str) -> OmniAddress {
        OmniAddress(value.to_owned())
    }

    pub fn parse_tcp(&self) -> anyhow::Result<String> {
        let (_, addr) = Self::parse_tcp_sub(&self.0).map_err(|e| e.to_owned())?;
        Ok(addr.to_string())
    }

    fn parse_tcp_sub(v: &str) -> IResult<&str, &str> {
        let (v, _) = tag("tcp")(v)?;
        let (v, addr) = delimited(char('('), delimited(multispace0, is_not(")"), multispace0), char(')'))(v)?;
        Ok((v, addr))
    }
}

impl fmt::Display for OmniAddress {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for OmniAddress {
    fn from(value: String) -> Self {
        Self::new(value.as_str())
    }
}

#[cfg(test)]
mod tests {
    use crate::model::OmniAddress;

    #[tokio::test]
    #[ignore]
    async fn add_port_mapping_test() {
        let addr = OmniAddress::new("tcp(127.0.0.1:8000)");
        println!("{:?}", addr.parse_tcp());
    }
}
