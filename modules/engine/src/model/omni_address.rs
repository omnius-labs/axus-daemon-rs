use std::net::SocketAddr;
use std::str::FromStr;

use nom::bytes::complete::{is_not, tag};
use nom::character::complete::{char, multispace0};
use nom::sequence::delimited;
use nom::IResult;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct OmniAddress {
    value: String,
}

impl OmniAddress {
    pub fn new(value: &str) -> OmniAddress {
        OmniAddress { value: value.to_owned() }
    }

    pub fn parse_tcp(self) -> anyhow::Result<SocketAddr> {
        let (_, addr) = Self::parse_tcp_sub(&self.value).map_err(|e| e.to_owned())?;
        Ok(SocketAddr::from_str(addr)?)
    }

    fn parse_tcp_sub(v: &str) -> IResult<&str, &str> {
        let (v, _) = tag("tcp")(v)?;
        let (v, addr) = delimited(char('('), delimited(multispace0, is_not(")"), multispace0), char(')'))(v)?;
        Ok((v, addr))
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