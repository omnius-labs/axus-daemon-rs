use std::sync::Arc;

use fast_socks5::client::Socks5Stream;
use tokio::net::TcpStream;

use crate::service::AsyncStream;

pub struct ConnectionTcpConnectorOption {
    pub proxy: TcpProxyOption,
}

pub struct TcpProxyOption {
    pub typ: TcpProxyType,
    pub addr: String,
}

pub enum TcpProxyType {
    None,
    Socks5,
}

pub struct ConnectionTcpConnector {
    option: ConnectionTcpConnectorOption,
}

impl ConnectionTcpConnector {
    pub async fn new(option: ConnectionTcpConnectorOption) -> anyhow::Result<Self> {
        Ok(Self { option })
    }

    pub async fn connect(&self, addr: &str) -> anyhow::Result<Arc<dyn AsyncStream>> {
        match self.option.proxy.typ {
            TcpProxyType::None => {
                let stream = TcpStream::connect(addr).await?;
                Ok(Arc::new(stream))
            }
            TcpProxyType::Socks5 => {
                if let Some((host, port)) = Self::parse_host_and_port(addr) {
                    let config = fast_socks5::client::Config::default();
                    let stream = Socks5Stream::connect(self.option.proxy.addr.as_str(), host, port, config).await?;
                    return Ok(Arc::new(stream));
                }
                anyhow::bail!("failed to connect by socks5: {:?}", addr);
            }
        }
    }

    fn parse_host_and_port(input: &str) -> Option<(String, u16)> {
        if let Some(idx) = input.rfind(':') {
            let (host_str, port_str) = input.split_at(idx);
            let host = host_str.to_string();
            let port_str = &port_str[1..]; // Skip the ':'
            let port = port_str.parse().ok()?;
            Some((host, port))
        } else {
            None
        }
    }
}
