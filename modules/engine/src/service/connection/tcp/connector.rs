use fast_socks5::client::Socks5Stream;
use tokio::net::TcpStream;

pub struct TcpProxyOption {
    pub typ: TcpProxyType,
    pub addr: Option<String>,
}

pub enum TcpProxyType {
    None,
    Socks5,
}

pub struct ConnectionTcpConnector {
    proxy_option: TcpProxyOption,
}

impl ConnectionTcpConnector {
    pub async fn new(proxy_option: TcpProxyOption) -> anyhow::Result<Self> {
        Ok(Self { proxy_option })
    }

    pub async fn connect(&self, addr: &str) -> anyhow::Result<TcpStream> {
        match self.proxy_option.typ {
            TcpProxyType::None => {
                let stream = TcpStream::connect(addr).await?;
                Ok(stream)
            }
            TcpProxyType::Socks5 => {
                if let Some((host, port)) = Self::parse_host_and_port(addr) {
                    if let Some(proxy_addr) = &self.proxy_option.addr {
                        let config = fast_socks5::client::Config::default();
                        let stream = Socks5Stream::connect(proxy_addr.as_str(), host, port, config).await?;
                        let stream = stream.get_socket();
                        return Ok(stream);
                    }
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
