use std::{
    net::{SocketAddr, SocketAddrV4, SocketAddrV6},
    str::FromStr,
};

use tokio::net::{TcpListener, TcpStream};

pub struct ConnectionTcpAccepter {
    listener: TcpListener,
}

impl ConnectionTcpAccepter {
    pub async fn new(addr: &str) -> anyhow::Result<ConnectionTcpAccepter> {
        if let Ok(addr) = SocketAddrV4::from_str(addr) {
            let listener = TcpListener::bind(addr).await?;

            return Ok(ConnectionTcpAccepter { listener });
        } else if let Ok(addr) = SocketAddrV6::from_str(addr) {
            let listener = TcpListener::bind(addr).await?;
            return Ok(ConnectionTcpAccepter { listener });
        }
        anyhow::bail!("invalid address");
    }

    pub async fn accept(&self) -> anyhow::Result<(TcpStream, SocketAddr)> {
        Ok(self.listener.accept().await?)
    }
}
