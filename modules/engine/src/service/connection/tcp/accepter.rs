use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
};

use async_trait::async_trait;
use tokio::net::TcpListener;

use omnius_core_base::{net::Reachable, terminable::Terminable};
use omnius_core_omnikit::model::OmniAddr;

use crate::service::connection::FramedStream;

use super::UpnpClient;

#[async_trait]
pub trait ConnectionTcpAccepter {
    async fn accept(&self) -> anyhow::Result<(FramedStream, SocketAddr)>;
    async fn get_global_ip_addresses(&self) -> anyhow::Result<Vec<IpAddr>>;
}

pub struct ConnectionTcpAccepterImpl {
    listener: TcpListener,
    upnp_port_mapping: Option<UpnpPortMapping>,
}

impl ConnectionTcpAccepterImpl {
    pub async fn new(addr: &OmniAddr, use_upnp: bool) -> anyhow::Result<Self> {
        let socket_addr = addr.parse_tcp_ip()?;
        if socket_addr.is_ipv4() {
            let listener = TcpListener::bind(socket_addr).await?;

            if use_upnp && socket_addr.ip().is_unspecified() {
                let upnp_port_mapping = UpnpPortMapping::new(socket_addr.port()).await;
                if let Ok(upnp_port_mapping) = upnp_port_mapping {
                    return Ok(Self {
                        listener,
                        upnp_port_mapping: Some(upnp_port_mapping),
                    });
                }
            }

            return Ok(Self {
                listener,
                upnp_port_mapping: None,
            });
        } else if socket_addr.is_ipv6() {
            let listener = TcpListener::bind(socket_addr).await?;
            return Ok(Self {
                listener,
                upnp_port_mapping: None,
            });
        }
        anyhow::bail!("invalid address");
    }
}

#[async_trait]
impl Terminable for ConnectionTcpAccepterImpl {
    type Error = anyhow::Error;
    async fn terminate(&self) -> anyhow::Result<()> {
        if let Some(upnp_port_mapping) = &self.upnp_port_mapping {
            upnp_port_mapping.terminate().await?;
        }
        Ok(())
    }
}

#[async_trait]
impl ConnectionTcpAccepter for ConnectionTcpAccepterImpl {
    async fn accept(&self) -> anyhow::Result<(FramedStream, SocketAddr)> {
        let (stream, addr) = self.listener.accept().await?;
        let (reader, writer) = tokio::io::split(stream);
        let stream = FramedStream::new(reader, writer);
        Ok((stream, addr))
    }

    async fn get_global_ip_addresses(&self) -> anyhow::Result<Vec<IpAddr>> {
        let mut res: Vec<IpAddr> = Vec::new();
        if let Ok(IpAddr::V4(ip4)) = local_ip_address::local_ip() {
            if ip4.is_reachable() {
                res.push(IpAddr::V4(ip4));
            }
        }
        if let Ok(IpAddr::V6(ip6)) = local_ip_address::local_ipv6() {
            if ip6.is_reachable() {
                res.push(IpAddr::V6(ip6));
            }
        }
        if let Some(upnp) = &self.upnp_port_mapping {
            if upnp.external_ip.is_reachable() {
                res.push(IpAddr::V4(upnp.external_ip));
            }
        }

        Ok(res)
    }
}

struct UpnpPortMapping {
    port: u16,
    external_ip: Ipv4Addr,
}

impl UpnpPortMapping {
    pub async fn new(port: u16) -> anyhow::Result<Self> {
        UpnpClient::delete_port_mapping("TCP", port).await?;
        UpnpClient::add_port_mapping("TCP", port, port, "axus").await?;
        let res = UpnpClient::get_external_ip_address().await?;
        let external_ip = res.get("NewExternalIPAddress").ok_or(anyhow::anyhow!("not found external ip"))?;
        let external_ip = Ipv4Addr::from_str(external_ip.as_str())?;
        Ok(Self { port, external_ip })
    }
}

#[async_trait]
impl Terminable for UpnpPortMapping {
    type Error = anyhow::Error;
    async fn terminate(&self) -> anyhow::Result<()> {
        UpnpClient::delete_port_mapping("TCP", self.port).await?;
        Ok(())
    }
}
