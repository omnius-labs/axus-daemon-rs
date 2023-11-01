use std::{collections::HashMap, net::IpAddr, time::Duration};

use futures_util::TryStreamExt;
use local_ip_address::list_afinet_netifas;
use once_cell::sync::Lazy;
use rupnp::ssdp::{SearchTarget, URN};

static URNS: Lazy<Vec<URN>> = Lazy::new(|| {
    vec![
        URN::service("schemas-upnp-org", "WANIPConnection", 1),
        URN::service("schemas-upnp-org", "WANPPPConnection", 1),
    ]
});

pub struct UpnpClient;

impl UpnpClient {
    pub async fn add_port_mapping(external_port: &str, internal_port: &str, description: &str) -> anyhow::Result<()> {
        for ip in Self::get_internal_ips().await? {
            for urn in URNS.iter() {
                let res = Self::add_port_mapping_sub(urn, external_port, internal_port, ip.to_string().as_str(), description).await;
                if res.is_err() {
                    continue;
                }
                return Ok(());
            }
        }

        anyhow::bail!("failed to open port");
    }

    pub async fn get_generic_port_mapping(index: i32) -> anyhow::Result<HashMap<String, String>> {
        for urn in URNS.iter() {
            let res = Self::get_generic_port_mapping_sub(urn, index).await;
            if res.is_err() {
                continue;
            }
            return Ok(res.unwrap());
        }

        anyhow::bail!("failed to open port");
    }

    async fn add_port_mapping_sub(urn: &URN, external_port: &str, internal_port: &str, internal_ip: &str, description: &str) -> anyhow::Result<()> {
        let name = "AddPortMapping";
        let args = format!(
            r#"\
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
<s:Body>
 <u:AddPortMapping xmlns:u="{urn}">
  <NewRemoteHost></NewRemoteHost>
  <NewExternalPort>"{external_port}"</NewExternalPort>
  <NewProtocol>TCP</NewProtocol>
  <NewInternalPort>{internal_port}</NewInternalPort>
  <NewInternalClient>{internal_ip}</NewInternalClient>
  <NewEnabled>1</NewEnabled>
  <NewPortMappingDescription>{description}</NewPortMappingDescription>
  <NewLeaseDuration>0</NewLeaseDuration>
 </u:AddPortMapping>
</s:Body>
/s:Envelope>
"#
        );

        Self::action(urn, name, args.as_str()).await?;

        Ok(())
    }

    async fn get_generic_port_mapping_sub(urn: &URN, index: i32) -> anyhow::Result<HashMap<String, String>> {
        let name = "GetGenericPortMappingEntry";
        let args = format!(
            r#"\
<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
<s:Body>
  <u:GetGenericPortMappingEntry xmlns:u="{urn}">
   <NewPortMappingIndex>"{index}"</NewPortMappingIndex>
  </u:GetGenericPortMappingEntry>
</s:Body>
/s:Envelope>
"#
        );

        Self::action(urn, name, args.as_str()).await
    }

    async fn get_internal_ips() -> anyhow::Result<Vec<IpAddr>> {
        let mut res: Vec<IpAddr> = Vec::new();
        let network_interfaces = list_afinet_netifas().unwrap();
        for (_, ip) in network_interfaces.iter() {
            if !ip.is_ipv4() || ip.is_loopback() {
                continue;
            }
            res.push(ip.to_owned());
        }
        Ok(res)
    }

    async fn action(urn: &URN, name: &str, args: &str) -> anyhow::Result<HashMap<String, String>> {
        let search_target = SearchTarget::URN(urn.clone());
        let devices = rupnp::discover(&search_target, Duration::from_secs(3)).await?;
        pin_utils::pin_mut!(devices);

        while let Some(device) = devices.try_next().await? {
            let service = device.find_service(&urn.clone());
            if service.is_none() {
                continue;
            }
            let service = service.unwrap();

            let action = format!(r#"SOAPAction"{urn}#{name}""#);
            let result = service.action(device.url(), &action, args).await;
            if result.is_err() {
                continue;
            }

            return Ok(result.unwrap());
        }

        anyhow::bail!("failed to add port mapping");
    }
}

#[cfg(test)]
mod tests {
    use crate::service::UpnpClient;

    #[tokio::test]
    #[ignore]
    async fn get_generic_port_mapping_test() {
        let _ = UpnpClient::get_generic_port_mapping(1).await;
    }
}
