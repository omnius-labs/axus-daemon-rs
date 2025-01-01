use std::fmt;

use omnius_core_omnikit::model::OmniAddr;
use omnius_core_rocketpack::{RocketMessage, RocketMessageReader, RocketMessageWriter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeProfile {
    pub id: Vec<u8>,
    pub addrs: Vec<OmniAddr>,
}

impl fmt::Display for NodeProfile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let addrs: Vec<String> = self.addrs.iter().map(|n| n.to_string()).collect();
        write!(f, "id: {}, addrs: [{}]", hex::encode(&self.id), addrs.join(", "))
    }
}

impl RocketMessage for NodeProfile {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, _depth: u32) -> anyhow::Result<()> {
        writer.put_bytes(&value.id);

        writer.put_u32(value.addrs.len().try_into()?);
        for v in &value.addrs {
            writer.put_str(v.as_str());
        }

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, _depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let id = reader.get_bytes(128)?;

        let len = reader.get_u32()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut addrs = Vec::with_capacity(len.try_into()?);
        for _ in 0..len {
            addrs.push(OmniAddr::new(reader.get_string(1024)?.as_str()));
        }

        Ok(Self { id, addrs })
    }
}
