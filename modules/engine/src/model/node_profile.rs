use std::fmt;

use omnius_core_base::ensure_err;
use omnius_core_omnikit::model::OmniAddr;

use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeProfile {
    pub id: Vec<u8>,
    pub addrs: Vec<OmniAddr>,
}

impl std::fmt::Display for NodeProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let addrs: Vec<String> = self.addrs.iter().map(|n| n.to_string()).collect();
        write!(f, "id: {}, addrs: [{}]", hex::encode(&self.id), addrs.join(", "))
    }
}

impl RocketMessage for NodeProfile {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, _depth: u32) -> RocketPackResult<()> {
        writer.put_bytes(&value.id);

        writer.put_u32(value.addrs.len() as u32);
        for v in &value.addrs {
            writer.put_str(v.as_str());
        }

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, _depth: u32) -> RocketPackResult<Self>
    where
        Self: Sized,
    {
        let get_too_large_err = || RocketPackError::new(RocketPackErrorKind::TooLarge).message("len too large");

        let id = reader.get_bytes(128)?;

        let len = reader.get_u32()?;
        ensure_err!(len > 128, get_too_large_err);

        let mut addrs = Vec::with_capacity(len as usize);
        for _ in 0..len {
            addrs.push(OmniAddr::new(reader.get_string(1024)?.as_str()));
        }

        Ok(Self { id, addrs })
    }
}
