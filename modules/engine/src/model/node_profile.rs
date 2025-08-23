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
        writer.put_u32(1);
        writer.put_bytes(&value.id);

        writer.put_u32(2);
        writer.put_u32(value.addrs.len() as u32);
        for v in &value.addrs {
            writer.put_str(v.as_str());
        }

        writer.put_u32(0);

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, _depth: u32) -> RocketPackResult<Self>
    where
        Self: Sized,
    {
        let get_too_large_err = || {
            RocketPackError::builder()
                .kind(RocketPackErrorKind::TooLarge)
                .message("len too large")
                .build()
        };

        let mut id: Option<Vec<u8>> = None;
        let mut addrs: Option<Vec<OmniAddr>> = None;

        loop {
            let field_id = reader.get_u32()?;
            if field_id == 0 {
                break;
            }

            match field_id {
                1 => {
                    id = Some(reader.get_bytes(128)?);
                }
                2 => {
                    let len = reader.get_u32()?;
                    ensure_err!(len <= 128, get_too_large_err);

                    let mut vs = Vec::with_capacity(len as usize);
                    for _ in 0..len {
                        vs.push(OmniAddr::new(reader.get_string(1024)?.as_str()));
                    }
                    addrs = Some(vs);
                }
                _ => {}
            }
        }

        Ok(Self {
            id: id.ok_or_else(|| RocketPackError::builder().kind(RocketPackErrorKind::InvalidFormat).build())?,
            addrs: addrs.ok_or_else(|| RocketPackError::builder().kind(RocketPackErrorKind::InvalidFormat).build())?,
        })
    }
}
