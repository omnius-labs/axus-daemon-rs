use omnius_core_omnikit::model::OmniHash;
use omnius_core_rocketpack::{RocketMessage, RocketMessageReader, RocketMessageWriter};

pub struct MerkleLayer {
    pub rank: u32,
    pub hashes: Vec<OmniHash>,
}

impl RocketMessage for MerkleLayer {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, depth: u32) -> anyhow::Result<()> {
        writer.put_u32(value.rank);

        writer.put_u32(value.hashes.len().try_into()?);
        for v in &value.hashes {
            OmniHash::pack(writer, v, depth + 1)?;
        }

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let rank = reader.get_u32()?;

        let len = reader.get_u32()?;
        if len > 128 {
            anyhow::bail!("len too large");
        }
        let mut hashes = Vec::with_capacity(len.try_into()?);
        for _ in 0..len {
            hashes.push(OmniHash::unpack(reader, depth + 1)?);
        }

        Ok(Self { rank, hashes })
    }
}
