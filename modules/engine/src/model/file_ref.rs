use omnius_core_omnikit::model::OmniHash;
use omnius_core_rocketpack::{RocketMessage, RocketMessageReader, RocketMessageWriter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FileRef {
    pub name: String,
    pub hash: OmniHash,
}

impl RocketMessage for FileRef {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, depth: u32) -> anyhow::Result<()> {
        writer.write_str(&value.name);
        OmniHash::pack(writer, &value.hash, depth + 1)
    }

    fn unpack(reader: &mut RocketMessageReader, depth: u32) -> anyhow::Result<Self>
    where
        Self: Sized,
    {
        let name = reader.get_string(1024)?.parse()?;
        let hash = OmniHash::unpack(reader, depth + 1)?;

        Ok(Self { name, hash })
    }
}
