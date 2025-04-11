use omnius_core_omnikit::model::OmniHash;
use omnius_core_rocketpack::{Result as RocketPackResult, RocketMessage, RocketMessageReader, RocketMessageWriter};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AssetKey {
    pub typ: String,
    pub hash: OmniHash,
}

impl RocketMessage for AssetKey {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, depth: u32) -> RocketPackResult<()> {
        writer.put_str(value.typ.to_string().as_str());
        OmniHash::pack(writer, &value.hash, depth + 1)?;

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, depth: u32) -> RocketPackResult<Self>
    where
        Self: Sized,
    {
        let typ = reader.get_string(1024)?.parse()?;
        let hash = OmniHash::unpack(reader, depth + 1)?;

        Ok(Self { typ, hash })
    }
}
