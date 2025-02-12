use omnius_core_omnikit::model::OmniHash;

pub struct PublishedBlock {
    pub root_hash: OmniHash,
    pub block_hash: OmniHash,
    pub depth: u32,
    pub index: u32,
}
