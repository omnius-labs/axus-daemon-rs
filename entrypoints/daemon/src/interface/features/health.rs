use omnius_core_omnikit::service::remoting::OmniRemotingDefaultErrorMessage;
use omnius_core_rocketpack::{EmptyRocketMessage, RocketMessage, RocketMessageReader, RocketMessageWriter};

use crate::{prelude::*, shared::AppState};

pub async fn health(state: &AppState, _: EmptyRocketMessage) -> std::result::Result<HealthResponse, OmniRemotingDefaultErrorMessage> {
    let res = HealthResponse {
        git_tag: state.info.git_tag.to_string(),
    };
    Ok(res)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthResponse {
    pub git_tag: String,
}

impl RocketMessage for HealthResponse {
    fn pack(writer: &mut RocketMessageWriter, value: &Self, _depth: u32) -> RocketPackResult<()> {
        writer.put_str(&value.git_tag);

        Ok(())
    }

    fn unpack(reader: &mut RocketMessageReader, _depth: u32) -> RocketPackResult<Self>
    where
        Self: Sized,
    {
        let git_tag = reader.get_string(1024)?;

        Ok(Self { git_tag })
    }
}
