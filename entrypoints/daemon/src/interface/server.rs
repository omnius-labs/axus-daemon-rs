use tokio::net::TcpListener;

use omnius_core_omnikit::service::remoting::{OmniRemotingDefaultErrorMessage, OmniRemotingListener};
use tracing::warn;

use crate::{prelude::*, shared::AppState};

use super::features;

pub struct RpcServer;

impl RpcServer {
    pub async fn serve(state: AppState) -> Result<()> {
        let tcp_listener = TcpListener::bind(state.conf.listen_addr.to_string()).await?;

        loop {
            let (tcp_stream, _) = tcp_listener.accept().await?;
            let (reader, writer) = tokio::io::split(tcp_stream);

            let mut remoting_listener = OmniRemotingListener::<_, _, OmniRemotingDefaultErrorMessage>::new(reader, writer, 1024 * 1024);
            remoting_listener.handshake().await?;

            match remoting_listener.function_id()? {
                0 => remoting_listener.listen(async |p| features::health(&state, p).await).await?,
                _ => warn!("not supported"),
            }

            remoting_listener.close().await?;
        }
    }
}
