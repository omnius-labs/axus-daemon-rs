use fast_socks5::client::Socks5Stream;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
};

mod tcp;

pub use tcp::*;

pub trait AsyncStream: AsyncRead + AsyncWrite {}

impl AsyncStream for TcpStream {}
impl AsyncStream for Socks5Stream<TcpStream> {}
