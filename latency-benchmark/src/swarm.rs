//! Shared swarm-building primitives for the latency benchmark.
//!
//! The same transport (TCP + plaintext + yamux) builds both the client and the
//! server swarm; role (dial vs listen, send vs echo) is decided by the caller.
//! The workload is a single long-lived `libp2p-stream` substream per peer.

use std::{io, time::Duration};

use futures::prelude::*;
use libp2p_core::{Transport, muxing::StreamMuxerBox, upgrade::Version};
use libp2p_identity::{Keypair, PeerId};
use libp2p_swarm::Swarm;

/// Stream-protocol identifier for the long-lived-stream workload.
pub const STREAM_PROTOCOL: libp2p_swarm::StreamProtocol =
    libp2p_swarm::StreamProtocol::new("/latency-bench-stream/1");

pub(crate) async fn read_framed<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut len = [0u8; 4];
    io.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    if n > 16 * 1024 * 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }
    let mut buf = vec![0u8; n];
    io.read_exact(&mut buf).await?;
    Ok(buf)
}

pub(crate) async fn write_framed<T>(io: &mut T, data: &[u8]) -> io::Result<()>
where
    T: AsyncWrite + Unpin + Send,
{
    let len = u32::try_from(data.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "frame too large"))?;
    io.write_all(&len.to_be_bytes()).await?;
    io.write_all(data).await?;
    io.flush().await?;
    Ok(())
}

/// Build a tokio-backed swarm carrying a [`libp2p_stream::Behaviour`], so the
/// workload can open a single long-lived stream per peer.
pub fn build_stream_swarm() -> Swarm<libp2p_stream::Behaviour> {
    build_stream_internal(libp2p_swarm::Config::with_tokio_executor())
}

/// Same as [`build_stream_swarm`] but with a caller-supplied
/// [`libp2p_swarm::Executor`], so the client can run on an instrumented runtime.
pub fn build_stream_swarm_with_executor<E>(executor: E) -> Swarm<libp2p_stream::Behaviour>
where
    E: libp2p_swarm::Executor + Send + 'static,
{
    build_stream_internal(libp2p_swarm::Config::with_executor(executor))
}

fn build_stream_internal(cfg: libp2p_swarm::Config) -> Swarm<libp2p_stream::Behaviour> {
    let (identity, transport) = build_transport();
    let peer_id = PeerId::from(identity.public());
    Swarm::new(
        transport,
        libp2p_stream::Behaviour::new(),
        peer_id,
        cfg.with_idle_connection_timeout(Duration::from_secs(3600)),
    )
}

/// TCP + plaintext + yamux.
fn build_transport() -> (
    Keypair,
    libp2p_core::transport::Boxed<(PeerId, StreamMuxerBox)>,
) {
    let identity = Keypair::generate_ed25519();
    let yamux_cfg = libp2p_yamux::Config::default();
    let transport = libp2p_tcp::tokio::Transport::new(libp2p_tcp::Config::default())
        .upgrade(Version::V1)
        .authenticate(libp2p_plaintext::Config::new(&identity))
        .multiplex(yamux_cfg)
        .boxed();
    (identity, transport)
}
