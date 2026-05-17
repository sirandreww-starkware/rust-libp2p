//! Shared swarm-building primitives for the latency benchmark.
//!
//! The same code constructs both the central swarm and each peer swarm; role
//! (dial vs listen, send vs echo) is decided by the caller.

use std::{io, time::Duration};

use futures::prelude::*;
use libp2p_core::{Transport, upgrade::Version};
use libp2p_identity::{Keypair, PeerId};
use libp2p_request_response as request_response;
use libp2p_swarm::Swarm;

/// Stream-protocol identifier used by the byte-echo workload.
pub const PROTOCOL: &str = "/latency-bench/1";

/// Length-prefixed `Vec<u8>` codec. 4-byte big-endian length, then the bytes.
#[derive(Clone, Default)]
pub struct ByteCodec;

impl request_response::Codec for ByteCodec {
    type Protocol = &'static str;
    type Request = Vec<u8>;
    type Response = Vec<u8>;

    async fn read_request<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Vec<u8>>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn read_response<T>(&mut self, _: &Self::Protocol, io: &mut T) -> io::Result<Vec<u8>>
    where
        T: AsyncRead + Unpin + Send,
    {
        read_framed(io).await
    }

    async fn write_request<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        req: Vec<u8>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &req).await
    }

    async fn write_response<T>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        res: Vec<u8>,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        write_framed(io, &res).await
    }
}

async fn read_framed<T>(io: &mut T) -> io::Result<Vec<u8>>
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

async fn write_framed<T>(io: &mut T, data: &[u8]) -> io::Result<()>
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

/// Build a tokio-backed swarm with TCP + plaintext + yamux carrying a single
/// request/response protocol. Uses libp2p-swarm's default tokio executor.
pub fn build_swarm() -> Swarm<request_response::Behaviour<ByteCodec>> {
    build_internal(libp2p_swarm::Config::with_tokio_executor())
}

/// Same shape as [`build_swarm`] but with a caller-supplied [`libp2p_swarm::Executor`],
/// so the central role can instrument each per-connection task with
/// `tokio_metrics::TaskMonitor`.
pub fn build_swarm_with_executor<E>(executor: E) -> Swarm<request_response::Behaviour<ByteCodec>>
where
    E: libp2p_swarm::Executor + Send + 'static,
{
    build_internal(libp2p_swarm::Config::with_executor(executor))
}

fn build_internal(cfg: libp2p_swarm::Config) -> Swarm<request_response::Behaviour<ByteCodec>> {
    let identity = Keypair::generate_ed25519();
    let peer_id = PeerId::from(identity.public());

    let transport = libp2p_tcp::tokio::Transport::new(libp2p_tcp::Config::default())
        .upgrade(Version::V1)
        .authenticate(libp2p_plaintext::Config::new(&identity))
        .multiplex(libp2p_yamux::Config::default())
        .boxed();

    let behaviour = request_response::Behaviour::<ByteCodec>::new(
        std::iter::once((PROTOCOL, request_response::ProtocolSupport::Full)),
        request_response::Config::default(),
    );

    Swarm::new(
        transport,
        behaviour,
        peer_id,
        cfg.with_idle_connection_timeout(Duration::from_secs(3600)),
    )
}
