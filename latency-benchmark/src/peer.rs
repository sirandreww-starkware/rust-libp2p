//! Server role: listen on TCP, accept the one stream protocol, and echo framed
//! messages back. Prints its listen multiaddr to stdout (one line) so the
//! orchestrator can find it.

use std::error::Error;

use futures::StreamExt;
use libp2p_swarm::SwarmEvent;

use crate::swarm::{STREAM_PROTOCOL, build_stream_swarm, read_framed, write_framed};

#[derive(Debug, clap::Args)]
pub struct PeerArgs {
    /// IP:port to listen on. Defaults to 0.0.0.0:0 (random port).
    #[arg(long, default_value = "/ip4/0.0.0.0/tcp/0")]
    pub listen: String,

    /// Tokio worker thread count for this process. Default 1 to keep the server
    /// single-threaded and isolated from other scenarios' scheduling.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,
}

pub async fn run(args: PeerArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut swarm = build_stream_swarm();
    let mut incoming = swarm.behaviour().new_control().accept(STREAM_PROTOCOL)?;
    let listen_addr: libp2p_core::Multiaddr = args.listen.parse()?;
    swarm.listen_on(listen_addr)?;

    let mut printed_addr = false;
    loop {
        tokio::select! {
            event = swarm.next() => match event {
                Some(SwarmEvent::NewListenAddr { address, .. }) => {
                    print_addr_once(&mut printed_addr, address, *swarm.local_peer_id());
                }
                Some(_) => {}
                None => break,
            },
            // One long-lived stream per peer: echo framed messages until it closes.
            Some((_peer, mut stream)) = incoming.next() => {
                tokio::spawn(async move {
                    while let Ok(msg) = read_framed(&mut stream).await {
                        if write_framed(&mut stream, &msg).await.is_err() {
                            break;
                        }
                    }
                });
            }
        }
    }
    Ok(())
}

fn print_addr_once(
    printed: &mut bool,
    address: libp2p_core::Multiaddr,
    peer_id: libp2p_identity::PeerId,
) {
    if !*printed {
        let full = address.with(libp2p_core::multiaddr::Protocol::P2p(peer_id));
        // Single line on stdout; the orchestrator reads exactly one.
        println!("{full}");
        *printed = true;
    }
}
