//! Peer role: listen on TCP, echo every request-response, print the listen
//! multiaddr to stdout (one line) so the orchestrator can find us.

use std::error::Error;

use futures::StreamExt;
use libp2p_request_response as request_response;
use libp2p_swarm::SwarmEvent;

use crate::swarm::build_swarm;

#[derive(Debug, clap::Args)]
pub struct PeerArgs {
    /// IP:port to listen on. Defaults to 0.0.0.0:0 (random port).
    #[arg(long, default_value = "/ip4/0.0.0.0/tcp/0")]
    pub listen: String,

    /// Tokio worker thread count for this process. Default 1 to keep each
    /// peer single-threaded and isolated from other peers' scheduling.
    #[arg(long, default_value_t = 1)]
    pub worker_threads: usize,
}

pub async fn run(args: PeerArgs) -> Result<(), Box<dyn Error + Send + Sync>> {
    let mut swarm = build_swarm();
    let listen_addr: libp2p_core::Multiaddr = args.listen.parse()?;
    swarm.listen_on(listen_addr)?;

    let mut printed_addr = false;
    loop {
        let event = match swarm.next().await {
            Some(ev) => ev,
            None => break,
        };
        match event {
            SwarmEvent::NewListenAddr { address, .. } => {
                if !printed_addr {
                    // First listener address: print {addr}/p2p/{peer_id} so the
                    // orchestrator can dial us directly.
                    let peer_id = *swarm.local_peer_id();
                    let full = address.with(libp2p_core::multiaddr::Protocol::P2p(peer_id));
                    // Single line on stdout; the orchestrator reads exactly one.
                    println!("{full}");
                    printed_addr = true;
                }
            }
            SwarmEvent::Behaviour(request_response::Event::Message {
                message:
                    request_response::Message::Request {
                        request, channel, ..
                    },
                ..
            }) => {
                let _ = swarm.behaviour_mut().send_response(channel, request);
            }
            _ => {}
        }
    }
    Ok(())
}
