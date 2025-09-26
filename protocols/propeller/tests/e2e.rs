//! End-to-end tests with large networks and leader rotation.

use std::{collections::HashMap, time::Duration};

use futures::{stream::SelectAll, StreamExt};
use libp2p_identity::PeerId;
use libp2p_propeller::{Behaviour, Config, Event, MessageAuthenticity, MessageId};
use libp2p_swarm::Swarm;
use libp2p_swarm_test::SwarmExt as _;
use rand::{Rng, SeedableRng};
use rstest::rstest;
use tracing_subscriber::EnvFilter;

async fn create_swarm() -> Swarm<Behaviour> {
    use libp2p_core::{transport::MemoryTransport, upgrade::Version, Transport as _};
    use libp2p_identity::Keypair;

    let config = Config::builder()
        .emit_shred_received_events(true)
        .validation_mode(libp2p_propeller::ValidationMode::None)
        .build();

    let identity = Keypair::generate_ed25519();
    let peer_id = PeerId::from(identity.public());

    let transport = MemoryTransport::default()
        .or_transport(libp2p_tcp::tokio::Transport::default())
        .upgrade(Version::V1)
        .authenticate(libp2p_plaintext::Config::new(&identity))
        .multiplex(libp2p_yamux::Config::default())
        .timeout(Duration::from_secs(120))
        .boxed();

    // Use a much longer idle connection timeout to prevent disconnections during long tests
    let swarm_config = libp2p_swarm::Config::with_tokio_executor()
        .with_idle_connection_timeout(Duration::from_secs(3600)); // 1 hour

    Swarm::new(
        transport,
        Behaviour::new(MessageAuthenticity::Signed(identity), config),
        peer_id,
        swarm_config,
    )
}

async fn setup_network(num_nodes: usize) -> (Vec<Swarm<Behaviour>>, Vec<PeerId>) {
    let mut swarms = Vec::with_capacity(num_nodes);
    let mut peer_ids = Vec::with_capacity(num_nodes);

    for _ in 0..num_nodes {
        let mut swarm = create_swarm().await;
        let peer_id = *swarm.local_peer_id();

        swarm.listen().with_memory_addr_external().await;

        peer_ids.push(peer_id);
        swarms.push(swarm);
    }

    connect_all_peers(&mut swarms).await;
    add_all_peers(&mut swarms, &peer_ids);

    (swarms, peer_ids)
}

async fn connect_all_peers(swarms: &mut [Swarm<Behaviour>]) {
    let num_nodes = swarms.len();

    for i in 0..num_nodes {
        for j in (i + 1)..num_nodes {
            let (left, right) = swarms.split_at_mut(j);
            let swarm_i = &mut left[i];
            let swarm_j = &mut right[0];

            swarm_j.connect(swarm_i).await;
        }
    }
}

fn add_all_peers(swarms: &mut [Swarm<Behaviour>], peer_ids: &[PeerId]) {
    let peer_weights: Vec<(PeerId, u64)> =
        peer_ids.iter().map(|&peer_id| (peer_id, 1000)).collect();

    for swarm in swarms.iter_mut() {
        let _ = swarm.behaviour_mut().add_peers(peer_weights.clone());
    }
}

fn set_leader(swarms: &mut [Swarm<Behaviour>], leader_peer_id: PeerId) {
    for swarm in swarms.iter_mut() {
        let _ = swarm.behaviour_mut().set_leader(leader_peer_id);
    }
}

async fn collect_message_events(
    swarms: &mut [Swarm<Behaviour>],
    number_of_messages: usize,
    leader_idx: usize,
    early_stop: bool,
) -> (
    HashMap<(usize, MessageId), Vec<u8>>,
    HashMap<(usize, MessageId, u32), Vec<u8>>,
) {
    let mut received_messages: HashMap<(usize, MessageId), Vec<u8>> = HashMap::new();
    let mut received_shreds: HashMap<(usize, MessageId, u32), Vec<u8>> = HashMap::new();
    tracing::info!("🔍 Collecting events, need {} messages", number_of_messages);

    // Create a SelectAll to efficiently poll all swarm streams
    let mut select_all = SelectAll::new();

    // Add each swarm's stream with its index
    for (node_idx, swarm) in swarms.iter_mut().enumerate() {
        let stream = swarm.map(move |event| (node_idx, event));
        select_all.push(stream);
    }

    let number_of_shreds = (Config::default().fec_data_shreds()
        + Config::default().fec_coding_shreds())
        * number_of_messages;

    while let Some((node_idx, swarm_event)) = select_all.next().await {
        if let Ok(event) = swarm_event.try_into_behaviour_event() {
            match event {
                Event::ShredReceived { peer_id: _, shred } => {
                    if received_shreds.contains_key(&(
                        node_idx,
                        shred.id.message_id,
                        shred.id.index,
                    )) {
                        panic!(
                            "🚨 DUPLICATE SHRED: Node {} received a duplicate shred! This should not happen. message_id={}, index={}",
                            node_idx, shred.id.message_id, shred.id.index
                        );
                    }
                    received_shreds
                        .insert((node_idx, shred.id.message_id, shred.id.index), shred.data);
                    tracing::info!(
                        "📨 Node {} received shred for message_id={} index={} ({}/{})",
                        node_idx,
                        shred.id.message_id,
                        shred.id.index,
                        received_shreds.len(),
                        number_of_shreds,
                    );
                    if number_of_shreds == received_shreds.len() {
                        break;
                    }
                }
                Event::MessageReceived { data, message_id } => {
                    if received_messages.contains_key(&(node_idx, message_id)) {
                        panic!(
                                "🚨 DUPLICATE MESSAGE: Node {} received a duplicate message! This should not happen. message_id: {}",
                                node_idx, message_id
                            );
                    }
                    assert!(received_messages.len() < number_of_messages);
                    assert_ne!(node_idx, leader_idx);
                    received_messages.insert((node_idx, message_id), data);
                    tracing::info!(
                        "📨 Node {} received message {} ({}/{})",
                        node_idx,
                        message_id,
                        received_messages.len(),
                        number_of_messages
                    );
                    if received_messages.len() == number_of_messages && early_stop {
                        break;
                    }
                }
                Event::ShredSendFailed { peer_id, error } => {
                    panic!(
                        "Node {} failed to send shred to peer {}: {}",
                        node_idx, peer_id, error
                    );
                }
                Event::ShredVerificationFailed { peer_id, error } => {
                    panic!(
                        "Node {} failed to verify shred from peer {}: {}",
                        node_idx, peer_id, error
                    );
                }
                Event::MessageReconstructionFailed { message_id, error } => {
                    panic!(
                            "Node {} failed to reconstruct message from shreds: message_id={}, error={}",
                            node_idx, message_id, error
                        );
                }
            }
        }
    }

    (received_messages, received_shreds)
}

// fn collect_a_bit_more_message_events(swarms: &mut [Swarm<Behaviour>]) {
//     for swarm in swarms.iter_mut() {
//         swarm.select_next_some().now_or_never();
//     }
// }

#[rstest]
#[case::two_nodes_big_message(2, 1, 1, 1<<23, false)]
#[case::single_message_single_leader_1kb(100, 1, 1, 1024, false)]
#[case::multiple_messages_single_leader_1kb(100, 2, 1, 1024, false)]
#[case::single_message_multiple_leaders_1kb(100, 1, 2, 1024, false)]
// #[case::multiple_messages_multiple_leaders_1kb(3, 4, 1024, false)]
#[case::single_message_2kb(100, 1, 1, 1<<11, false)]
#[case::single_message_4kb(100, 1, 1, 1<<12, false)]
#[case::single_message_8kb(100, 1, 1, 1<<13, false)]
#[case::single_message_16kb(100, 1, 1, 1<<14, false)]
#[case::single_message_32kb(100, 1, 1, 1<<15, false)]
#[case::single_message_64kb(100, 1, 1, 1<<16, false)]
// #[case::single_message_128kb_early_stop(100, 1, 1, 1<<17, true)]
// #[case::single_message_256kb_early_stop(100, 1, 1, 1<<18, true)]
#[tokio::test]
async fn test_100_swarms(
    #[case] num_nodes: usize,
    #[case] number_of_messages: usize,
    #[case] number_of_leaders: usize,
    #[case] message_size: usize,
    #[case] early_stop: bool,
) {
    use tracing::level_filters::LevelFilter;

    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .try_init();

    let (mut swarms, peer_ids) = setup_network(num_nodes).await;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    for leader_idx in (0..num_nodes).step_by(num_nodes / number_of_leaders) {
        tracing::info!("🔄 Starting rotation to leader {}", leader_idx);

        let leader_peer_id = peer_ids[leader_idx];
        tracing::info!("🎯 Setting leader to peer_id: {}", leader_peer_id);
        set_leader(&mut swarms, leader_peer_id);
        assert!(
            swarms[leader_idx].behaviour().is_leader(),
            "Node {} should be leader",
            leader_idx
        );
        tracing::info!("✅ Leader {} confirmed", leader_idx);

        tracing::info!("🔄 Creating test messages and shreds");
        let mut test_messages = HashMap::new();
        let mut test_shreds = HashMap::new();
        for _ in 0..number_of_messages {
            // Generate test message with the specified size
            let mut test_message = Vec::with_capacity(message_size);
            for _ in 0..message_size {
                test_message.push(rng.random::<u8>());
            }
            let message_id = rng.random::<u64>();
            let shreds = swarms[leader_idx]
                .behaviour_mut()
                .create_shreds_from_data(test_message.clone(), message_id)
                .unwrap();

            test_messages.insert(message_id, test_message);
            for shred in shreds {
                test_shreds.insert((leader_idx, message_id, shred.id.index), shred.data);
            }
        }

        for (message_id, test_message) in test_messages.iter() {
            tracing::info!(
                "📡 Leader {} broadcasting message {} of {} bytes",
                leader_idx,
                message_id,
                test_message.len()
            );
            swarms[leader_idx]
                .behaviour_mut()
                .broadcast(test_message.clone(), *message_id)
                .unwrap();
        }

        // Check connection status before broadcasting
        let connected_count = swarms[leader_idx].connected_peers().count();
        tracing::info!(
            "🔗 Leader {} has {} connected peers",
            leader_idx,
            connected_count
        );

        tracing::info!("⏳ Collecting message events for leader {}...", leader_idx);
        let (received_messages, received_shreds) = collect_message_events(
            &mut swarms,
            number_of_messages * (num_nodes - 1),
            leader_idx,
            early_stop,
        )
        .await;
        assert_eq!(
            received_messages.len(),
            number_of_messages * (num_nodes - 1)
        );
        if !early_stop {
            assert_eq!(
                received_shreds.len(),
                number_of_messages
                    * (num_nodes - 1)
                    * (Config::default().fec_data_shreds() + Config::default().fec_coding_shreds()),
            );
        }

        for ((node_idx, message_id), message) in received_messages {
            let test_message = test_messages.get(&message_id).unwrap();
            assert_eq!(
                &message, test_message,
                "Node {} received incorrect reconstructed message from leader {}: message_id={}",
                node_idx, leader_idx, message_id
            );
        }

        for ((node_idx, message_id, index), shred) in received_shreds {
            let test_shred = test_shreds.get(&(leader_idx, message_id, index)).unwrap();
            assert_eq!(
                &shred, test_shred,
                "Node {} received incorrect shred from leader {}: message_id={}, index={}",
                node_idx, leader_idx, message_id, index
            );
        }

        tracing::info!("✅ ✅ ✅ Leader {} broadcast successful", leader_idx);
    }
}
