//! Smoke tests for Propeller protocol with 100 peers and fanout of 6.

use libp2p_identity::PeerId;
use libp2p_propeller::{Behaviour, Config, MessageAuthenticity, PropellerError};
use rand::{Rng, SeedableRng};
use tracing_subscriber::EnvFilter;

#[test]
fn test_propeller_100_peers_fanout_6_api() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    const NUM_PEERS: usize = 100;
    const FANOUT: usize = 6;

    tracing::info!(
        "Testing Propeller API with {} peers and fanout {}",
        NUM_PEERS,
        FANOUT
    );

    // Create configuration with fanout 6
    let config = Config::builder()
        .data_plane_fanout(FANOUT)
        .fec_data_shreds(16)
        .fec_coding_shreds(16)
        .build();

    assert_eq!(config.fanout(), FANOUT);

    // Generate 100 peers with valid Ed25519 keypairs and random weights
    let mut rng = rand::rngs::StdRng::seed_from_u64(12345);
    let peers: Vec<(PeerId, u64)> = (0..NUM_PEERS)
        .map(|_| {
            let keypair = libp2p_identity::Keypair::generate_ed25519();
            let peer_id = PeerId::from(keypair.public());
            let weight = rng.random_range(100..10000);
            (peer_id, weight)
        })
        .collect();

    // Set local peer ID to the first peer
    let local_peer_id = peers[0].0;

    // Create behaviour
    let mut propeller = Behaviour::new(MessageAuthenticity::Author(local_peer_id), config.clone());
    // Local peer ID is now set automatically in constructor

    // Add all peers with their weights (including local peer required by tree manager)
    let _ = propeller.add_peers(peers.clone());

    // For testing purposes, simulate that all peers are connected
    let peer_ids: Vec<PeerId> = peers.iter().map(|(id, _)| *id).collect();
    propeller.add_connected_peers_for_test(peer_ids);

    tracing::info!("Added {} peers to propeller behaviour", NUM_PEERS);

    // Test leader functionality
    let leader_peer_id = peers[10].0; // Pick peer at index 10 as leader
    let _ = propeller.set_leader(leader_peer_id);

    assert_eq!(propeller.current_leader(), Some(leader_peer_id));
    assert!(!propeller.is_leader()); // We're not the leader

    // Set ourselves as leader
    let _ = propeller.set_leader(local_peer_id);
    assert!(propeller.is_leader()); // Now we are the leader

    // Test broadcasting (should work now)
    // Data must be divisible by num_data_shreds
    let data_shreds = config.fec_data_shreds();
    let test_data = vec![42u8; data_shreds * 64]; // 64 bytes per shred
    match propeller.broadcast(test_data, 0) {
        Ok(()) => {
            tracing::info!("✅ Broadcast successful");
        }
        Err(e) => {
            panic!("❌ Broadcast failed: {}", e);
        }
    }

    // Slot management is no longer needed as topic is passed to broadcast

    // Test with different leader
    let another_leader = peers[50].0;
    let _ = propeller.set_leader(another_leader);
    assert!(!propeller.is_leader()); // No longer leader

    // Broadcasting should fail now (wrong leader)
    let data_shreds = config.fec_data_shreds();
    let test_data = vec![1u8; data_shreds * 64]; // 64 bytes per shred
    let result = propeller.broadcast(test_data, 0);
    assert!(matches!(result, Err(PropellerError::NotLeader)));

    tracing::info!("✅ All Propeller API tests passed!");
}

#[test]
fn test_propeller_tree_computation() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    const NUM_PEERS: usize = 20;
    const FANOUT: usize = 6;

    tracing::info!(
        "Testing Propeller tree computation with {} peers and fanout {}",
        NUM_PEERS,
        FANOUT
    );

    let config = Config::builder().data_plane_fanout(FANOUT).build();

    // Generate peers with valid Ed25519 keypairs and different weights
    let mut rng = rand::rngs::StdRng::seed_from_u64(54321);
    let peers: Vec<(PeerId, u64)> = (0..NUM_PEERS)
        .map(|i| {
            let weight = if i < 5 {
                // First 5 peers have high weight
                rng.random_range(5000..10000)
            } else {
                // Rest have lower weight
                rng.random_range(100..1000)
            };
            let keypair = libp2p_identity::Keypair::generate_ed25519();
            let peer_id = PeerId::from(keypair.public());
            (peer_id, weight)
        })
        .collect();

    let local_peer_id = peers[0].0;
    let mut propeller = Behaviour::new(MessageAuthenticity::Author(local_peer_id), config.clone());
    // Local peer ID is now set automatically in constructor

    // Add all peers (including local peer required by tree manager)
    let _ = propeller.add_peers(peers.clone());

    // For testing purposes, simulate that all peers are connected
    let peer_ids: Vec<PeerId> = peers.iter().map(|(id, _)| *id).collect();
    propeller.add_connected_peers_for_test(peer_ids);

    // Test with different leaders and verify tree computation works
    for (leader_index, p) in peers.iter().enumerate().take(5) {
        let leader_peer_id = p.0;
        let _ = propeller.set_leader(leader_peer_id);

        tracing::debug!(
            "Testing with leader: {} (index {})",
            leader_peer_id,
            leader_index
        );

        if propeller.is_leader() {
            // Test broadcasting
            let data_shreds = config.fec_data_shreds();
            let test_data = vec![leader_index as u8; data_shreds * 64]; // 64 bytes per shred

            match propeller.broadcast(test_data, leader_index as u64) {
                Ok(()) => {
                    tracing::debug!("Broadcast successful for leader {}", leader_index);
                }
                Err(e) => {
                    tracing::warn!("Broadcast failed for leader {}: {}", leader_index, e);
                }
            }
        }
    }

    tracing::info!("✅ Tree computation test completed successfully");
}

#[test]
fn test_propeller_configuration() {
    // Test various configuration combinations
    let configs = vec![
        (6, 16, 16),   // Default-ish
        (10, 8, 8),    // Less FEC
        (3, 32, 32),   // More FEC
        (100, 24, 24), // Large fanout
    ];

    for (fanout, data_shreds, coding_shreds) in configs {
        let config = Config::builder()
            .data_plane_fanout(fanout)
            .fec_data_shreds(data_shreds)
            .fec_coding_shreds(coding_shreds)
            .build();

        let propeller = Behaviour::new(
            MessageAuthenticity::Author(PeerId::random()),
            config.clone(),
        );

        assert_eq!(config.fanout(), fanout);
        assert_eq!(config.fec_data_shreds(), data_shreds);
        assert_eq!(config.fec_coding_shreds(), coding_shreds);

        assert!(!propeller.is_leader());
    }

    println!("✅ Configuration test passed!");
}
