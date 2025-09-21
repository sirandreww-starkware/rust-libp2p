//! Dynamic propeller tree computation logic.
//!
//! This module implements the core tree topology algorithms inspired by Solana's Turbine protocol.
//! The tree is computed dynamically for each shred using deterministic seeded randomization
//! based on the leader and shred ID, making the network resilient to targeted attacks.

use std::collections::HashMap;

use libp2p_identity::PeerId;
use rand::{
    distr::{weighted::WeightedIndex, Distribution},
    SeedableRng,
};
use rand_chacha::ChaChaRng;

use crate::{
    message::ShredHash,
    types::{PropellerError, PropellerNode},
};

/// Propeller tree manager that computes tree topology dynamically for each shred.
#[derive(Debug, Clone)]
pub(crate) struct PropellerTreeManager {
    /// All nodes in the cluster with their weights, sorted by (weight, peer_id) descending.
    nodes: Vec<PropellerNode>,
    /// This node's peer ID.
    local_peer_id: PeerId,
    /// The current leader peer ID.
    leader: Option<(PeerId, usize)>,
    /// Data plane fanout (number of children each node has).
    fanout: usize,
}

impl PropellerTreeManager {
    /// Create a new propeller tree manager.
    pub(crate) fn new(local_peer_id: PeerId, fanout: usize) -> Self {
        Self {
            nodes: Vec::new(),
            local_peer_id,
            leader: None,
            fanout,
        }
    }

    pub(crate) fn get_local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    pub(crate) fn set_leader(&mut self, leader_id: PeerId) -> Result<(), PropellerError> {
        let leader_index = self
            .nodes
            .iter()
            .position(|node| node.peer_id == leader_id)
            .ok_or(PropellerError::PeerNotFound)?;
        self.leader = Some((leader_id, leader_index));
        Ok(())
    }

    /// Get the current leader peer ID.
    pub(crate) fn current_leader(&self) -> Option<PeerId> {
        self.leader.map(|(leader_id, _)| leader_id)
    }

    /// Check if this node is currently the leader.
    pub(crate) fn is_leader(&self) -> bool {
        if let Some((leader_id, _)) = self.leader {
            leader_id == self.local_peer_id
        } else {
            false
        }
    }

    /// Update the cluster nodes with their weights.
    /// Nodes are sorted by (weight, peer_id) in descending order for deterministic behavior.
    ///
    /// Note: invalidates the leader (must be set again)
    pub(crate) fn update_nodes(
        &mut self,
        peer_weights: &HashMap<PeerId, u64>,
    ) -> Result<(), PropellerError> {
        if !peer_weights.contains_key(&self.local_peer_id) {
            return Err(PropellerError::LocalPeerNotInPeerWeights);
        }

        // Convert to PropellerNode and sort by weight descending, then by peer_id for determinism
        let mut nodes: Vec<PropellerNode> = peer_weights
            .iter()
            .map(|(&peer_id, &weight)| PropellerNode { peer_id, weight })
            .collect();

        nodes.sort_by(|a, b| {
            // Sort by weight descending, then by peer_id ascending for determinism
            b.weight
                .cmp(&a.weight)
                .then_with(|| a.peer_id.cmp(&b.peer_id))
        });

        self.nodes = nodes;
        self.leader = None;
        Ok(())
    }

    fn build_index_tree(&self, shred_hash: &ShredHash) -> Result<Vec<usize>, PropellerError> {
        let mut rng = ChaChaRng::from_seed(*shred_hash);

        let mut indices_to_choose_from: Vec<usize> = (0..self.nodes.len()).collect();
        let mut result = Vec::with_capacity(self.nodes.len());

        // remove the leader from the indices (should be at index 0 at the end)
        let leader_index = self.leader.as_ref().ok_or(PropellerError::LeaderNotSet)?.1;
        indices_to_choose_from.remove(leader_index);
        result.push(leader_index);

        for _ in 0..indices_to_choose_from.len() {
            let weights: Vec<_> = indices_to_choose_from
                .iter()
                .map(|index| self.nodes[*index].weight)
                .collect();
            let weighted_index = WeightedIndex::new(weights).unwrap();
            let index_in_indices_to_choose_from = weighted_index.sample(&mut rng);
            let index = indices_to_choose_from.swap_remove(index_in_indices_to_choose_from);
            result.push(index);
        }

        assert_eq!(
            result.len(),
            self.nodes.len(),
            "All indices should be present"
        );
        assert!(
            {
                let mut a = result.clone();
                a.sort();
                a.dedup();
                a.len() == self.nodes.len()
            },
            "All indices should be unique"
        );
        assert!(
            result.iter().all(|index| index < &self.nodes.len()),
            "All indices should be less than the number of nodes"
        );
        assert!(
            self.nodes[result[0]].peer_id == self.leader.as_ref().unwrap().0,
            "Leader should be at index 0"
        );

        Ok(result)
    }

    pub(crate) fn get_children_in_tree(
        &self,
        shred_hash: &ShredHash,
    ) -> Result<Vec<PeerId>, PropellerError> {
        let tree = self.build_index_tree(shred_hash)?;
        let our_position = tree
            .iter()
            .position(|index| self.local_peer_id == self.nodes[*index].peer_id)
            .ok_or(PropellerError::LocalPeerNotInPeerWeights)?;
        assert_eq!(self.nodes[tree[our_position]].peer_id, self.local_peer_id);
        let children = get_tree_children(our_position, self.fanout, self.nodes.len())
            .into_iter()
            .map(|index| self.nodes[tree[index]].peer_id)
            .collect();

        Ok(children)
    }

    pub(crate) fn get_parent_in_tree(
        &self,
        shred_hash: &ShredHash,
    ) -> Result<Option<PeerId>, PropellerError> {
        let tree = self.build_index_tree(shred_hash)?;
        let our_position = tree
            .iter()
            .position(|index| self.local_peer_id == self.nodes[*index].peer_id)
            .ok_or(PropellerError::LocalPeerNotInPeerWeights)?;
        let parent = get_tree_parent(our_position, self.fanout);
        Ok(parent.map(|index| self.nodes[tree[index]].peer_id))
    }
}

/// Get the parent position for a node at the given position in a tree.
/// Returns the parent position in the shuffled node list, or None if this is the root.
fn get_tree_parent(position: usize, fanout: usize) -> Option<usize> {
    if position == 0 {
        None // Root has no parent
    } else {
        Some((position - 1) / fanout)
    }
}

/// Get the children indices for a node at the given position in a tree.
/// Returns the positions of children in the shuffled node list.
fn get_tree_children(position: usize, fanout: usize, total_nodes: usize) -> Vec<usize> {
    let mut children = Vec::new();

    // First child position = position * fanout + 1
    let first_child = position * fanout + 1;

    // Add up to `fanout` children
    for i in 0..fanout {
        let child_pos = first_child + i;
        if child_pos < total_nodes {
            children.push(child_pos);
        } else {
            break;
        }
    }

    children
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tree_parent_child_relationships() {
        // Test with fanout 2
        assert_eq!(get_tree_parent(0, 2), None); // Root has no parent
        assert_eq!(get_tree_parent(1, 2), Some(0)); // Child of root
        assert_eq!(get_tree_parent(2, 2), Some(0)); // Child of root
        assert_eq!(get_tree_parent(3, 2), Some(1)); // Child of node 1
        assert_eq!(get_tree_parent(4, 2), Some(1)); // Child of node 1
        assert_eq!(get_tree_parent(5, 2), Some(2)); // Child of node 2

        // Test children
        assert_eq!(get_tree_children(0, 2, 10), vec![1, 2]); // Root's children
        assert_eq!(get_tree_children(1, 2, 10), vec![3, 4]); // Node 1's children
        assert_eq!(get_tree_children(2, 2, 10), vec![5, 6]); // Node 2's children
    }
}
