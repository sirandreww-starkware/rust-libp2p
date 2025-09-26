//! Core types for the Propeller protocol.

use libp2p_identity::PeerId;

use crate::message::{MessageId, Shred};

/// Specific errors that can occur during shred verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShredVerificationError {
    /// No leader was set when shred was received.
    NoLeaderSet,
    /// Shred came from unexpected publisher (not the current leader).
    UnexpectedPublisher {
        /// The peer that published the shred.
        publisher: PeerId,
    },
    /// Leader should not receive shreds (they broadcast them).
    LeaderReceivingShred,
    /// Shred is already in cache (duplicate).
    DuplicateShred,
    /// Failed to get parent in tree topology.
    TreeError(PropellerError),
    /// Shred failed parent verification in tree topology.
    ParentVerificationFailed {
        /// The expected sender according to tree topology.
        expected_sender: PeerId,
    },
    /// Shred signature verification failed.
    SignatureVerificationFailed(PropellerError),
}

impl std::fmt::Display for ShredVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ShredVerificationError::NoLeaderSet => {
                write!(f, "Received shred but no leader was set yet")
            }
            ShredVerificationError::UnexpectedPublisher { publisher } => {
                write!(
                    f,
                    "Received shred from unexpected publisher (publisher = {})",
                    publisher
                )
            }
            ShredVerificationError::LeaderReceivingShred => {
                write!(f, "Leader should not receive a shred")
            }
            ShredVerificationError::DuplicateShred => {
                write!(f, "Received shred that is already in cache")
            }
            ShredVerificationError::TreeError(e) => {
                write!(f, "Received shred but error getting parent in tree: {}", e)
            }
            ShredVerificationError::ParentVerificationFailed { expected_sender } => {
                write!(
                    f,
                    "Shred failed parent verification (expected sender = {})",
                    expected_sender
                )
            }
            ShredVerificationError::SignatureVerificationFailed(e) => {
                write!(f, "Shred failed signature verification: {}", e)
            }
        }
    }
}

impl std::error::Error for ShredVerificationError {}

/// Events emitted by the Propeller behaviour.
#[derive(Debug, Clone)]
pub enum Event {
    /// A shred has been received from a peer.
    ShredReceived {
        /// The peer that sent the shred.
        peer_id: PeerId,
        /// The received shred.
        shred: Shred,
    },
    /// A complete message has been reconstructed from shreds.
    MessageReceived {
        /// The message id the message belongs to.
        message_id: MessageId,
        /// The reconstructed message data.
        data: Vec<u8>,
    },
    /// Failed to reconstruct a message from shreds.
    MessageReconstructionFailed {
        /// The message id the message belongs to.
        message_id: MessageId,
        /// The error that occurred.
        error: PropellerError,
    },
    /// Failed to send a shred to a peer.
    ShredSendFailed {
        /// The peer we failed to send to.
        peer_id: PeerId,
        /// The error that occurred.
        error: String,
    },
    /// Failed to verify shred
    ShredVerificationFailed {
        /// The peer we failed to verify the shred from. (The sender of the shred that should
        /// probably be reported)
        peer_id: PeerId,
        /// The specific verification error that occurred.
        error: ShredVerificationError,
    },
}

/// Node information for propeller tree topology.
#[derive(Clone, Debug)]
pub struct PropellerNode {
    /// The peer ID of this node.
    pub peer_id: PeerId,
    /// The weight of this node for tree positioning.
    pub weight: u64,
}

/// Errors that can occur in the Propeller protocol.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropellerError {
    /// This node is not the current leader and cannot broadcast.
    NotLeader,
    /// Shred validation failed.
    InvalidShred,
    /// Shred signature verification failed.
    ShredSignatureVerificationFailed(String),
    /// Signing failed.
    SigningFailed(String),
    /// Erasure encoding operation failed.
    ErasureEncodingFailed(String),
    /// Erasure decoding operation failed.
    ErasureDecodingFailed(String),
    /// Tree generation failed.
    TreeGenerationFailed,
    /// Peer not found in tree.
    PeerNotFound,
    /// Invalid data size for broadcasting.
    InvalidDataSize,
    /// Protocol error with message.
    ProtocolError(String),
    /// Leader not set.
    LeaderNotSet,
    /// Peer not connected.
    PeerNotConnected(PeerId),
    /// Invalid public key.
    InvalidPublicKey,
    /// Local peer not in peer weights.
    LocalPeerNotInPeerWeights,
    /// IO error with message.
    IoError(String),
}

impl std::fmt::Display for PropellerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PropellerError::NotLeader => write!(f, "This node is not the current leader"),
            PropellerError::InvalidShred => write!(f, "Shred validation failed"),
            PropellerError::ShredSignatureVerificationFailed(msg) => write!(f, "Shred signature verification failed: {}", msg),
            PropellerError::ErasureEncodingFailed(msg) => write!(f, "Erasure encoding operation failed: {}", msg),
            PropellerError::ErasureDecodingFailed(msg) => write!(f, "Erasure decoding operation failed: {}", msg),
            PropellerError::TreeGenerationFailed => write!(f, "Tree generation failed"),
            PropellerError::PeerNotFound => write!(f, "Peer not found in tree"),
            PropellerError::InvalidDataSize => write!(
                f,
                "Invalid data size for broadcasting, data size must be divisible by number of data shreds"
            ),
            PropellerError::SigningFailed(msg) => write!(f, "Signing failed: {}", msg),
            PropellerError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            PropellerError::IoError(msg) => write!(f, "IO error: {}", msg),
            PropellerError::LeaderNotSet => write!(f, "Leader not set when it should be"),
            PropellerError::InvalidPublicKey => {
                write!(
                    f,
                    "Invalid public key (does not match PeerId), or no public key available"
                )
            }
            PropellerError::LocalPeerNotInPeerWeights => {
                write!(f, "Local peer not in peer weights")
            }
            PropellerError::PeerNotConnected(peer_id) => {
                write!(f, "Peer not connected: {}", peer_id)
            }
        }
    }
}

impl std::error::Error for PropellerError {}
