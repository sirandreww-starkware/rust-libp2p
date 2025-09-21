//! Message types for the Propeller protocol.

use bytes::{Buf, BufMut, BytesMut};
use libp2p_core::PeerId;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::trace;

/// Represents a message identifier for message grouping.
pub type MessageId = u64;

/// Represents a hash of a shred.
pub(crate) type ShredHash = [u8; 32];

/// Represents a shred index within a message.
pub type ShredIndex = u32;

/// Unique identifier for a shred.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ShredId {
    /// The message id this shred belongs to.
    pub message_id: MessageId,
    /// The index of this shred within the message.
    pub index: ShredIndex,
    /// The leader that created this shred.
    pub publisher: PeerId,
}

/// A shred - the atomic unit of data transmission in Turbine.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Shred {
    /// Unique identifier for this shred.
    pub id: ShredId,
    /// The actual data payload.
    pub data: Vec<u8>,
    /// Signature for verification.
    pub signature: Vec<u8>,
}

impl Shred {
    pub fn hash_without_signature(&self) -> ShredHash {
        let mut hasher = Sha256::new();
        hasher.update(self.id.message_id.to_be_bytes());
        hasher.update(self.id.index.to_be_bytes());
        hasher.update(self.id.publisher.to_bytes());
        hasher.update(&self.data);
        hasher.finalize().into()
    }

    /// Encode the shred without signature into a byte vector for signing.
    /// This follows a similar pattern to gossipsub's message encoding for signatures.
    pub(crate) fn encode_without_signature(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.id.message_id.to_be_bytes());
        buf.extend_from_slice(&self.id.index.to_be_bytes());
        let publisher_bytes = self.id.publisher.to_bytes();
        buf.push(publisher_bytes.len() as u8);
        buf.extend_from_slice(&publisher_bytes);
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.data);
        // Note: signature is intentionally omitted for signing
        buf
    }
}

impl Shred {
    /// Encode the shred into a byte vector using length-prefixed format.
    ///
    /// Format: [total_length: u32][message_id: u64][index: u32][publisher_len: u8][publisher:
    /// bytes][data_len: u32][data: bytes][signature_len: u32][signature: bytes]
    pub fn encode(&self, dst: &mut BytesMut) {
        // Calculate total message size first
        let publisher_bytes = self.id.publisher.to_bytes();
        let total_size = std::mem::size_of::<u64>() // message_id
            + std::mem::size_of::<u32>() // index
            + std::mem::size_of::<u8>() // publisher_len
            + publisher_bytes.len() // publisher bytes
            + std::mem::size_of::<u32>() // data_len
            + self.data.len() // data bytes
            + std::mem::size_of::<u32>() // signature_len
            + self.signature.len(); // signature bytes

        trace!(
            "📤 ENCODE: Encoding shred {} (total: {} bytes, data: {} bytes)",
            self.id.index,
            total_size,
            self.data.len()
        );

        // Write total length first (this is the key for proper streaming)
        dst.put_u32(total_size as u32);

        // Write the actual message
        dst.put_u64(self.id.message_id);
        dst.put_u32(self.id.index);
        dst.put_u8(publisher_bytes.len() as u8);
        dst.put_slice(&publisher_bytes);
        dst.put_u32(self.data.len() as u32);
        dst.extend_from_slice(&self.data);
        dst.put_u32(self.signature.len() as u32);
        dst.extend_from_slice(&self.signature);
    }

    /// Decode a shred from a byte buffer using length-prefixed format.
    ///
    /// Returns Some(shred) if a complete message is available,
    /// None if more data is needed for streaming.
    pub fn decode(src: &mut BytesMut) -> Option<Self> {
        let initial_len = src.len();
        trace!(
            "🔍 DECODE: Starting decode, buffer size: {} bytes",
            initial_len
        );

        // First, we need at least size_of::<u32>() bytes for the total length
        if src.len() < std::mem::size_of::<u32>() {
            trace!(
                "⏸️ DECODE: Need more data for length header: {} < {}",
                src.len(),
                std::mem::size_of::<u32>()
            );
            return None;
        }

        // Peek at the total message length without consuming it
        let total_length = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        trace!(
            "🔍 DECODE: Message requires {} bytes total, have {} bytes",
            total_length + std::mem::size_of::<u32>(),
            src.len() // + size_of::<u32>() for the length header itself
        );

        // Check if we have the complete message
        if src.len() < std::mem::size_of::<u32>() + total_length {
            let needed = (std::mem::size_of::<u32>() + total_length) - src.len();
            trace!("⏸️ DECODE: Need {} more bytes for complete message", needed);
            return None;
        }

        // We have a complete message! Start consuming bytes
        trace!(
            "🎉 DECODE: Complete message available, decoding {} bytes",
            total_length + std::mem::size_of::<u32>()
        );

        // Consume the length header
        let _total_length = src.get_u32();

        // Decode the actual message
        let message_id = src.get_u64();
        let index = src.get_u32();

        let publisher_len = src.get_u8() as usize;
        let publisher_bytes = src.split_to(publisher_len);
        let publisher = PeerId::from_bytes(&publisher_bytes).ok()?;

        let data_len = src.get_u32() as usize;
        let data = src.split_to(data_len).to_vec();

        let signature_len = src.get_u32() as usize;
        let signature = src.split_to(signature_len).to_vec();

        trace!(
            "✅ DECODE: Successfully decoded shred {} (data: {} bytes, signature: {} bytes)",
            index,
            data.len(),
            signature.len()
        );

        // Log remaining bytes for next message (if any)
        if !src.is_empty() {
            trace!("📦 DECODE: {} bytes remaining for next message", src.len());
        }

        Some(Self {
            id: ShredId {
                message_id,
                index,
                publisher,
            },
            data,
            signature,
        })
    }
}

/// Messages exchanged in the Propeller protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PropellerMessage {
    /// A shred being propagated through the network.
    pub shred: Shred,
}

impl PropellerMessage {
    /// Encode the message into a byte vector.
    pub fn encode(&self, dst: &mut BytesMut) {
        self.shred.encode(dst);
    }

    /// Decode the message from a byte vector.
    pub fn decode(src: &mut BytesMut) -> Option<Self> {
        Shred::decode(src).map(|shred| Self { shred })
    }
}
