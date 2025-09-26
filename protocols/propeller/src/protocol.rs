//! Propeller protocol definitions and message handling.

use std::{convert::Infallible, io, pin::Pin};

use asynchronous_codec::{Decoder, Encoder, Framed};
use bytes::BytesMut;
use futures::{future, prelude::*};
use libp2p_core::{InboundUpgrade, OutboundUpgrade, UpgradeInfo};
use libp2p_swarm::StreamProtocol;
use tracing::trace;

use crate::message::PropellerMessage;

/// Propeller protocol upgrade for libp2p streams.
#[derive(Debug, Clone)]
pub struct PropellerProtocol {
    protocol_id: StreamProtocol,
}

impl PropellerProtocol {
    /// Create a new Propeller protocol.
    pub fn new(protocol_id: StreamProtocol) -> Self {
        Self { protocol_id }
    }
}

impl UpgradeInfo for PropellerProtocol {
    type Info = StreamProtocol;
    type InfoIter = std::iter::Once<Self::Info>;

    fn protocol_info(&self) -> Self::InfoIter {
        std::iter::once(self.protocol_id.clone())
    }
}

impl<TSocket> InboundUpgrade<TSocket> for PropellerProtocol
where
    TSocket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Framed<TSocket, PropellerCodec>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_inbound(self, socket: TSocket, _: Self::Info) -> Self::Future {
        let codec = PropellerCodec::default();
        Box::pin(future::ok(Framed::new(socket, codec)))
    }
}

impl<TSocket> OutboundUpgrade<TSocket> for PropellerProtocol
where
    TSocket: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    type Output = Framed<TSocket, PropellerCodec>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Output, Self::Error>> + Send>>;

    fn upgrade_outbound(self, socket: TSocket, _: Self::Info) -> Self::Future {
        let codec = PropellerCodec::default();
        Box::pin(future::ok(Framed::new(socket, codec)))
    }
}

/// Simple binary codec for Propeller messages.
#[derive(Debug, Default)]
pub struct PropellerCodec {}

impl Encoder for PropellerCodec {
    type Item<'a> = PropellerMessage;
    type Error = io::Error;

    fn encode(&mut self, item: Self::Item<'_>, dst: &mut BytesMut) -> Result<(), Self::Error> {
        item.encode(dst);
        Ok(())
    }
}

impl Decoder for PropellerCodec {
    type Item = PropellerMessage;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        trace!(
            "🔍 CODEC: decode() called with buffer size: {} bytes",
            src.len()
        );
        let result = PropellerMessage::decode(src);
        match &result {
            Some(msg) => {
                trace!(
                    "✅ CODEC: Successfully decoded message for shred {}",
                    msg.shred.id.index
                );
            }
            None => {
                trace!(
                    "❌ CODEC: Failed to decode message from buffer of {} bytes",
                    src.len()
                );
                // if !src.is_empty() {
                //     tracing::error!(
                //         "⚠️ CODEC: Buffer not empty after failed decode - {} bytes remaining",
                //         src.len()
                //     );
                // }
            }
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use libp2p_core::PeerId;

    use super::*;
    use crate::message::{Shred, ShredId};

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = PropellerCodec::default();
        let mut buffer = BytesMut::new();

        let shred = Shred {
            id: ShredId {
                message_id: 100,
                index: 5,
                publisher: PeerId::random(),
            },
            data: vec![1, 2, 3, 4, 5],
            signature: vec![0; 64],
        };

        let original_message = PropellerMessage {
            shred: shred.clone(),
        };

        // Encode
        codec.encode(original_message.clone(), &mut buffer).unwrap();

        // Decode
        let decoded_message = codec.decode(&mut buffer).unwrap().unwrap();

        // Verify
        let orig = &original_message.shred;
        let decoded = &decoded_message.shred;
        assert_eq!(orig.id.message_id, decoded.id.message_id);
        assert_eq!(orig.id.index, decoded.id.index);
        assert_eq!(orig.data, decoded.data);
    }
}
