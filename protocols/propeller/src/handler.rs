//! Propeller connection handler using request-response pattern for reliable message transmission.

use std::{
    collections::VecDeque,
    task::{Context, Poll, Waker},
};

use asynchronous_codec::Framed;
use futures::{SinkExt, StreamExt};
use libp2p_swarm::{
    handler::{
        ConnectionEvent, ConnectionHandler, ConnectionHandlerEvent, DialUpgradeError,
        FullyNegotiatedInbound, FullyNegotiatedOutbound, ListenUpgradeError, SubstreamProtocol,
    },
    Stream, StreamProtocol,
};
use tracing::{trace, warn};

use crate::{
    message::PropellerMessage,
    protocol::{PropellerCodec, PropellerProtocol},
};

/// Events that can be sent to the handler from the behaviour.
#[derive(Debug, Clone)]
pub enum HandlerIn {
    /// Send a message to the peer.
    SendMessage(PropellerMessage),
}

/// Events that can be received from the handler by the behaviour.
#[derive(Debug, Clone)]
pub enum HandlerOut {
    /// A message was received from the peer.
    Message(PropellerMessage),
    /// Failed to send a message.
    SendError(String),
}

/// The maximum number of inbound or outbound substream attempts we allow.
const MAX_SUBSTREAM_ATTEMPTS: usize = 1000; // Increased for TCP transport reliability

/// State of an inbound substream (receiving requests).
enum InboundSubstreamState {
    /// Waiting for a message from the remote.
    WaitingInput(Framed<Stream, PropellerCodec>),
    /// The substream is being closed.
    Closing(Framed<Stream, PropellerCodec>),
}

/// State of an outbound substream (sending requests).
enum OutboundSubstreamState {
    /// Sending a message to the remote.
    SendingMessage(Framed<Stream, PropellerCodec>, PropellerMessage),
    /// Flushing the message to ensure it's sent.
    FlushingMessage(Framed<Stream, PropellerCodec>),
    /// The substream is being closed.
    Closing(Framed<Stream, PropellerCodec>),
}

/// Propeller connection handler using request-response pattern.
///
/// Unlike the previous long-lived stream approach, this handler creates a new
/// substream for each message, following the request-response pattern. This
/// eliminates the bidirectional connection sharing issues we encountered.
pub struct Handler {
    /// Protocol used for stream negotiation.
    protocol: StreamProtocol,

    /// Queue of messages to be sent.
    send_queue: VecDeque<PropellerMessage>,

    /// Currently active outbound substreams.
    outbound_substreams: Vec<OutboundSubstreamState>,

    /// Currently active inbound substreams.
    inbound_substreams: Vec<InboundSubstreamState>,

    /// Number of outbound substream attempts.
    outbound_substream_attempts: usize,

    /// Number of inbound substream attempts.
    inbound_substream_attempts: usize,

    /// Events to be returned to the behaviour.
    events: VecDeque<HandlerOut>,

    /// Stored waker to notify when events are ready.
    waker: Option<Waker>,
}

impl Handler {
    /// Create a new handler.
    pub fn new(protocol: StreamProtocol) -> Self {
        Self {
            protocol,
            send_queue: VecDeque::new(),
            outbound_substreams: Vec::new(),
            inbound_substreams: Vec::new(),
            outbound_substream_attempts: 0,
            inbound_substream_attempts: 0,
            events: VecDeque::new(),
            waker: None,
        }
    }

    /// Handle a fully negotiated inbound substream.
    fn on_fully_negotiated_inbound(&mut self, substream: Framed<Stream, PropellerCodec>) {
        trace!("📥 HANDLER: New inbound substream established, total inbound: {}", 
                      self.inbound_substreams.len() + 1);
        self.inbound_substreams
            .push(InboundSubstreamState::WaitingInput(substream));
    }

    /// Handle a fully negotiated outbound substream.
    fn on_fully_negotiated_outbound(&mut self, substream: Framed<Stream, PropellerCodec>) {
        trace!("🔗 HANDLER: New outbound substream established, queue_size: {}", self.send_queue.len());
        if let Some(message) = self.send_queue.pop_front() {
            trace!("📤 HANDLER: Assigning shred {} to new substream (queue: {} -> {})", 
                          message.shred.id.index, self.send_queue.len() + 1, self.send_queue.len());
            self.outbound_substreams
                .push(OutboundSubstreamState::SendingMessage(substream, message));
        } else {
            warn!("⚠️ HANDLER: No messages in queue, closing unused substream");
            self.outbound_substreams
                .push(OutboundSubstreamState::Closing(substream));
        }
    }

    fn emit_event(&mut self, event: HandlerOut) {
        self.events.push_back(event);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    fn pending_outbound_requests(&self) -> usize {
        // Estimate pending requests: attempts made but not yet active substreams
        self.outbound_substream_attempts
            .saturating_sub(self.outbound_substreams.len())
    }

    fn poll_outbound_substreams(&mut self, cx: &mut Context<'_>) {
        let initial_count = self.outbound_substreams.len();
        let mut i = 0;
        let mut removed_count = 0;
        
        while i < self.outbound_substreams.len() {
            let state = self.outbound_substreams.swap_remove(i);
            match self.process_outbound_state(cx, state) {
                SubstreamAction::Continue(new_state) => {
                    self.outbound_substreams.insert(i, new_state);
                    i += 1;
                }
                SubstreamAction::Remove => {
                    removed_count += 1;
                    trace!("🗑️ HANDLER: Outbound substream removed (total removed: {})", removed_count);
                }
            }
        }
        
        if removed_count > 0 {
            trace!("📊 HANDLER: Outbound substreams: {} -> {} (removed: {})", 
                          initial_count, self.outbound_substreams.len(), removed_count);
        }
    }

    fn poll_inbound_substreams(&mut self, cx: &mut Context<'_>) {
        let initial_count = self.inbound_substreams.len();
        let mut i = 0;
        let mut removed_count = 0;
        
        if initial_count > 0 {
            trace!("🔄 HANDLER: Polling {} inbound substreams", initial_count);
        }
        
        while i < self.inbound_substreams.len() {
            let state = self.inbound_substreams.swap_remove(i);
            match self.process_inbound_state(cx, state) {
                SubstreamAction::Continue(new_state) => {
                    self.inbound_substreams.insert(i, new_state);
                    i += 1;
                }
                SubstreamAction::Remove => {
                    removed_count += 1;
                    trace!("🗑️ HANDLER: Inbound substream removed");
                }
            }
        }
        
        if removed_count > 0 {
            trace!("📊 HANDLER: Inbound substreams: {} -> {} (removed: {})", 
                          initial_count, self.inbound_substreams.len(), removed_count);
        }
    }

    fn process_outbound_state(
        &mut self,
        cx: &mut Context<'_>,
        state: OutboundSubstreamState,
    ) -> SubstreamAction<OutboundSubstreamState> {
        match state {
            OutboundSubstreamState::SendingMessage(framed, message) => {
                self.poll_send_message(cx, framed, message)
            }
            OutboundSubstreamState::FlushingMessage(framed) => self.poll_flush_message(cx, framed),
            OutboundSubstreamState::Closing(framed) => self.poll_close_outbound(cx, framed),
        }
    }

    fn process_inbound_state(
        &mut self,
        cx: &mut Context<'_>,
        state: InboundSubstreamState,
    ) -> SubstreamAction<InboundSubstreamState> {
        match state {
            InboundSubstreamState::WaitingInput(framed) => self.poll_receive_message(cx, framed),
            InboundSubstreamState::Closing(framed) => self.poll_close_inbound(cx, framed),
        }
    }

    fn poll_send_message(
        &mut self,
        cx: &mut Context<'_>,
        mut framed: Framed<Stream, PropellerCodec>,
        message: PropellerMessage,
    ) -> SubstreamAction<OutboundSubstreamState> {
        trace!("🚀 HANDLER: Attempting to send shred {} (size: {} bytes)", 
                      message.shred.id.index, message.shred.data.len());
        match framed.poll_ready_unpin(cx) {
            Poll::Ready(Ok(())) => match framed.start_send_unpin(message.clone()) {
                Ok(()) => {
                    trace!("✅ HANDLER: Shred {} queued successfully, moving to flush", 
                                  message.shred.id.index);
                    SubstreamAction::Continue(OutboundSubstreamState::FlushingMessage(framed))
                }
                Err(e) => {
                    self.emit_event(HandlerOut::SendError(format!("Send failed: {}", e)));
                    SubstreamAction::Remove
                }
            },
            Poll::Ready(Err(e)) => {
                self.emit_event(HandlerOut::SendError(format!("Stream error: {}", e)));
                SubstreamAction::Remove
            }
            Poll::Pending => {
                trace!("⏸️ HANDLER: Send pending for shred {}, will retry", 
                               message.shred.id.index);
                SubstreamAction::Continue(OutboundSubstreamState::SendingMessage(framed, message))
            }
        }
    }

    fn poll_flush_message(
        &mut self,
        cx: &mut Context<'_>,
        mut framed: Framed<Stream, PropellerCodec>,
    ) -> SubstreamAction<OutboundSubstreamState> {
        match framed.poll_flush_unpin(cx) {
            Poll::Ready(Ok(())) => {
                trace!("✅ HANDLER: Message flushed successfully, closing stream");
                SubstreamAction::Continue(OutboundSubstreamState::Closing(framed))
            }
            Poll::Ready(Err(e)) => {
                self.emit_event(HandlerOut::SendError(format!("Flush failed: {}", e)));
                SubstreamAction::Remove
            }
            Poll::Pending => {
                trace!("⏸️ HANDLER: Flush pending, will retry");
                SubstreamAction::Continue(OutboundSubstreamState::FlushingMessage(framed))
            }
        }
    }

    fn poll_close_outbound(
        &mut self,
        cx: &mut Context<'_>,
        mut framed: Framed<Stream, PropellerCodec>,
    ) -> SubstreamAction<OutboundSubstreamState> {
        match framed.poll_close_unpin(cx) {
            Poll::Ready(_) => {
                trace!("Outbound substream closed");
                if self.outbound_substream_attempts > 0 {
                    self.outbound_substream_attempts -= 1;
                }
                SubstreamAction::Remove
            }
            Poll::Pending => SubstreamAction::Continue(OutboundSubstreamState::Closing(framed)),
        }
    }

    fn poll_receive_message(
        &mut self,
        cx: &mut Context<'_>,
        mut framed: Framed<Stream, PropellerCodec>,
    ) -> SubstreamAction<InboundSubstreamState> {
        trace!("🔍 HANDLER: Polling inbound stream for message");
        match framed.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(message))) => {
                trace!("📨 HANDLER: Received shred {} (size: {} bytes)", 
                              message.shred.id.index, message.shred.data.len());
                self.emit_event(HandlerOut::Message(message));
                SubstreamAction::Continue(InboundSubstreamState::Closing(framed))
            }
            Poll::Ready(Some(Err(e))) => {
                self.emit_event(HandlerOut::SendError(format!("Failed to read from inbound stream: {}", e)));
                SubstreamAction::Remove
            }
            Poll::Ready(None) => {
                trace!("🔚 HANDLER: Inbound stream closed by remote - NO MESSAGE RECEIVED");
                SubstreamAction::Remove
            }
            Poll::Pending => {
                trace!("⏸️ HANDLER: Receive pending, continuing to wait");
                SubstreamAction::Continue(InboundSubstreamState::WaitingInput(framed))
            }
        }
    }

    fn poll_close_inbound(
        &mut self,
        cx: &mut Context<'_>,
        mut framed: Framed<Stream, PropellerCodec>,
    ) -> SubstreamAction<InboundSubstreamState> {
        match framed.poll_close_unpin(cx) {
            Poll::Ready(_) => {
                trace!("Inbound substream closed");
                SubstreamAction::Remove
            }
            Poll::Pending => SubstreamAction::Continue(InboundSubstreamState::Closing(framed)),
        }
    }
}

enum SubstreamAction<S> {
    Continue(S),
    Remove,
}

impl ConnectionHandler for Handler {
    type FromBehaviour = HandlerIn;
    type ToBehaviour = HandlerOut;
    type InboundProtocol = PropellerProtocol;
    type OutboundProtocol = PropellerProtocol;
    type InboundOpenInfo = ();
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol, Self::InboundOpenInfo> {
        SubstreamProtocol::new(PropellerProtocol::new(self.protocol.clone()), ())
    }

    fn poll(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<
        ConnectionHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::ToBehaviour>,
    > {
        // Store the waker for later use
        self.waker = Some(cx.waker().clone());

        // Return any pending events first
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        // Process outbound substreams
        self.poll_outbound_substreams(cx);

        // Process inbound substreams
        self.poll_inbound_substreams(cx);

        // Request new outbound substreams if we have messages to send
        // Be more aggressive about requesting multiple substreams to avoid serialization
        let messages_waiting = self.send_queue.len();
        let active_substreams = self.outbound_substreams.len();
        let can_request_more = self.outbound_substream_attempts < MAX_SUBSTREAM_ATTEMPTS
            && active_substreams < MAX_SUBSTREAM_ATTEMPTS;

        if messages_waiting > 0 && can_request_more {
            // Calculate how many substreams we should have active for current queue
            let desired_substreams = messages_waiting.clamp(1, 32); // Cap at 32 concurrent
            let pending_requests = self.pending_outbound_requests();
            let total_substreams_needed = active_substreams + pending_requests;
            
            if desired_substreams > total_substreams_needed {
                trace!(
                    "🚀 HANDLER: Requesting substream: queue={}, active={}, pending={}, attempts={}, desired={}",
                    messages_waiting, active_substreams, pending_requests, 
                    self.outbound_substream_attempts, desired_substreams
                );
                self.outbound_substream_attempts += 1;
                return Poll::Ready(ConnectionHandlerEvent::OutboundSubstreamRequest {
                    protocol: SubstreamProtocol::new(
                        PropellerProtocol::new(self.protocol.clone()),
                        (),
                    ),
                });
            } else if messages_waiting > 0 {
                trace!(
                    "⏸️ HANDLER: Not requesting substream: queue={}, active={}, pending={}, attempts={}, desired={}",
                    messages_waiting, active_substreams, pending_requests,
                    self.outbound_substream_attempts, desired_substreams
                );
            }
        } else if messages_waiting > 0 {
            warn!(
                "⚠️ HANDLER: Messages waiting but can't request more substreams: queue={}, attempts={}, active={}",
                messages_waiting, self.outbound_substream_attempts, active_substreams
            );
        }

        // Return any events that were generated during processing
        if let Some(event) = self.events.pop_front() {
            return Poll::Ready(ConnectionHandlerEvent::NotifyBehaviour(event));
        }

        Poll::Pending
    }

    fn on_behaviour_event(&mut self, event: Self::FromBehaviour) {
        match event {
            HandlerIn::SendMessage(message) => {
                trace!(
                    "📥 HANDLER: Queuing shred {} (size: {} bytes), queue_size: {} -> {}",
                    message.shred.id.index,
                    message.shred.data.len(),
                    self.send_queue.len(),
                    self.send_queue.len() + 1
                );
                self.send_queue.push_back(message);
                
                // Wake the handler to process the new message
                if let Some(waker) = &self.waker {
                    waker.wake_by_ref();
                }
            }
        }
    }

    fn on_connection_event(
        &mut self,
        event: ConnectionEvent<
            Self::InboundProtocol,
            Self::OutboundProtocol,
            Self::InboundOpenInfo,
            Self::OutboundOpenInfo,
        >,
    ) {
        match event {
            ConnectionEvent::FullyNegotiatedInbound(FullyNegotiatedInbound {
                protocol, ..
            }) => {
                self.inbound_substream_attempts += 1;
                self.on_fully_negotiated_inbound(protocol);
            }
            ConnectionEvent::FullyNegotiatedOutbound(FullyNegotiatedOutbound {
                protocol, ..
            }) => {
                self.on_fully_negotiated_outbound(protocol);
            }
            ConnectionEvent::DialUpgradeError(DialUpgradeError { error, .. }) => {
                // self.emit_event(HandlerOut::SendError(format!("Dial failed: {}", error)));
                trace!("Dial failed: {}", error);

                // CRITICAL: Decrement the attempt counter so we can try again
                if self.outbound_substream_attempts > 0 {
                    self.outbound_substream_attempts -= 1;
                }
            }
            ConnectionEvent::ListenUpgradeError(ListenUpgradeError { error: _, .. }) => {}
            _ => {}
        }
    }
}
