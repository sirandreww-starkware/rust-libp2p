// Copyright 2021 Protocol Labs.
// Copyright 2018 Parity Technologies (UK) Ltd.
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
// OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

//! Async functions driving pending and established connections in the form of a task.

use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

use futures::{
    channel::{mpsc, oneshot},
    future::{poll_fn, Either, Future},
    SinkExt, StreamExt,
};
use libp2p_core::muxing::StreamMuxerBox;

use super::concurrent_dial::ConcurrentDial;
use crate::{
    connection::{
        self, ConnectionError, ConnectionId, PendingInboundConnectionError,
        PendingOutboundConnectionError,
    },
    transport::TransportError,
    ConnectionHandler, Multiaddr, PeerId,
};

/// Commands that can be sent to a task driving an established connection.
#[derive(Debug)]
pub(crate) enum Command<T> {
    /// Notify the connection handler of an event.
    NotifyHandler(T),
    /// Gracefully close the connection (active close) before
    /// terminating the task.
    Close,
}

pub(crate) enum PendingConnectionEvent {
    ConnectionEstablished {
        id: ConnectionId,
        output: (PeerId, StreamMuxerBox),
        /// [`Some`] when the new connection is an outgoing connection.
        /// Addresses are dialed in parallel. Contains the addresses and errors
        /// of dial attempts that failed before the one successful dial.
        outgoing: Option<(Multiaddr, Vec<(Multiaddr, TransportError<std::io::Error>)>)>,
    },
    /// A pending connection failed.
    PendingFailed {
        id: ConnectionId,
        error: Either<PendingOutboundConnectionError, PendingInboundConnectionError>,
    },
}

#[derive(Debug)]
pub(crate) enum EstablishedConnectionEvent<ToBehaviour> {
    /// A node we are connected to has changed its address.
    AddressChange {
        id: ConnectionId,
        peer_id: PeerId,
        new_address: Multiaddr,
    },
    /// Notify the manager of an event from the connection.
    Notify {
        id: ConnectionId,
        peer_id: PeerId,
        event: ToBehaviour,
    },
    /// A connection closed, possibly due to an error.
    ///
    /// If `error` is `None`, the connection has completed
    /// an active orderly close.
    Closed {
        id: ConnectionId,
        peer_id: PeerId,
        error: Option<ConnectionError>,
    },
}

pub(crate) async fn new_for_pending_outgoing_connection(
    connection_id: ConnectionId,
    dial: ConcurrentDial,
    abort_receiver: oneshot::Receiver<Infallible>,
    mut events: mpsc::Sender<PendingConnectionEvent>,
) {
    match futures::future::select(abort_receiver, Box::pin(dial)).await {
        Either::Left((Err(oneshot::Canceled), _)) => {
            let _ = events
                .send(PendingConnectionEvent::PendingFailed {
                    id: connection_id,
                    error: Either::Left(PendingOutboundConnectionError::Aborted),
                })
                .await;
        }
        Either::Left((Ok(v), _)) => libp2p_core::util::unreachable(v),
        Either::Right((Ok((address, output, errors)), _)) => {
            let _ = events
                .send(PendingConnectionEvent::ConnectionEstablished {
                    id: connection_id,
                    output,
                    outgoing: Some((address, errors)),
                })
                .await;
        }
        Either::Right((Err(e), _)) => {
            let _ = events
                .send(PendingConnectionEvent::PendingFailed {
                    id: connection_id,
                    error: Either::Left(PendingOutboundConnectionError::Transport(e)),
                })
                .await;
        }
    }
}

pub(crate) async fn new_for_pending_incoming_connection<TFut>(
    connection_id: ConnectionId,
    future: TFut,
    abort_receiver: oneshot::Receiver<Infallible>,
    mut events: mpsc::Sender<PendingConnectionEvent>,
) where
    TFut: Future<Output = Result<(PeerId, StreamMuxerBox), std::io::Error>> + Send + 'static,
{
    match futures::future::select(abort_receiver, Box::pin(future)).await {
        Either::Left((Err(oneshot::Canceled), _)) => {
            let _ = events
                .send(PendingConnectionEvent::PendingFailed {
                    id: connection_id,
                    error: Either::Right(PendingInboundConnectionError::Aborted),
                })
                .await;
        }
        Either::Left((Ok(v), _)) => libp2p_core::util::unreachable(v),
        Either::Right((Ok(output), _)) => {
            let _ = events
                .send(PendingConnectionEvent::ConnectionEstablished {
                    id: connection_id,
                    output,
                    outgoing: None,
                })
                .await;
        }
        Either::Right((Err(e), _)) => {
            let _ = events
                .send(PendingConnectionEvent::PendingFailed {
                    id: connection_id,
                    error: Either::Right(PendingInboundConnectionError::Transport(
                        TransportError::Other(e),
                    )),
                })
                .await;
        }
    }
}

/// Result of polling the connection loop - determines what action to take next.
enum ConnectionLoopAction<THandler: ConnectionHandler> {
    /// An event needs to be sent to the pool
    SendEvent(EstablishedConnectionEvent<THandler::ToBehaviour>),
    /// The connection should be gracefully closed
    Close,
    /// The connection encountered an error
    Error(ConnectionError),
    /// The manager disappeared (command channel closed)
    ManagerGone,
}

pub(crate) async fn new_for_established_connection<THandler>(
    connection_id: ConnectionId,
    peer_id: PeerId,
    mut connection: crate::connection::Connection<THandler>,
    mut command_receiver: mpsc::Receiver<Command<THandler::FromBehaviour>>,
    mut events: mpsc::Sender<EstablishedConnectionEvent<THandler::ToBehaviour>>,
) where
    THandler: ConnectionHandler,
{
    loop {
        // Use a single poll_fn that polls both the command receiver and connection.
        // This avoids the waker accumulation issue that occurs when using
        // futures::future::select in a loop, where each iteration creates new
        // futures that register additional wakers without cleaning up old ones.
        let action = poll_fn(|cx: &mut Context<'_>| {
            // Poll connection events first - prioritize network I/O over commands
            // from the behavior layer for better responsiveness.
            match Pin::new(&mut connection).poll(cx) {
                Poll::Ready(Ok(connection::Event::Handler(event))) => {
                    return Poll::Ready(ConnectionLoopAction::SendEvent(
                        EstablishedConnectionEvent::Notify {
                            id: connection_id,
                            peer_id,
                            event,
                        },
                    ));
                }
                Poll::Ready(Ok(connection::Event::AddressChange(new_address))) => {
                    return Poll::Ready(ConnectionLoopAction::SendEvent(
                        EstablishedConnectionEvent::AddressChange {
                            id: connection_id,
                            peer_id,
                            new_address,
                        },
                    ));
                }
                Poll::Ready(Err(error)) => {
                    return Poll::Ready(ConnectionLoopAction::<THandler>::Error(error));
                }
                Poll::Pending => {}
            }

            // Then poll commands from the behavior layer
            loop {
                match command_receiver.poll_next_unpin(cx) {
                    Poll::Ready(Some(Command::NotifyHandler(event))) => {
                        connection.on_behaviour_event(event);
                        // Continue polling - there may be more commands
                        continue;
                    }
                    Poll::Ready(Some(Command::Close)) => {
                        return Poll::Ready(ConnectionLoopAction::<THandler>::Close);
                    }
                    Poll::Ready(None) => {
                        return Poll::Ready(ConnectionLoopAction::<THandler>::ManagerGone);
                    }
                    Poll::Pending => break,
                }
            }

            Poll::Pending
        })
        .await;

        match action {
            ConnectionLoopAction::SendEvent(event) => {
                let _ = events.send(event).await;
            }
            ConnectionLoopAction::Close => {
                command_receiver.close();
                let (remaining_events, closing_muxer) = connection.close();

                let _ = events
                    .send_all(&mut remaining_events.map(|event| {
                        Ok(EstablishedConnectionEvent::Notify {
                            id: connection_id,
                            event,
                            peer_id,
                        })
                    }))
                    .await;

                let error = closing_muxer.await.err().map(ConnectionError::IO);

                let _ = events
                    .send(EstablishedConnectionEvent::Closed {
                        id: connection_id,
                        peer_id,
                        error,
                    })
                    .await;
                return;
            }
            ConnectionLoopAction::Error(error) => {
                command_receiver.close();
                let (remaining_events, _closing_muxer) = connection.close();

                let _ = events
                    .send_all(&mut remaining_events.map(|event| {
                        Ok(EstablishedConnectionEvent::Notify {
                            id: connection_id,
                            event,
                            peer_id,
                        })
                    }))
                    .await;

                let _ = events
                    .send(EstablishedConnectionEvent::Closed {
                        id: connection_id,
                        peer_id,
                        error: Some(error),
                    })
                    .await;
                return;
            }
            ConnectionLoopAction::ManagerGone => return,
        }
    }
}
