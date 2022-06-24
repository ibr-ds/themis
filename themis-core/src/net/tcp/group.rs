use std::{future::Future, net::SocketAddr, pin::Pin, sync::Arc, task::Poll};

use bytes::Bytes;
use fnv::FnvHashMap;
use futures_util::{
    select, stream::FuturesUnordered, task::Context, FutureExt, StreamExt, TryFutureExt,
};
use tokio::{sync::Notify, task::JoinHandle};
use tokio_util::codec::{Framed, FramedParts};
use tracing::warn;

use crate::{
    authentication::{Authenticator, AuthenticatorExt, Cryptography},
    comms::{unbounded, Receiver, Sender},
    error::LogErrorExt,
    execute::spawn,
    net::{
        codec::MessageCodec,
        messages::{Message, Raw, Tag},
        tcp::{
            connection::{recv_task, send_task},
            endpoint::connect,
        },
        AuthenticationKind, NewConnection, RawMessage, Role,
    },
    Error, Result,
};

use super::TcpConnection;

#[derive(Debug)]
pub enum Update {
    Connected { id: u64, key: Bytes },
    Disconnected(u64),
}

#[derive(Debug, Clone, Copy)]
struct ConnData {
    role: Role,
    remote_id: u64,
    remote_addr: SocketAddr,
}

struct RecvHandle {
    remote: JoinHandle<()>,
    data: ConnData,
}

impl RecvHandle {
    fn new(remote: JoinHandle<()>, role: Role, remote_id: u64, remote_addr: SocketAddr) -> Self {
        Self {
            remote,
            data: ConnData {
                role,
                remote_id,
                remote_addr,
            },
        }
    }
}

impl Future for RecvHandle {
    type Output = ConnData;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.remote.poll_unpin(ctx).map(|_| self.data)
    }
}

pub struct Bootstrap {
    expected_connections: usize,
    notify: Arc<Notify>,
}

impl Bootstrap {
    pub fn new(expected_connections: usize, notify: Arc<Notify>) -> Self {
        Self {
            expected_connections,
            notify,
        }
    }

    pub fn signal_all(&self) {
        self.notify.notify_waiters();
    }
}

struct GroupState<Auth, T> {
    // connections
    recv_handles: FuturesUnordered<RecvHandle>,
    send_handles: FnvHashMap<u64, Sender<RawMessage<T>>>,

    // crypto
    auth: Box<dyn Authenticator<T> + Send>,
    auth_factory: Auth,
    // bootstrap
    bootstrap: Option<Bootstrap>,
}

pub struct Group<Auth, T> {
    // channels
    connections_tx: Sender<NewConnection<TcpConnection>>,
    connections_rx: Receiver<NewConnection<TcpConnection>>,
    outgoing_rx: Receiver<Message<Raw<T>>>,
    incoming_tx: Sender<Message<Raw<T>>>,

    state: GroupState<Auth, T>,
}

impl<Auth: Cryptography<T> + Clone, T> Group<Auth, T> {
    pub fn conn_sender(&self) -> Sender<NewConnection<TcpConnection>> {
        self.connections_tx.clone()
    }

    pub fn new(
        outgoing_rx: Receiver<Message<Raw<T>>>,
        incoming_tx: Sender<Message<Raw<T>>>,
        auth_factory: Auth,
        bootstrap: Option<Bootstrap>,
    ) -> Self {
        let (connections_s, connections_r) = unbounded();
        Self {
            connections_tx: connections_s,
            connections_rx: connections_r,
            outgoing_rx,
            incoming_tx,
            state: GroupState {
                recv_handles: FuturesUnordered::default(),
                send_handles: FnvHashMap::default(),
                auth: auth_factory.create_signer(&Bytes::new()),
                auth_factory,
                bootstrap,
            },
        }
    }
}

impl<Auth: Cryptography<T> + Clone + 'static, T: Tag> Group<Auth, T> {
    pub async fn execute(mut self) {
        let mut outgoing_rx = self.outgoing_rx.fuse();
        let mut connections_rx = self.connections_rx.fuse();

        loop {
            select! {
                hs = connections_rx.select_next_some() => {
                    self.state.handle_new_connection(self.incoming_tx.clone(), hs);
                },
                outgoing = outgoing_rx.select_next_some() => {
                    let _ = self.state.handle_send_message(outgoing).await;
                },
                conn_data = self.state.recv_handles.select_next_some() => {
                    self.state.handle_dropped_connection(self.connections_tx.clone(), conn_data);
                },
                complete => {
                    break;
                },
            };
        }
        warn!("Connection group terminated due to channel drop");
    }
}

impl<Auth: Cryptography<T> + Clone + 'static, T: Tag> GroupState<Auth, T> {
    fn handle_dropped_connection(
        &mut self,
        sender: Sender<NewConnection<TcpConnection>>,
        conn: ConnData,
    ) {
        tracing::warn!("Connection to {} disconnected.", conn.remote_id);
        self.send_handles.remove(&conn.remote_id);
        if conn.role == Role::Connector {
            let handshake = self.auth_factory.clone();
            spawn(async move {
                if let Err(e) = connect(conn.remote_addr, sender, handshake).await {
                    tracing::error!("Could not reconnect to {:?}: {}.", conn.remote_addr, e);
                }
            });
        }
    }

    fn handle_new_connection(
        &mut self,
        incoming_tx: Sender<RawMessage<T>>,
        hs: NewConnection<TcpConnection>,
    ) {
        let id = hs.handshake.peer_id;
        if let Err(err) = self.try_handle_new_connection(incoming_tx, hs) {
            tracing::warn!("Connection from {} dropped during setup: {}", id, err)
        }
    }

    fn try_handle_new_connection(
        &mut self,
        incoming_tx: Sender<RawMessage<T>>,
        hs: NewConnection<TcpConnection>,
    ) -> std::io::Result<()> {
        let remote_addr = hs.io.get_ref().peer_addr()?;
        tracing::info!("Connected {} from {}", hs.handshake.peer_id, remote_addr);
        let old_codec = hs.io.into_parts();

        old_codec.io.set_nodelay(true)?;

        let mut message_codec = FramedParts::new(
            old_codec.io,
            MessageCodec::new(self.auth_factory.create_signer(&hs.handshake.key)),
        );
        message_codec.read_buf = old_codec.read_buf;
        message_codec.write_buf = old_codec.write_buf;

        let (sink, stream) = Framed::from_parts(message_codec).split();

        let (send_s, send_r) = unbounded();
        let peer_id = hs.handshake.peer_id;
        let send_task = send_task(send_r, sink)
            .map_err(move |source| Error::Io {
                name: "send task",
                id: peer_id,
                source,
            })
            .log_warn();
        spawn(send_task);
        self.send_handles.insert(hs.handshake.peer_id, send_s);

        let recv_task = spawn(
            recv_task(incoming_tx, stream)
                .map_err(move |source| Error::Io {
                    name: "receive task",
                    id: peer_id,
                    source,
                })
                .log_warn(),
        );
        self.recv_handles.push(RecvHandle::new(
            recv_task,
            hs.role,
            hs.handshake.peer_id,
            remote_addr,
        ));

        if let Some(bootstrap) = &self.bootstrap {
            if self.recv_handles.len() == bootstrap.expected_connections {
                bootstrap.notify.notify_one();
                self.bootstrap.take();
            }
        }
        Ok(())
    }

    async fn handle_send_message(&mut self, mut message: Message<Raw<T>>) -> Result<()> {
        tracing::trace!(
            "sending message type {:?} to {}",
            message.inner.tag,
            message.destination
        );
        if message.is_broadcast() {
            // if supported, we can sign broadcast messages here
            if message.tag().requires_signing() == Some(AuthenticationKind::Framework) {
                self.auth
                    .sign_broadcast(&mut message)
                    .map_err(|e| crate::Error::protocol(e))?;
            }
            let mut disconnected = Vec::new();
            for (id, sender) in self.send_handles.iter_mut() {
                if sender.send(message.clone()).await.is_err() {
                    disconnected.push(*id);
                }
            }
            for dc in disconnected {
                self.send_handles.remove(&dc);
            }
        } else {
            let dest = message.destination;
            let conn = self.send_handles.get_mut(&dest);
            if let Some(conn) = conn {
                if let Err(_e) = conn.send(message).await {
                    self.send_handles.remove(&dest);
                }
            } else {
                tracing::warn!(
                    "Cannot send {:?} message to {}: Does not have a connection.",
                    message.tag(),
                    message.destination
                );
            }
        }
        Ok(())
    }
}
