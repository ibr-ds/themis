use bytes::{Buf, Bytes, BytesMut};
use futures_util::{sink::SinkExt, stream::StreamExt, Sink, Stream};
use ring::{
    agreement::{self, agree_ephemeral, UnparsedPublicKey},
    rand::SystemRandom,
};
use std::{future::Future, io, io::ErrorKind::ConnectionAborted, pin::Pin};
use tracing::{debug, trace};

use crate::error::Error;

pub trait FramedStream:
    Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin + Send + Sync
{
}

impl<T> FramedStream for T where
    T: Stream<Item = io::Result<BytesMut>> + Sink<Bytes, Error = io::Error> + Unpin + Send + Sync
{
}

pub trait HandshakeFactory {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send>;
}

pub trait Handshake {
    fn execute<'i>(self: Box<Self>, io: &'i mut dyn FramedStream) -> HandshakeFuture<'i>;
}

pub type HandshakeFuture<'a> =
    Pin<Box<dyn Future<Output = crate::Result<HandshakeResult>> + Send + Sync + 'a>>;

// impl<F> Handshake for F
// where
//     F: for<'a> FnMut(&'a mut dyn FramedStream) -> HandshakeFuture<'a>,
// {
//     fn execute<'a>(&mut self, io: &'a mut dyn FramedStream) -> HandshakeFuture<'a> {
//         self(io)
//     }
// }

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum Role {
    Connector,
    Acceptor,
}

#[derive(Debug)]
pub struct HandshakeResult {
    pub peer_id: u64,
    pub key: Bytes,
}

impl HandshakeResult {
    pub fn new(peer_id: u64, key: Bytes) -> Self {
        Self { peer_id, key }
    }
}

#[derive(Debug, Copy, Clone)]
pub struct IdHandshake {
    id: u64,
    role: Role,
}

impl IdHandshake {
    pub fn new(id: u64, role: Role) -> Self {
        Self { id, role }
    }
}

impl IdHandshake {
    async fn send_id(&mut self, io: &mut dyn FramedStream) -> io::Result<()> {
        io.send(Bytes::copy_from_slice(&self.id.to_le_bytes()[..]))
            .await
    }

    async fn read_id(&mut self, io: &mut dyn FramedStream) -> io::Result<u64> {
        let mut frame = io.next().await.unwrap_or_else(|| {
            Err(io::Error::new(
                ConnectionAborted,
                "Connection dropped during Handshake.",
            ))
        })?;
        let uuid = frame.get_u64_le();
        Ok(uuid)
    }

    pub async fn id_handshake(
        mut self,
        mut io: &mut dyn FramedStream,
    ) -> crate::Result<HandshakeResult> {
        trace!("Handshake state {:?}", self.role);
        let peer_id = match self.role {
            Role::Acceptor => {
                self.send_id(&mut io).await.map_err(|source| Error::Io {
                    name: "handshake",
                    id: 0_u64,
                    source,
                })?;
                let peer_id = self.read_id(&mut io).await.map_err(|source| Error::Io {
                    name: "handshake",
                    id: 0_u64,
                    source,
                })?;
                trace!("read peer id: {}", peer_id);
                peer_id
            }
            Role::Connector => {
                self.send_id(&mut io).await.map_err(|source| Error::Io {
                    name: "handshake",
                    id: 0_u64,
                    source,
                })?;
                let peer_id = self.read_id(&mut io).await.map_err(|source| Error::Io {
                    name: "handshake",
                    id: 0_u64,
                    source,
                })?;
                trace!("read peer id: {}", peer_id);
                peer_id
            }
        };
        debug!("handshake completed with {}", peer_id);
        Ok(HandshakeResult::new(peer_id, Bytes::new()))
    }
}

impl Handshake for IdHandshake {
    fn execute<'i>(self: Box<Self>, io: &'i mut dyn FramedStream) -> HandshakeFuture<'i> {
        Box::pin(self.id_handshake(io))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SymmKeyHandshake {
    inner: IdHandshake,
}

impl SymmKeyHandshake {
    pub fn new(id: u64, role: Role) -> Self {
        Self {
            inner: IdHandshake::new(id, role),
        }
    }

    pub async fn sym_key_handshake(
        self,
        io: &mut dyn FramedStream,
    ) -> crate::Result<HandshakeResult> {
        let rng = SystemRandom::new();
        let priv_key = agreement::EphemeralPrivateKey::generate(&agreement::X25519, &rng)
            .expect("Could not generate Key for Handshake");
        let my_pub = priv_key
            .compute_public_key()
            .expect("Could not get public key for handshake");

        let res = self.inner.id_handshake(io).await?;

        let peer_id = res.peer_id;
        io.send(Bytes::copy_from_slice(my_pub.as_ref()))
            .await
            .map_err(|source| Error::Io {
                name: "handshake",
                id: peer_id,
                source,
            })?;
        let peer_key = io
            .next()
            .await
            .unwrap_or_else(|| Err(io::Error::from(ConnectionAborted)))
            .map_err(|source| Error::Io {
                name: "handshake",
                id: peer_id,
                source,
            })?;

        let peer_pub = UnparsedPublicKey::new(&agreement::X25519, peer_key);
        let key = agree_ephemeral(
            priv_key,
            &peer_pub,
            io::Error::from(ConnectionAborted),
            |key| Ok(Bytes::copy_from_slice(key)),
        )
        .map_err(|source| Error::Io {
            name: "handshake",
            id: peer_id,
            source,
        })?;

        debug!("symmetric key: {:?}", key.as_ref());

        Ok(HandshakeResult::new(res.peer_id, key))
    }
}

impl Handshake for SymmKeyHandshake {
    fn execute<'i>(self: Box<Self>, io: &'i mut dyn FramedStream) -> HandshakeFuture<'i> {
        Box::pin(self.sym_key_handshake(io))
    }
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use tokio::{
        io::duplex,
        net::{TcpListener, TcpStream},
    };
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use super::{IdHandshake, Role};

    async fn make_socket_pair() -> (TcpStream, TcpStream) {
        let server = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).await.unwrap();
        let server_addr = server.local_addr().unwrap();

        let connect = async { TcpStream::connect(server_addr).await.unwrap() };
        let accept = async { server.accept().await.unwrap().0 };
        tokio::join!(connect, accept)
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn id_handshake() {
        let _ = tracing_subscriber::fmt::try_init();

        let (client, server) = duplex(8192);

        let mut client = Framed::new(client, LengthDelimitedCodec::default());
        let mut server = Framed::new(server, LengthDelimitedCodec::default());

        let client_handshake = async {
            let (s, r) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let hs = IdHandshake::new(100, Role::Connector);
                let hs = hs.id_handshake(&mut client).await.unwrap();
                s.send(hs).unwrap();
            });
            r.await.unwrap()
        };
        let server_handshake = async {
            let (s, r) = tokio::sync::oneshot::channel();
            tokio::spawn(async move {
                let hs = IdHandshake::new(1, Role::Acceptor);
                let r = hs.id_handshake(&mut server).await.unwrap();
                s.send(r).unwrap();
            });
            r.await.unwrap()
        };

        let (c, s) = tokio::join!(client_handshake, server_handshake);
        assert_eq!(c.peer_id, 1);
        assert_eq!(s.peer_id, 100);
    }
}
