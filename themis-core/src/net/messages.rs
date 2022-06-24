use std::{
    fmt::{Debug, Display, Formatter},
    io,
};

use bytes::Bytes;

use rmp_serde::Serializer;
use serde::{Deserialize, Serialize};

use crate::{protocol::Match, Result};
use core::fmt;
use std::cmp::Ordering;

pub const BROADCAST_DEST: u64 = u64::max_value();

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthenticationKind {
    Protocol = 1,
    Framework,
}

impl AuthenticationKind {
    pub fn is_protocol(self) -> bool {
        self == AuthenticationKind::Protocol
    }

    pub fn is_framework(self) -> bool {
        self == AuthenticationKind::Framework
    }
}

/// Tag Trait
/// Intended to be a number or enum, therefore many constraints
pub trait Tag:
    Send + Sync + Copy + Eq + Debug + Serialize + for<'d> Deserialize<'d> + Unpin + 'static
{
    /// Indicates whether a message with this tag value should be signed
    fn requires_signing(&self) -> Option<AuthenticationKind>;
    fn requires_validation(&self) -> Option<AuthenticationKind>;
}

/// Raw message that is not fully deserialized
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Raw<Tag> {
    /// Message Tag
    pub tag: Tag,
    /// The actual message
    pub bytes: Bytes,
}

impl<Tag> Raw<Tag> {
    pub fn new(tag: Tag, bytes: Bytes) -> Self {
        Self { tag, bytes }
    }
}

/// Inner Message that can be put in Message<T> in deserialized form
/// Must define its Tag that will be put in the Raw struct when this is serialized
pub trait NetworkMessage<T: Tag>: Debug + PartialEq + Serialize + for<'d> Deserialize<'d> {
    const TAG: T;

    fn pack(&self) -> Result<Raw<T>> {
        let mut ser = Serializer::new(Vec::new());
        self.serialize(&mut ser)?;
        let raw = Raw {
            tag: Self::TAG,
            bytes: ser.into_inner().into(),
        };
        Ok(raw)
    }

    fn unpack(raw: &Raw<T>) -> Result<Self>
    where
        Self: Sized,
    {
        if Self::TAG == raw.tag {
            let msg: Self =
                rmp_serde::from_slice(&raw.bytes).map_err(|e| crate::Error::Deserialize {
                    reason: format!("unpack {:?}", raw.tag).into(),
                    source: e,
                })?;
            Ok(msg)
        } else {
            let tag_e = TagError {
                expected: Self::TAG,
                got: raw.tag,
            };
            Err(crate::Error::Io {
                source: io::Error::new(io::ErrorKind::InvalidData, tag_e),
                name: "bad tag",
                id: 0,
            })
        }
    }
}

/// Alias for Message<Raw<T>>
pub type RawMessage<T> = Message<Raw<T>>;

/// Lowest-Level message type for the Framework.
/// Includes common fields needed for almost everything
#[derive(Debug, Clone, Eq, Serialize, Deserialize, Hash, Default)]
pub struct Message<I> {
    /// The message's sender
    pub source: u64,
    /// The message's recipient
    pub destination: u64,
    /// Further content that can be deserialized later
    // #[serde(flatten)]
    pub inner: I,
    /// Checksum type field for HMAC/Signing
    /// Calculate over serialized data and therefore ignored by serde
    pub signature: Bytes,
}

impl<T> Display for Message<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<0x{:x}|0x{:x}|{}|{}>",
            self.source,
            self.destination,
            self.inner,
            DisplayBytes(&self.signature)
        )
    }
}

impl<I> Message<I> {
    /// Serialize the inner field to a Bytes object
    /// Returns Message<Bytes> or error.
    pub fn pack<T: Tag>(&self) -> Result<Message<Raw<T>>>
    where
        I: NetworkMessage<T>,
    {
        Ok(Message {
            source: self.source,
            destination: self.destination,
            inner: self.inner.pack()?,
            signature: self.signature.clone(),
        })
    }

    /// Get message in serialized form. Needed for verification of inner messages
    pub fn raw<T: Tag>(&self) -> Bytes
    where
        I: NetworkMessage<T>,
    {
        let raw = self.inner.pack().expect("failed to serialize message");
        raw.bytes
    }
}

impl<T> Message<T> {
    /// Creates a new message.
    pub fn new(source: u64, destination: u64, inner: T) -> Self {
        Self {
            source,
            destination,
            inner,
            signature: Bytes::new(),
        }
    }

    /// Creates a new broadcast message. source == destination is used to indicate Broadcasts
    #[inline(always)]
    pub fn broadcast(source: u64, inner: T) -> Self {
        Self::new(source, u64::max_value(), inner)
    }

    pub fn make_broadcast(&mut self) {
        self.destination = u64::max_value();
    }

    /// Check if broadcast message
    pub fn is_broadcast(&self) -> bool {
        self.destination == u64::max_value()
    }

    /// Retrieves the checksum of this Message<Bytes>
    pub fn signature(&self) -> &Bytes {
        &self.signature
    }
}

impl<T: Tag> Message<Raw<T>> {
    /// Deserializer Inner Bytes object to type M,
    /// Returns Message<M> or deserialization error.
    pub fn unpack<M>(&self) -> Result<Message<M>>
    where
        M: NetworkMessage<T>,
    {
        Ok(Message {
            source: self.source,
            destination: self.destination,
            inner: M::unpack(&self.inner)?,
            signature: self.signature.clone(),
        })
    }

    pub fn tag(&self) -> T {
        self.inner.tag
    }
}

/// Ignore checksum for PartialEq
impl<T: PartialEq> PartialEq for Message<T> {
    fn eq(&self, other: &Message<T>) -> bool {
        self.source == other.source
            && self.destination == other.destination
            && self.inner == other.inner
    }
}

impl<T, U> Match<Message<U>> for Message<T>
where
    T: PartialEq<U> + Debug + PartialEq,
{
    fn matches(&self, other: &Message<U>) -> bool {
        self.inner == other.inner
    }
}

/// Convenience trait for messages that contain a sequence number of some kind
/// e.g. counters, timestamps
pub trait Sequenced {
    /// returns the message's sequence number
    fn sequence(&self) -> u64;
}

impl<T: Sequenced> Sequenced for Message<T> {
    fn sequence(&self) -> u64 {
        self.inner.sequence()
    }
}

impl<T: Sequenced + PartialEq> PartialOrd for Message<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.sequence().partial_cmp(&other.sequence())
    }
}

#[derive(Debug)]
struct TagError<T: Tag> {
    expected: T,
    got: T,
}

impl<T: Tag> Display for TagError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Expected {:?}, got {:?}", self.expected, self.got)
    }
}

impl<T: Tag> std::error::Error for TagError<T> {}

const PRINT_CAP: usize = 4;
#[derive(Ord, PartialOrd, Eq, PartialEq)]
pub struct DisplayBytes<'a, T = Bytes>(pub &'a T);

impl<'a, T> Debug for DisplayBytes<'a, T>
where
    T: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}

impl<'a, T> Display for DisplayBytes<'a, T>
where
    T: AsRef<[u8]>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if !self.0.as_ref().is_empty() {
            for c in &self.0.as_ref()[..PRINT_CAP] {
                write!(f, "{:02x}", c)?;
            }
        } else {
            write!(f, "()")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::{
        error::Error,
        net::{Ipv4Addr, SocketAddrV4},
    };

    use bytes::{Bytes, BytesMut};
    use futures_util::{sink::SinkExt, stream::StreamExt};
    use ring::{
        hmac::{Key, HMAC_SHA256},
        rand::SystemRandom,
    };
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::{Framed, LengthDelimitedCodec};

    use super::Raw;
    use crate::net::{codec::MessageCodec, messages::Message, AuthenticationKind, Tag};
    use rand::random;

    impl Tag for i32 {
        fn requires_signing(&self) -> Option<AuthenticationKind> {
            Some(AuthenticationKind::Framework)
        }
        fn requires_validation(&self) -> Option<AuthenticationKind> {
            Some(AuthenticationKind::Framework)
        }
    }

    const PAYLOAD: &[u8] = &[1, 2, 3, 4, 5];

    async fn get_socket_pair() -> (TcpStream, TcpStream) {
        let loopback = Ipv4Addr::new(127, 0, 0, 1);
        // Assigning port 0 requests the OS to assign a free port
        let socket = SocketAddrV4::new(loopback, 0);
        let socket: SocketAddr = socket.into();
        let listener = TcpListener::bind(&socket).await.unwrap();
        let port = listener.local_addr().unwrap();
        let clientf = TcpStream::connect(&port);

        let client = clientf.await.unwrap();
        let server = listener.accept().await.unwrap();
        (server.0, client)
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_socket_pair() -> Result<(), Box<dyn Error>> {
        let pair = get_socket_pair().await;
        assert_eq!(pair.0.local_addr()?, pair.1.peer_addr()?);
        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_framed() -> Result<(), Box<dyn Error>> {
        let sockets = get_socket_pair().await;
        let mut fout = Framed::new(sockets.0, LengthDelimitedCodec::default());
        let mut fin = Framed::new(sockets.1, LengthDelimitedCodec::default());

        fout.send(BytesMut::from(PAYLOAD).freeze()).await?;

        let frame = fin.next().await.unwrap()?;

        assert_eq!(frame.as_ref(), PAYLOAD);

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_sink_stream() -> Result<(), Box<dyn Error>> {
        let sockets = get_socket_pair().await;
        let mut fout = Framed::new(sockets.0, MessageCodec::default());
        let mut fin = Framed::new(sockets.1, MessageCodec::default());
        let uuid = random();

        let msg = Message::new(uuid, uuid, Raw::new(0, PAYLOAD.into()));
        fout.send(msg.clone()).await?;

        let message = fin.next().await.unwrap()?;

        assert_eq!(message, msg);

        Ok(())
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_sink_stream_many() -> Result<(), Box<dyn Error>> {
        let (sock_in, sock_out) = get_socket_pair().await;
        let mut fout = Framed::new(sock_in, MessageCodec::default());
        let mut fin = Framed::new(sock_out, MessageCodec::default());
        let mut msgs = Vec::new();

        for i in 0..10 {
            let msg = Message::new(0, i, Raw::new(0, PAYLOAD.into()));
            fout.send(msg.clone()).await?;
            msgs.push(msg);
        }

        for msg in msgs {
            let message = fin.next().await.unwrap()?;
            assert_eq!(message, msg);
        }

        Ok(())
    }

    use crate::authentication::hmac::Hmac;
    use std::net::SocketAddr;

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_hmac() -> Result<(), Box<dyn Error>> {
        let rng = SystemRandom::new();
        let key = Key::generate(HMAC_SHA256, &rng).map_err(|_| "could not generate key")?;
        let uuid = random();

        let ctx = Hmac::new(key.clone());
        let ctx2 = Hmac::new(key);

        let sockets = get_socket_pair().await;
        let mut fout = Framed::new(sockets.0, MessageCodec::new(Box::new(ctx)));
        let mut fin = Framed::new(sockets.1, MessageCodec::new(Box::new(ctx2)));

        let msg = Message::new(uuid, uuid, Raw::new(0, PAYLOAD.into()));
        fout.send(msg.clone()).await?;

        let message = fin.next().await.unwrap()?;

        assert_eq!(message, msg);
        assert_ne!(Bytes::new(), message.signature);

        Ok(())
    }
}
