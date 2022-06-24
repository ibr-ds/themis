mod tcp;

mod codec;
mod handshake;
mod messages;

use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

pub use self::{handshake::*, messages::*, tcp::*};
pub use crate::net::codec::MessageCodec;

pub type Connection<T = TcpStream> = Framed<T, LengthDelimitedCodec>;
pub type TaggedConnection<T> = Framed<TcpStream, MessageCodec<T>>;

#[derive(Debug)]
pub struct NewConnection<T> {
    pub io: T,
    pub role: Role,
    pub handshake: HandshakeResult,
}

impl<T> NewConnection<T> {
    pub fn acceptor(io: T, handshake: HandshakeResult) -> Self {
        Self {
            io,
            role: Role::Acceptor,
            handshake,
        }
    }
    pub fn connector(io: T, handshake: HandshakeResult) -> Self {
        Self {
            io,
            role: Role::Connector,
            handshake,
        }
    }
}
