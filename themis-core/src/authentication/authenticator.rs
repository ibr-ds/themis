use std::{
    fmt::{self, Debug, Display, Formatter},
    io::{self, ErrorKind::InvalidData},
};

use crate::net::{Message, NetworkMessage, RawMessage};

pub trait Authenticator<T>: Debug {
    /// Decides, ifi this crypto implementation can sign broadcasts
    /// Otherwise each message is signed individually
    fn broadcast_opt(&self) -> bool;

    fn sign(&mut self, message: &mut RawMessage<T>) -> Result<(), Error>;

    fn verify(&mut self, message: &RawMessage<T>) -> Result<(), Error>;
}

impl<Tag> Authenticator<Tag> for Box<dyn Authenticator<Tag>> {
    fn broadcast_opt(&self) -> bool {
        (**self).broadcast_opt()
    }

    fn sign(&mut self, message: &mut RawMessage<Tag>) -> Result<(), Error> {
        (**self).sign(message)
    }

    fn verify(&mut self, message: &RawMessage<Tag>) -> Result<(), Error> {
        (**self).verify(message)
    }
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Crypto,
    KeyNotFound(u64),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Authentication Error: {:?}", self)
    }
}

impl From<ring::error::Unspecified> for Error {
    fn from(_e: ring::error::Unspecified) -> Self {
        Self::Crypto
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl std::error::Error for Error {}

pub trait AuthenticatorExt<Tag>: Authenticator<Tag>
where
    Tag: crate::net::Tag,
{
    fn sign_broadcast(&mut self, message: &mut RawMessage<Tag>) -> Result<(), Error> {
        if self.broadcast_opt() {
            self.sign(message)
        } else {
            Ok(())
        }
    }

    fn sign_message(&mut self, message: &mut RawMessage<Tag>) -> Result<(), Error> {
        // Don't sign broadcasts, if broadcasts are signed with sign_broadcast
        if self.broadcast_opt() && message.is_broadcast() {
            Ok(())
        } else {
            self.sign(message)
        }
    }

    fn sign_unpacked<T>(&mut self, message: &mut Message<T>) -> Result<(), crate::Error>
    where
        T: NetworkMessage<Tag>,
    {
        let mut packed = message.pack()?;
        self.sign(&mut packed)
            .map_err(|e| io::Error::new(InvalidData, e))
            .expect("sign");

        message.signature = packed.signature;

        Ok(())
    }

    fn verify_unpacked<T>(&mut self, message: &Message<T>) -> Result<(), crate::Error>
    where
        T: NetworkMessage<Tag>,
    {
        let packed = message.pack()?;
        self.verify(&packed)
            .map_err(|e| io::Error::new(InvalidData, e))
            .map_err(|e| crate::Error::Io {
                name: "verify message",
                id: message.source,
                source: e,
            })?;

        Ok(())
    }
}

impl<T: ?Sized, Tag> AuthenticatorExt<Tag> for T
where
    T: Authenticator<Tag>,
    Tag: crate::net::Tag,
{
}
