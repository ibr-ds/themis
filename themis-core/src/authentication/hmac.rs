use bytes::Bytes;
use ring::hmac::{self, Key};

use crate::{
    authentication::{Authenticator, Cryptography, Error},
    net::{Handshake, HandshakeFactory, Message, Raw, Role, SymmKeyHandshake},
};

use super::BoxAuthenticator;

#[derive(Debug)]
pub struct Hmac {
    key: Key,
}

impl Hmac {
    pub fn new(key: Key) -> Self {
        Self { key }
    }
}

impl<T> Authenticator<T> for Hmac {
    fn broadcast_opt(&self) -> bool {
        false
    }

    fn sign(&mut self, message: &mut Message<Raw<T>>) -> Result<(), Error> {
        let sig = hmac::sign(&self.key, &message.inner.bytes);
        message.signature = Bytes::copy_from_slice(sig.as_ref());
        Ok(())
    }

    fn verify(&mut self, message: &Message<Raw<T>>) -> Result<(), Error> {
        hmac::verify(&self.key, &message.inner.bytes, &message.signature)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct HmacFactory {
    algo: hmac::Algorithm,
    me: u64,
}

impl HmacFactory {
    pub fn new(me: u64, algo: hmac::Algorithm) -> Self {
        Self { me, algo }
    }
}

impl<T> Cryptography<T> for HmacFactory {
    fn create_signer(&self, material: &Bytes) -> BoxAuthenticator<T> {
        Box::new(Hmac {
            key: Key::new(self.algo, material),
        })
    }
}

impl HandshakeFactory for HmacFactory {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send> {
        Box::new(SymmKeyHandshake::new(self.me, role))
    }
}
