use std::{fs::read, io, path::Path, sync::Arc};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use ring::signature::{Ed25519KeyPair, KeyPair, Signature, UnparsedPublicKey, ED25519};

use lazy_static::lazy_static;

use crate::{
    authentication::{Authenticator, Cryptography, Error},
    config::Peer,
    net::{Handshake, HandshakeFactory, IdHandshake, Message, Raw, Role},
};

use super::BoxAuthenticator;

lazy_static! {
    static ref EDDSA_KEYS: ArcSwap<FnvHashMap<u64, Bytes>> =
        ArcSwap::new(Arc::new(FnvHashMap::default()));
}

fn load_keys(peers: &[Peer]) -> io::Result<()> {
    let mut key_map = FnvHashMap::default();
    for peer in peers {
        let key = read(&peer.public_key)?;
        key_map.insert(peer.id as u64, key.into());
    }
    EDDSA_KEYS.store(Arc::new(key_map));
    Ok(())
}

pub fn set_keys(peers: FnvHashMap<u64, Bytes>) {
    EDDSA_KEYS.store(Arc::new(peers))
}

#[derive(Debug, Clone)]
pub struct Ed25519Auth {
    private_key: Arc<Ed25519KeyPair>,
}

impl Ed25519Auth {
    pub fn new(keyfile: &Path) -> io::Result<Self> {
        let key = read(keyfile)?;
        let private_key = Ed25519KeyPair::from_pkcs8(&key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into();
        Ok(Self { private_key })
    }

    pub fn from_bytes(key: &[u8]) -> io::Result<Self> {
        let private_key = Ed25519KeyPair::from_pkcs8(key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into();
        Ok(Self { private_key })
    }

    pub fn sign_bytes(&self, message: &[u8]) -> Signature {
        self.private_key.sign(message)
    }

    pub fn verify_bytes(&self, source: u64, message: &[u8], signature: &[u8]) -> Result<(), Error> {
        let key_map = EDDSA_KEYS.load();
        let pub_key = key_map.get(&source).ok_or(Error::KeyNotFound(source))?;
        let public_key = UnparsedPublicKey::new(&ED25519, pub_key);

        match public_key.verify(message, signature) {
            Err(_) => Err(Error::Crypto),
            Ok(_) => Ok(()),
        }
    }

    pub fn verify_own(&self, message: &[u8], signature: &[u8]) -> Result<(), Error> {
        let pk = self.private_key.public_key();
        let pk = UnparsedPublicKey::new(&ED25519, pk);
        match pk.verify(message, signature) {
            Err(_) => Err(Error::Crypto),
            Ok(_) => Ok(()),
        }
    }
}

impl<T> Authenticator<T> for Ed25519Auth {
    fn broadcast_opt(&self) -> bool {
        true
    }

    fn sign(&mut self, message: &mut Message<Raw<T>>) -> Result<(), Error> {
        let mut sig = BytesMut::new();
        let signature = self.private_key.sign(&message.inner.bytes);
        sig.extend_from_slice(signature.as_ref());
        message.signature = sig.freeze();
        Ok(())
    }

    fn verify(&mut self, message: &Message<Raw<T>>) -> Result<(), Error> {
        let key_map = EDDSA_KEYS.load();
        let pub_key = key_map
            .get(&message.source)
            .ok_or(Error::KeyNotFound(message.source))?;
        let public_key = UnparsedPublicKey::new(&ED25519, pub_key);

        public_key.verify(&message.inner.bytes, &message.signature)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Ed25519Factory {
    me: u64,
    keyfile: String,
    peers: Vec<Peer>,
}

impl Ed25519Factory {
    pub fn new(me: u64, keyfile: String, peers: Vec<Peer>) -> Self {
        Self { me, keyfile, peers }
    }

    pub fn create_signer(&self) -> Ed25519Auth {
        if EDDSA_KEYS.load().is_empty() {
            load_keys(&self.peers).expect("Unable to load Public Keys");
        }
        Ed25519Auth::new(self.keyfile.as_ref()).expect("Invalid Private Key")
    }
}

impl<T> Cryptography<T> for Ed25519Factory {
    fn create_signer(&self, _material: &Bytes) -> BoxAuthenticator<T> {
        if EDDSA_KEYS.load().is_empty() {
            load_keys(&self.peers).expect("Unable to load Public Keys");
        }
        Box::new(Ed25519Auth::new(self.keyfile.as_ref()).expect("Invalid Private Key"))
    }
}

impl HandshakeFactory for Ed25519Factory {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send> {
        let my_id = self.me;
        Box::new(IdHandshake::new(my_id, role))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use bytes::Bytes;
    use fnv::FnvHashMap;
    use ring::signature::{Ed25519KeyPair, KeyPair};

    use crate::{
        app::Request,
        authentication::{ed25519::Ed25519Auth, Authenticator},
        net::Message,
    };

    use super::EDDSA_KEYS;
    use ring::rand::SystemRandom;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn ed25519() {
        let key_pair = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let mut ecdsa = Ed25519Auth::from_bytes(key_pair.as_ref()).unwrap();

        let pub_key = ecdsa.private_key.public_key().as_ref();
        let mut map = FnvHashMap::default();
        map.insert(0, Bytes::copy_from_slice(pub_key));
        EDDSA_KEYS.store(Arc::new(map));

        let msg = Message::broadcast(0, Request::new(0, Bytes::from_static(b"hello")));
        let mut packed = msg.pack().expect("pack");

        ecdsa.sign(&mut packed).expect("sign");

        assert!(!packed.signature.is_empty());

        ecdsa.verify(&packed).expect("verify");
    }
}
