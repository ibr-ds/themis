use std::{fs::read, io, ops::Deref, sync::Arc};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use ring::{
    rand::SystemRandom,
    signature::{
        EcdsaKeyPair, UnparsedPublicKey, ECDSA_P256_SHA256_ASN1, ECDSA_P256_SHA256_ASN1_SIGNING,
    },
};

use lazy_static::lazy_static;

use crate::{
    authentication::{Authenticator, Cryptography, Error},
    config::Peer,
    net::{Handshake, HandshakeFactory, IdHandshake, Message, Raw, Role},
};
use std::path::Path;

use super::BoxAuthenticator;

lazy_static! {
    static ref ECDSA_KEYS: ArcSwap<FnvHashMap<u64, Bytes>> =
        ArcSwap::new(Arc::new(FnvHashMap::default()));
    static ref RNG: SystemRandom = SystemRandom::new();
}

fn load_keys(peers: &[Peer]) -> io::Result<()> {
    let mut key_map = FnvHashMap::default();
    for peer in peers {
        let key = read(&peer.public_key)?;
        key_map.insert(peer.id as u64, key.into());
    }
    ECDSA_KEYS.store(Arc::new(key_map));
    Ok(())
}

#[derive(Debug)]
pub struct EcdsaAuthenticator {
    private_key: Arc<EcdsaKeyPair>,
}

impl EcdsaAuthenticator {
    pub fn new(keyfile: &Path) -> io::Result<Self> {
        let key = read(keyfile)?;
        let private_key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, &key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into();
        Ok(Self { private_key })
    }

    fn from_bytes(key: &[u8]) -> io::Result<Self> {
        let private_key = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into();
        Ok(Self { private_key })
    }
}

impl<T> Authenticator<T> for EcdsaAuthenticator {
    fn broadcast_opt(&self) -> bool {
        true
    }

    fn sign(&mut self, message: &mut Message<Raw<T>>) -> Result<(), Error> {
        let mut sig = BytesMut::new();
        let signature = self.private_key.sign(RNG.deref(), &message.inner.bytes)?;
        sig.extend_from_slice(signature.as_ref());
        message.signature = sig.freeze();
        Ok(())
    }

    fn verify(&mut self, message: &Message<Raw<T>>) -> Result<(), Error> {
        let key_map = ECDSA_KEYS.load();
        let pub_key = key_map
            .get(&message.source)
            .ok_or(Error::KeyNotFound(message.source))?;
        let public_key = UnparsedPublicKey::new(&ECDSA_P256_SHA256_ASN1, pub_key);

        public_key.verify(&message.inner.bytes, &message.signature)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct EcdsaFactory {
    me: u64,
    keyfile: String,
    peers: Vec<Peer>,
}

impl EcdsaFactory {
    pub fn new(me: u64, keyfile: String, peers: Vec<Peer>) -> Self {
        Self { me, keyfile, peers }
    }
}

impl<T> Cryptography<T> for EcdsaFactory {
    fn create_signer(&self, _material: &Bytes) -> BoxAuthenticator<T> {
        if ECDSA_KEYS.load().is_empty() {
            load_keys(&self.peers).expect("Unable to load Public Keys");
        }
        Box::new(EcdsaAuthenticator::new(self.keyfile.as_ref()).expect("Invalid Private Key"))
    }
}

impl HandshakeFactory for EcdsaFactory {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send> {
        let my_id = self.me;
        Box::new(IdHandshake::new(my_id, role))
    }
}

#[cfg(test)]
mod test {
    use std::{ops::Deref, sync::Arc};

    use bytes::Bytes;
    use fnv::FnvHashMap;
    use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_ASN1_SIGNING};

    use crate::{
        app::Request,
        authentication::{ecdsa::EcdsaAuthenticator, Authenticator},
        net::Message,
    };

    use super::{ECDSA_KEYS, RNG};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn ecdsa() {
        let key_pair =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_ASN1_SIGNING, RNG.deref()).unwrap();
        let mut ecdsa = EcdsaAuthenticator::from_bytes(key_pair.as_ref()).unwrap();

        let pub_key = ecdsa.private_key.public_key().as_ref();
        let mut map = FnvHashMap::default();
        map.insert(0, Bytes::copy_from_slice(pub_key));
        ECDSA_KEYS.store(Arc::new(map));

        let msg = Message::broadcast(0, Request::new(0, Bytes::from_static(b"hello")));
        let mut packed = msg.pack().expect("pack");

        ecdsa.sign(&mut packed).expect("sign");

        assert!(!packed.signature.is_empty());

        ecdsa.verify(&packed).expect("verify");
    }
}
