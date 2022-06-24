use std::{fs::read, io, ops::Deref, sync::Arc};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use fnv::FnvHashMap;
use ring::{
    rand::SystemRandom,
    signature::{RsaKeyPair, UnparsedPublicKey, RSA_PKCS1_2048_8192_SHA256, RSA_PKCS1_SHA256},
};

use lazy_static::lazy_static;

use crate::{
    authentication::{Authenticator, Cryptography, Error},
    config::Peer,
    net::{Handshake, HandshakeFactory, IdHandshake, Message, Raw, Role},
};
use std::path::{Path, PathBuf};

lazy_static! {
    static ref RSA_KEYS: ArcSwap<FnvHashMap<u64, Bytes>> =
        ArcSwap::new(Arc::new(FnvHashMap::default()));
    static ref RNG: SystemRandom = SystemRandom::new();
}

fn load_keys(peers: &[Peer]) -> io::Result<()> {
    let mut key_map = FnvHashMap::default();
    for peer in peers {
        let key = read(&peer.public_key)?;
        key_map.insert(peer.id as u64, key.into());
    }
    RSA_KEYS.store(Arc::new(key_map));
    Ok(())
}

#[derive(Debug, Clone)]
pub struct RsaAuth {
    priv_key: Arc<RsaKeyPair>,
}

impl RsaAuth {
    fn new(keyfile: &Path) -> io::Result<Self> {
        let key = read(keyfile)?;
        let priv_key = RsaKeyPair::from_pkcs8(&key)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            .into();
        Ok(Self { priv_key })
    }

    fn from_key(key: RsaKeyPair) -> Self {
        Self {
            priv_key: Arc::new(key),
        }
    }
}

impl<T> Authenticator<T> for RsaAuth {
    fn broadcast_opt(&self) -> bool {
        true
    }

    fn sign(&mut self, message: &mut Message<Raw<T>>) -> Result<(), Error> {
        let mut sig = BytesMut::new();
        sig.resize(self.priv_key.public_modulus_len(), 0);
        self.priv_key.sign(
            &RSA_PKCS1_SHA256,
            RNG.deref(),
            &message.inner.bytes,
            &mut sig,
        )?;
        message.signature = sig.freeze();
        Ok(())
    }

    fn verify(&mut self, message: &Message<Raw<T>>) -> Result<(), Error> {
        let key_map = RSA_KEYS.load();
        let pub_key = key_map
            .get(&message.source)
            .ok_or(Error::KeyNotFound(message.source))?;
        let public_key = UnparsedPublicKey::new(&RSA_PKCS1_2048_8192_SHA256, pub_key);

        public_key.verify(&message.inner.bytes, &message.signature)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RsaFactory {
    me: u64,
    keyfile: PathBuf,
    peers: Vec<Peer>,
}

impl RsaFactory {
    pub fn new(me: u64, keyfile: PathBuf, config: Vec<Peer>) -> Self {
        Self {
            me,
            keyfile,
            peers: config,
        }
    }
}

impl<T> Cryptography<T> for RsaFactory {
    fn create_signer(&self, _material: &Bytes) -> Box<dyn Authenticator<T> + Send + Sync> {
        if RSA_KEYS.load().is_empty() {
            load_keys(&self.peers).expect("Unable to load Public Keys");
        }
        Box::new(RsaAuth::new(&self.keyfile).expect("Invalid Private Key"))
    }
}

impl HandshakeFactory for RsaFactory {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send> {
        let my_id = self.me;
        Box::new(IdHandshake::new(my_id, role))
    }
}

#[cfg(test)]
mod test {
    use crate::{
        app::Request,
        authentication::{Authenticator, AuthenticatorExt},
        net::Message,
    };
    use bytes::Bytes;
    use ring::signature::{KeyPair, RsaKeyPair};

    use super::{RsaAuth, RSA_KEYS};
    use fnv::FnvHashMap;
    use std::sync::Arc;

    fn generate_keypar() -> RsaKeyPair {
        let ring_key = include_bytes!("rsatestkey.bin");
        let ring_key = RsaKeyPair::from_pkcs8(ring_key).unwrap();

        ring_key
    }

    fn test_keys(me: u64) -> RsaKeyPair {
        let mut keymap: FnvHashMap<u64, Bytes> = FnvHashMap::default();
        let my_key = generate_keypar();
        keymap.insert(me, Bytes::copy_from_slice(&my_key.public_key().as_ref()));
        RSA_KEYS.store(Arc::new(keymap));
        my_key
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn run_rsa() {
        let me = test_keys(0);
        let mut rsa = RsaAuth::from_key(me);

        let msg = Message::broadcast(0, Request::new(0, Bytes::from_static(b"hello")));
        let mut packed = msg.pack().expect("pack");

        rsa.sign(&mut packed).expect("sign");

        assert!(!packed.signature.is_empty());

        rsa.verify(&packed).expect("verify");
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn run_rsa_unpacked() {
        let me = test_keys(0);
        let mut rsa = RsaAuth::from_key(me);

        let mut msg = Message::broadcast(0, Request::new(0, Bytes::from_static(b"hello")));

        rsa.sign_unpacked(&mut msg).expect("sign");

        assert!(!msg.signature.is_empty());

        rsa.verify_unpacked(&msg).expect("verify");
    }
}
