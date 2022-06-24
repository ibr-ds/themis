use std::fmt::Debug;

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub use authenticator::*;
pub use factory::*;
use themis_config::Config;

use crate::{
    app::Client,
    authentication::{
        dummy::Dummy, ecdsa::EcdsaFactory, ed25519::Ed25519Factory, hmac::HmacFactory,
        rsa::RsaFactory,
    },
    config::Peer,
    net::{Handshake, HandshakeFactory, IdHandshake, Role},
};

mod authenticator;
pub mod dummy;
pub mod ecdsa;
pub mod ed25519;
mod factory;
pub mod hmac;
pub mod rsa;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum Authentication {
    Rsa,
    Ecdsa,
    Ed25519,
    Hmac,
    None,
}

#[derive(Debug, Clone)]
pub enum AuthFactory<T: Debug> {
    Rsa(RsaFactory),
    Ecdsa(EcdsaFactory),
    Hmac(HmacFactory),
    Ed25519(Ed25519Factory),
    None { me: u64 },
    Custom(Arc<dyn Cryptography<T> + Send + Sync>),
}

pub fn create_crypto_clients(config: &Config) -> AuthFactory<Client> {
    let auth: Authentication = config
        .get("authentication.clients")
        .unwrap_or(Authentication::Hmac);
    let me = config.get("id").expect("id");

    match auth {
        Authentication::Hmac => {
            let factory = HmacFactory::new(me, ring::hmac::HMAC_SHA256);
            AuthFactory::Hmac(factory)
        }
        Authentication::None => AuthFactory::None { me },
        _ => panic!("unsupported auth method"),
    }
}

pub fn create_crypto_replicas<T: Debug>(config: &Config) -> AuthFactory<T> {
    let auth: Authentication = config
        .get("authentication.peers")
        .unwrap_or(Authentication::Ed25519);

    let me = config.get("id").expect("id");

    match auth {
        Authentication::Rsa => {
            let peers: Vec<Peer> = config.get("peers").expect("peers");
            let keyfile: String = config
                .get(&format!("peers[{}].private_key", me))
                .expect("keyfile");
            AuthFactory::Rsa(RsaFactory::new(me, keyfile.into(), peers))
        }
        Authentication::Ecdsa => {
            let peers: Vec<Peer> = config.get("peers").expect("peers");
            let keyfile: String = config
                .get(&format!("peers[{}].private_key", me))
                .expect("keyfile");
            AuthFactory::Ecdsa(EcdsaFactory::new(me, keyfile, peers))
        }
        Authentication::Ed25519 => {
            let peers: Vec<Peer> = config.get("peers").expect("peers");
            let keyfile: String = config
                .get(&format!("peers[{}].private_key", me))
                .expect("keyfile");
            AuthFactory::Ed25519(Ed25519Factory::new(me, keyfile, peers))
        }
        Authentication::None => AuthFactory::None { me },
        _ => panic!("unsupported auth method"),
    }
}

impl<T: Debug> HandshakeFactory for AuthFactory<T> {
    fn create_handshake(&self, role: Role) -> Box<dyn Handshake + Send> {
        match self {
            AuthFactory::Rsa(f) => f.create_handshake(role),
            AuthFactory::Ecdsa(f) => f.create_handshake(role),
            AuthFactory::Hmac(f) => f.create_handshake(role),
            AuthFactory::Ed25519(f) => f.create_handshake(role),
            AuthFactory::Custom(f) => f.create_handshake(role),
            &AuthFactory::None { me } => Box::new(IdHandshake::new(me, role)),
        }
    }
}

impl<T: Debug> Cryptography<T> for AuthFactory<T> {
    fn create_signer(&self, material: &Bytes) -> BoxAuthenticator<T> {
        match self {
            AuthFactory::Rsa(f) => f.create_signer(material),
            AuthFactory::Ecdsa(f) => f.create_signer(material),
            AuthFactory::Ed25519(f) => Cryptography::create_signer(f, material),
            AuthFactory::Hmac(f) => f.create_signer(material),
            AuthFactory::Custom(f) => f.create_signer(material),
            AuthFactory::None { .. } => Box::new(Dummy),
        }
    }
}
