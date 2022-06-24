use std::fmt::Debug;

use bytes::Bytes;

use crate::{authentication::Authenticator, net::HandshakeFactory};

pub type BoxAuthenticator<T> = Box<dyn Authenticator<T> + Send + Sync + 'static>;

pub trait Cryptography<T>: Debug + Send + Sync + HandshakeFactory {
    fn create_signer(&self, material: &Bytes) -> BoxAuthenticator<T>;
}
