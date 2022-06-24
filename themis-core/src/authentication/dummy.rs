use crate::{
    authentication::{Authenticator, Error},
    net::{Message, Raw},
};
use bytes::Bytes;

#[derive(Debug)]
pub struct Dummy;

impl<T> Authenticator<T> for Dummy {
    fn broadcast_opt(&self) -> bool {
        true
    }

    fn sign(&mut self, message: &mut Message<Raw<T>>) -> Result<(), Error> {
        message.signature = Bytes::from_static(b"signed");
        Ok(())
    }

    fn verify(&mut self, message: &Message<Raw<T>>) -> Result<(), Error> {
        if message.signature.as_ref() == b"signed" {
            Ok(())
        } else {
            Err(Error::Crypto)
        }
    }
}

#[cfg(test)]
mod test {
    use crate::{
        app::Request,
        authentication::{dummy::Dummy, Authenticator},
        net::Message,
    };
    use bytes::Bytes;

    #[test]
    fn test_dummy() {
        let msg = Message::new(0, 0, Request::new(0, Bytes::default()));
        let mut msg = msg.pack().unwrap();

        let mut dummy_auth = Dummy;
        dummy_auth.sign(&mut msg).expect("sign");
        dummy_auth.verify(&msg).expect("verify");
    }
}
