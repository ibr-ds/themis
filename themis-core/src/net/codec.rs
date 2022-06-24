use std::{
    fmt::{self, Debug, Formatter},
    io::{self, ErrorKind, ErrorKind::InvalidData},
    marker::PhantomData,
};

use bytes::{
    buf::{Buf, BufMut},
    BytesMut,
};
use rmp_serde::Serializer;
use serde::ser::Serialize;
use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

use crate::{
    authentication::{dummy::Dummy, Authenticator, AuthenticatorExt},
    net::{
        messages::{Message, Raw},
        AuthenticationKind,
    },
};

use super::messages::Tag;

pub struct MessageCodec<Tag> {
    inner: LengthDelimitedCodec,
    verify: Box<dyn Authenticator<Tag> + Send>,
    encode_buf: BytesMut,
    _phantom: PhantomData<Tag>,
}

impl<Tag> Debug for MessageCodec<Tag> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MessageCodec")
            .field("inner", &self.inner)
            .finish()
    }
}

impl<Tag> MessageCodec<Tag> {
    pub fn new(verify: Box<dyn Authenticator<Tag> + Send>) -> Self {
        Self {
            verify,
            inner: LengthDelimitedCodec::builder()
                .max_frame_length(usize::max_value())
                .new_codec(),
            ..Default::default()
        }
    }
}

impl<TAG: Tag> Decoder for MessageCodec<TAG> {
    type Item = Message<Raw<TAG>>;
    type Error = Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let inner = self.inner.decode(src)?;
        let frame = match inner {
            None => return Ok(None),
            Some(frame) => frame,
        };

        tracing::trace!("decoding new message len={}", frame.len());
        let mut reader = frame.reader(); //io::Cursor
        let message: Message<Raw<TAG>> = rmp_serde::from_read(&mut reader)?;

        if message.tag().requires_validation() == Some(AuthenticationKind::Framework) {
            // verify hmac, if has context
            self.verify
                .verify(&message)
                .map_err(|e| io::Error::new(InvalidData, e))?;
            tracing::trace!("message verified");
        }

        Ok(Some(message))
    }
}

impl<TAG: Tag> Encoder<Message<Raw<TAG>>> for MessageCodec<TAG> {
    type Error = Error;

    fn encode(
        &mut self,
        mut message: Message<Raw<TAG>>,
        dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        if message.tag().requires_signing() == Some(AuthenticationKind::Framework) {
            self.verify
                .sign_message(&mut message)
                .map_err(|e| io::Error::new(InvalidData, e))?
        }

        let writer = (&mut self.encode_buf).writer();
        let mut se = Serializer::new(writer);
        message.serialize(&mut se)?;

        tracing::trace!("encoded {} bytes", self.encode_buf.len());
        self.inner
            .encode(self.encode_buf.split_off(0).freeze(), dst)?;
        self.encode_buf.reserve(1); //reclaim split_off space;
        Ok(())
    }
}

impl<Tag> Default for MessageCodec<Tag> {
    fn default() -> Self {
        Self {
            inner: LengthDelimitedCodec::default(),
            verify: Box::new(Dummy),
            encode_buf: BytesMut::new(),
            _phantom: PhantomData,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Encode error: {}", source)]
    Encode { source: rmp_serde::encode::Error },
    #[error("Decode error: {}", source)]
    Decode { source: rmp_serde::decode::Error },
    #[error("Io error in codec: {}", source)]
    Io { source: io::Error },
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Io { source: e }
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Self::Encode { source: e }
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(e: rmp_serde::decode::Error) -> Self {
        Self::Decode { source: e }
    }
}

impl From<Error> for io::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Io { source } => source,
            Error::Decode { source } => io::Error::new(ErrorKind::InvalidData, source),
            Error::Encode { source } => io::Error::new(ErrorKind::InvalidData, source),
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use bytes::{BufMut, BytesMut};

    #[test]
    fn bytes_mut_split_off() {
        let mut encode_buf = BytesMut::with_capacity(1024);
        let ptr_start = encode_buf.as_ptr();

        for &size in &[1usize, 10, 100, 500, 1000, 1024] {
            let _ = (&mut encode_buf)
                .writer()
                .write(vec![0u8; size].as_slice())
                .expect("encode");
            assert_eq!(encode_buf.len(), size);
            let freeze = encode_buf.split_off(0).freeze();
            assert_eq!(freeze.as_ptr(), ptr_start);
            assert_eq!(encode_buf.capacity(), 0);
            drop(freeze);
            encode_buf.reserve(1);
            assert_eq!(encode_buf.capacity(), 1024);
            assert_eq!(encode_buf.as_ptr(), ptr_start);
        }
    }
}
