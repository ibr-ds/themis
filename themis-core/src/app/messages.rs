use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::net::{NetworkMessage, Sequenced};

use super::Client;

pub use request::RequestFlags;
pub use response::ResponseFlags;

/// Basic Request type for applications
/// Includes sender id
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone, Hash)]
pub struct Request {
    /// Request sequence number, increasing
    /// e.g. PBFT uses timestamps
    pub sequence: u64,
    /// Request flags
    pub flags: RequestFlags,
    /// Request payload
    pub payload: Bytes,
}

impl Request {
    pub fn new(sequence: u64, payload: Bytes) -> Self {
        Self {
            sequence,
            flags: RequestFlags::empty(),
            payload,
        }
    }

    pub fn builder() -> request::Builder {
        request::Builder::default()
    }
}

impl Sequenced for Request {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl NetworkMessage<Client> for Request {
    const TAG: Client = Client::Request;
}

/// Basic Response type for applications
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct Response {
    pub sequence: u64,
    pub flags: ResponseFlags,
    /// Indicated that future requests should be sent to a different replica
    pub contact: Option<u64>,
    pub payload: Bytes,
}

impl Response {
    pub fn new(sequence: u64, payload: Bytes) -> Self {
        Self {
            sequence,
            flags: ResponseFlags::empty(),
            contact: Default::default(),
            payload,
        }
    }
}

impl Response {
    pub fn with_contact(sequence: u64, payload: Bytes, contact: u64) -> Self {
        Self {
            sequence,
            flags: ResponseFlags::empty(),
            contact: Some(contact),
            payload,
        }
    }
}

impl Sequenced for Response {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl NetworkMessage<Client> for Response {
    const TAG: Client = Client::Request;
}

pub mod request {
    use bitflags::bitflags;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::Request;

    bitflags! {
        #[derive(Serialize, Deserialize)]
        pub struct RequestFlags: u8 {
            const READ_ONLY = 0x1;
            const HASH_REPLY = 0x2;
            const UNSEQUENCED = 0x4;
            const NO_REPLY = 0x8;
        }
    }

    #[derive(Debug)]
    pub struct Builder {
        request: Request,
    }

    impl Builder {
        fn new() -> Self {
            Self {
                request: Request {
                    sequence: 0,
                    flags: RequestFlags::empty(),
                    payload: Bytes::new(),
                },
            }
        }

        pub fn sequence(mut self, sequence: u64) -> Self {
            self.request.sequence = sequence;
            self
        }

        pub fn flag(mut self, flag: RequestFlags) -> Self {
            self.request.flags |= flag;
            self
        }

        pub fn payload(mut self, payload: Bytes) -> Self {
            self.request.payload = payload;
            self
        }

        pub fn build(self) -> Request {
            self.request
        }
    }

    impl Default for Builder {
        fn default() -> Self {
            Self::new()
        }
    }
}

pub mod response {
    use bitflags::bitflags;
    use bytes::Bytes;
    use serde::{Deserialize, Serialize};

    use super::Response;

    bitflags! {
        #[derive(Serialize, Deserialize)]
        pub struct ResponseFlags: u8 {
            const HASHED = 0x1;
        }
    }

    pub struct Builder {
        response: Response,
    }

    impl Default for Builder {
        fn default() -> Self {
            Self {
                response: Response {
                    sequence: 0,
                    flags: ResponseFlags::empty(),
                    contact: None,
                    payload: Bytes::new(),
                },
            }
        }
    }

    impl Builder {
        pub fn sequence(mut self, sequence: u64) -> Self {
            self.response.sequence = sequence;
            self
        }

        pub fn flag(mut self, flag: ResponseFlags) -> Self {
            self.response.flags |= flag;
            self
        }

        pub fn contact(mut self, contact: u64) -> Self {
            self.response.contact = Some(contact);
            self
        }

        pub fn payload(mut self, payload: Bytes) -> Self {
            self.response.payload = payload;
            self
        }

        pub fn build(self) -> Response {
            self.response
        }
    }
}

#[cfg(test)]
mod test {
    use crate::net::Message;

    use super::Request;

    #[test]
    fn serde() {
        let request = Message::broadcast(222, Request::new(333, vec![111].into()));
        let message = request.pack().unwrap();

        let _r2: Message<Request> = message.unpack().unwrap();
    }
}
