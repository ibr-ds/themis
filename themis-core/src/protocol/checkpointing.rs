use bytes::Bytes;
use ring::digest::Digest;
use serde::{Deserialize, Serialize};

use crate::net::{NetworkMessage, Sequenced, Tag};

use super::ProtocolTag;

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Checkpoint {
    sequence: u64,
    pub digest: Bytes,
}

impl Checkpoint {
    pub fn new(sequence: u64, state: Bytes) -> Self {
        Self {
            sequence,
            digest: state,
        }
    }
}

impl Sequenced for Checkpoint {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl PartialEq<Digest> for Checkpoint {
    fn eq(&self, other: &Digest) -> bool {
        other.as_ref() == self.digest
    }
}

impl PartialEq<Checkpoint> for Digest {
    fn eq(&self, other: &Checkpoint) -> bool {
        other.digest == self.as_ref()
    }
}

impl<T: Tag> NetworkMessage<ProtocolTag<T>> for Checkpoint {
    const TAG: ProtocolTag<T> = ProtocolTag::Checkpoint;
}

#[cfg(test)]
mod test {
    use super::Checkpoint;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize)]
    struct Ntc(Checkpoint);

    #[test]
    fn message_pack() {
        let cp = Checkpoint {
            sequence: 5,
            digest: vec![3; 64].into(),
        };

        let cp2 = Ntc(cp.clone());

        let vec1 = rmp_serde::to_vec(&cp).unwrap();
        let vec2 = rmp_serde::to_vec(&cp2).unwrap();

        println!("{:?}", &vec1);
        println!("{:?}", &vec2);
        assert_eq!(vec1, vec2);
    }
}
