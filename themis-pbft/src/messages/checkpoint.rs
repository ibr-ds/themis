use bytes::Bytes;
use serde::{Deserialize, Serialize};

use themis_core::net::Sequenced;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub struct FullCheckpoint {
    pub sequence: u64,
    pub handle: Bytes,
    pub data: Bytes,
}

impl FullCheckpoint {
    pub fn new(sequence: u64, handle: Bytes, data: Bytes) -> Self {
        Self {
            sequence,
            handle,
            data,
        }
    }

    pub fn empty(sequence: u64) -> Self {
        Self {
            sequence,
            handle: Bytes::new(),
            data: Bytes::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.handle.is_empty() && self.data.is_empty()
    }
}

impl Sequenced for FullCheckpoint {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct GetCheckpoint {
    sequence: u64,
}

impl GetCheckpoint {
    pub fn new(sequence: u64) -> Self {
        Self { sequence }
    }
}

impl Sequenced for GetCheckpoint {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}
