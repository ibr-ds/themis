use serde::{Deserialize, Serialize};

use themis_core::{
    net::{AuthenticationKind, Message, RawMessage, Sequenced, Tag},
    protocol::{Proposal, ProtocolTag},
};

macro_rules! tag {
    ($message:ident, $tag:tt) => {
        impl ::themis_core::net::NetworkMessage<$crate::messages::PBFT> for $message {
            const TAG: PBFT =
                ::themis_core::protocol::ProtocolTag::Protocol($crate::messages::PBFTTag::$tag);
        }
    };
}

mod ordering;
pub use ordering::{Commit, PrePrepare, Prepare};

mod view_change;
pub use view_change::{NewView, PrepareProof, ViewChange};

mod checkpoint;
use bytes::Bytes;
pub use checkpoint::{FullCheckpoint, GetCheckpoint};
use std::fmt::Display;
pub use themis_core::protocol::Checkpoint;

#[allow(non_camel_case_types)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[repr(u8)]
pub enum PBFTTag {
    PRE_PREPARE,
    PREPARE,
    COMMIT,
    VIEW_CHANGE,
    NEW_VIEW,

    FORWARD,
    ASSIGN,

    FULL_CHECKPOINT,
    GET_CHECKPOINT,

    GET_PROPOSAL,
    GET_RESPONSE,
}

tag!(GetCheckpoint, GET_CHECKPOINT);
tag!(FullCheckpoint, FULL_CHECKPOINT);
tag!(PrePrepare, PRE_PREPARE);
tag!(Prepare, PREPARE);
tag!(Commit, COMMIT);
tag!(ViewChange, VIEW_CHANGE);
tag!(NewView, NEW_VIEW);
tag!(GetProposal, GET_PROPOSAL);
tag!(Forward, FORWARD);
tag!(Assign, ASSIGN);
tag!(GetResponse, GET_RESPONSE);

pub type PBFT = ProtocolTag<PBFTTag>;
pub type PBFTMessage = RawMessage<PBFT>;

macro_rules! unauthed_msgs {
    ($i:ident) => {{
        use PBFTTag::*;
        match $i {
            // Forward is authenticated by digest in PrePrepare
            // GET_* are not stored and don't carry any important info
            // FULL_CHECKPOINT is authenticated by checkpoint quorum
            FORWARD | ASSIGN | FULL_CHECKPOINT | GET_CHECKPOINT | GET_PROPOSAL | GET_RESPONSE => {
                None
            }
            _ => Some(AuthenticationKind::Framework),
        }
    }};
}

impl Tag for PBFTTag {
    fn requires_signing(&self) -> Option<AuthenticationKind> {
        unauthed_msgs!(self)
    }

    fn requires_validation(&self) -> Option<AuthenticationKind> {
        unauthed_msgs!(self)
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Clone)]
pub(crate) struct GetProposal {
    pub digest: Bytes,
}

impl GetProposal {
    pub(crate) fn new(digest: Bytes) -> Self {
        Self { digest }
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Forward(pub Proposal);

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize, Clone)]
pub struct Assign {
    pub sequence: u64,
    pub batch: Proposal,
}

impl Assign {
    pub fn new(sequence: u64, batch: Proposal) -> Self {
        Self { sequence, batch }
    }
}

impl Sequenced for Assign {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl Display for Assign {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use themis_core::net::NetworkMessage;
        write!(
            f,
            "<{:?}|{}|size{}>",
            Self::TAG,
            self.sequence,
            self.batch.len()
        )
    }
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct GetResponse {
    pub proposal: Proposal,
}

impl GetResponse {
    pub(crate) fn new(proposal: Proposal) -> Self {
        Self { proposal }
    }
}

impl Display for GetResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use themis_core::net::NetworkMessage;
        write!(f, "<{:?}|size{}>", Self::TAG, self.proposal.len())
    }
}

pub(crate) trait Viewed {
    fn view(&self) -> u64;
}

impl<T: Viewed> Viewed for Message<T> {
    fn view(&self) -> u64 {
        self.inner.view()
    }
}
