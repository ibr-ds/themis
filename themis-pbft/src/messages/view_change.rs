use serde::{Deserialize, Serialize};

use themis_core::net::Message;

use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareProof(pub Message<PrePrepare>, pub Box<[Message<Prepare>]>);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ViewChange {
    new_view: u64,
    pub checkpoint: u64,
    pub checkpoint_proof: Box<[Message<Checkpoint>]>,
    pub prepares: Box<[PrepareProof]>,
}

impl ViewChange {
    pub fn new(
        new_view: u64,
        checkpoint: u64,
        checkpoint_proof: Box<[Message<Checkpoint>]>,
        prepares: Box<[PrepareProof]>,
    ) -> Self {
        Self {
            new_view,
            checkpoint,
            checkpoint_proof,
            prepares,
        }
    }
}

impl PartialEq for ViewChange {
    fn eq(&self, other: &Self) -> bool {
        self.new_view == other.new_view
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct NewView {
    view: u64,
    pub view_changes: Box<[Message<ViewChange>]>,
    pub pre_prepares: Box<[Message<PrePrepare>]>,
}

impl NewView {
    pub fn new(
        view: u64,
        view_changes: Box<[Message<ViewChange>]>,
        pre_prepares: Box<[Message<PrePrepare>]>,
    ) -> Self {
        Self {
            view,
            view_changes,
            pre_prepares,
        }
    }
}

impl Viewed for ViewChange {
    fn view(&self) -> u64 {
        self.new_view
    }
}

impl Viewed for NewView {
    fn view(&self) -> u64 {
        self.view
    }
}
