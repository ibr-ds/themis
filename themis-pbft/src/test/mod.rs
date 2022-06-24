use std::collections::vec_deque::VecDeque;

use bytes::Bytes;

use themis_core::{
    app::{self, Client},
    comms::{unbounded, Receiver},
    config::{self, Config},
    net::{Message, Raw},
};

use crate::{
    messages::{self, Commit, PBFTMessage, PrePrepare, Prepare},
    PBFTConfig, PBFT,
};
use std::env::set_var;
use themis_core::authentication::{dummy::Dummy, Authenticator};

mod checkpointing;
mod regular;
mod request_ordering;
mod view_change;

pub use regular::test_round_leader;

#[derive(Debug)]
pub struct PBFTContext {
    pbft: PBFT,
    r: Receiver<PBFTMessage>,
    c: Receiver<Message<Raw<Client>>>,
    a: Receiver<app::Command>,
}

impl PBFTContext {
    pub fn new(
        pbft: PBFT,
        r: Receiver<PBFTMessage>,
        c: Receiver<Message<Raw<Client>>>,
        a: Receiver<app::Command>,
    ) -> Self {
        Self { pbft, r, c, a }
    }
}

#[allow(unused)]
fn setup_logging() {
    if std::env::var("RUST_LOG").is_err() {
        set_var("RUST_LOG", "debug");
    }
}

fn setup_verifier(_me: u64) -> Box<dyn Authenticator<messages::PBFT> + Send + Sync> {
    Box::new(Dummy)
}

pub fn add_pbft_config(config: &mut Config) {
    let pbftc = PBFTConfig::default();
    config.set("pbft", pbftc).expect("set pbft");
}

fn setup_pbft() -> PBFTContext {
    let mut config = config::default();
    add_pbft_config(&mut config);

    let r = unbounded();
    let c = unbounded();
    let a = unbounded();
    let pbft = PBFT::new(0, r.0, c.0, a.0, setup_verifier(0), &config);
    PBFTContext::new(pbft, r.1, c.1, a.1)
}

pub fn setup_pbft_backup() -> PBFTContext {
    let mut config = config::default();
    add_pbft_config(&mut config);

    let r = unbounded();
    let c = unbounded();
    let a = unbounded();
    let pbft = PBFT::new(1, r.0, c.0, a.0, setup_verifier(1), &config);
    PBFTContext::new(pbft, r.1, c.1, a.1)
}

fn setup_with_checkpoint(primary: bool, checkpoint_interval: u64) -> PBFTContext {
    let mut config = config::default();
    add_pbft_config(&mut config);
    config
        .set("pbft.checkpoint_interval", checkpoint_interval)
        .unwrap();

    let me = if primary { 0 } else { 1 };

    let r = unbounded();
    let c = unbounded();
    let a = unbounded();
    let pbft = PBFT::new(me, r.0, c.0, a.0, setup_verifier(me), &config);
    PBFTContext::new(pbft, r.1, c.1, a.1)
}

fn client() -> u64 {
    100
}

fn prepare_set(primary: u64, sequence: u64, view: u64, me: u64, digest: Bytes) -> Vec<PBFTMessage> {
    let mut msgs = VecDeque::with_capacity(4);
    for i in (0..4).filter(|i| *i != me) {
        let mut pre = if i == primary {
            Message::new(i, me, PrePrepare::new(sequence, view, digest.clone()))
                .pack()
                .unwrap()
        } else {
            Message::new(i, me, Prepare::new(sequence, view, digest.clone()))
                .pack()
                .unwrap()
        };
        Dummy.sign(&mut pre).unwrap();
        msgs.push_back(pre);
    }

    msgs.into()
}

fn commit_set(sequence: u64, view: u64, me: u64, hash: Bytes) -> Vec<PBFTMessage> {
    let mut msgs = Vec::new();
    for i in (0..4).filter(|i| *i != me) {
        let commit = Message::new(i, me, Commit::new(sequence, view, hash.clone()));
        msgs.push(commit.pack().unwrap());
    }
    msgs
}
