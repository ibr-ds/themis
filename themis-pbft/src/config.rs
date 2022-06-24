use serde::{Deserialize, Serialize};
use themis_core::config::{well_known, Config};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[repr(u8)]
pub enum ReplyMode {
    All,
    None,
    //TODO Feature: Hash Replies
}

/// Decides how requests are forwarded
/// 1. from the primary to backups along with a pre-prepare
/// 2. from backups to the primary when receiving request directly from a client
#[derive(Debug, Clone, Copy, Eq, PartialEq, Deserialize, Serialize)]
#[repr(u8)]
pub enum ForwardingMode {
    /// Forward full requests
    Full,
    /// Only send hashes, replicas must receive the request through other means (e.g. mandatory broadcasting)
    Hashes,
    /// Don't forward anything. This is incompatible with batching, as backups will not be informed about formed batches.
    None,
}

const REQUEST_PROPOSALS: fn() -> bool = || false;

/// Configuration Items for PBFT
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct PBFTConfig {
    #[serde(default)]
    pub num_peers: u64,
    pub faults: u64,
    pub checkpoint_interval: u64,
    pub high_mark_delta: usize,
    pub request_timeout: u64,
    pub keep_checkpoints: u64,
    #[serde(default = "REQUEST_PROPOSALS")]
    pub request_proposals: bool,
    pub primary_forwarding: ForwardingMode,
    pub backup_forwarding: ForwardingMode,
    pub reply_mode: ReplyMode,
    /// Delays proposing by some microseconds
    pub debug_delay_proposal_us: u64,
    /// forces a view change by not proposing
    pub debug_force_vc: ForceVc,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ForceVc {
    pub in_view: u64,
    pub at_seq: u64,
}

impl PBFTConfig {
    pub fn from_config(config: &Config) -> Self {
        let num_peers = well_known::num_peers(config).expect("num peers");
        let mut pbft: Self = config.get("pbft").expect("pbft");
        pbft.num_peers = num_peers as u64;
        pbft
    }

    pub fn votes(&self) -> usize {
        2 * self.faults as usize + 1
    }
}

impl Default for PBFTConfig {
    fn default() -> Self {
        Self {
            num_peers: 4,
            faults: 1,
            checkpoint_interval: 1000,
            high_mark_delta: 3000,
            request_timeout: 1000,
            keep_checkpoints: 2,
            request_proposals: false,
            reply_mode: ReplyMode::All,
            primary_forwarding: ForwardingMode::Full,
            backup_forwarding: ForwardingMode::Full,
            debug_delay_proposal_us: 0,
            debug_force_vc: ForceVc { in_view: 0, at_seq: 0 }
        }
    }
}
