use crate::messages;
use bytes::Bytes;
use ring::digest::{Digest, SHA256};
use std::collections::BTreeMap;
use themis_core::{
    net::{Message, Sequenced},
    protocol::quorum::{self, Quorum},
};

#[derive(Debug, Clone)]
pub(crate) enum StableCheckpoint {
    Own {
        handle: Bytes,
    },
    Foreign {
        message: Message<messages::FullCheckpoint>,
    },
}

impl StableCheckpoint {
    pub fn handle(&self) -> &Bytes {
        match self {
            Self::Own { handle } => handle,
            Self::Foreign { message, .. } => &message.inner.handle,
        }
    }
}
/// States of a checkpoint.
#[derive(Debug)]
pub(crate) enum Checkpoint {
    /// Not enough matching messages for this checkpoint to be stable.
    Open {
        my_handle: Option<Bytes>,
        proof: Quorum<Message<messages::Checkpoint>>,
    },
    /// Intermediate state. Enough messages to be stable. But we don't have the actual
    /// unhashed data.
    ProofStable {
        to_ask: usize,
        proof: Box<[Message<messages::Checkpoint>]>,
    },
    /// We own enough checkpoint messages and have the unhashed checkpoint.
    Stable {
        app_data: StableCheckpoint,
        proof: Box<[Message<messages::Checkpoint>]>,
    },
}

impl Checkpoint {
    fn new(faults: u64, capacity: u64) -> Self {
        Checkpoint::Open {
            my_handle: None,
            proof: Quorum::new((2 * faults + 1) as usize, capacity as usize),
        }
    }

    fn is_stable(&self) -> bool {
        matches!(self, Self::Stable { .. })
    }
}

#[derive(Debug)]
pub(crate) struct Checkpointing {
    checkpoints: BTreeMap<u64, Checkpoint>,
    keep_checkpoints: u64,
    faults: u64,
    num_peers: u64,
}

impl Checkpointing {
    pub fn new(keep_checkpoints: u64, faults: u64, num_peers: u64) -> Self {
        Self {
            checkpoints: Default::default(),
            keep_checkpoints,
            faults,
            num_peers,
        }
    }

    pub fn new_checkpoint(
        &mut self,
        sequence: u64,
        app_checkpoint: Bytes,
        own_message: Message<messages::Checkpoint>,
    ) -> Result<bool, Error> {
        let (faults, num_peers) = (self.faults, self.num_peers);
        let entry = self
            .checkpoints
            .entry(sequence)
            .or_insert_with(|| Checkpoint::new(faults, num_peers));

        match entry {
            Checkpoint::Open { my_handle, .. } => {
                *my_handle = Some(app_checkpoint);
                self.offer_proof(own_message)
            }
            Checkpoint::ProofStable { proof, .. } => {
                if proof[0].inner.digest == own_message.inner.digest {
                    let proof = std::mem::take(proof);
                    *entry = Checkpoint::Stable {
                        proof,
                        app_data: StableCheckpoint::Own {
                            handle: app_checkpoint,
                        },
                    };
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            _ => Ok(false),
        }
    }

    pub fn get_own_stable(&self, sequence: u64) -> Option<&[Message<messages::Checkpoint>]> {
        self.checkpoints.get(&sequence).and_then(|cp| match cp {
            Checkpoint::Stable {
                proof,
                app_data: StableCheckpoint::Own { .. },
            } => Some(proof.as_ref()),
            _ => None,
        })
    }

    pub fn offer_proof(&mut self, message: Message<messages::Checkpoint>) -> Result<bool, Error> {
        let (faults, num_peers) = (self.faults, self.num_peers);
        let entry = self
            .checkpoints
            .entry(message.sequence())
            .or_insert_with(|| Checkpoint::new(faults, num_peers));

        match entry {
            Checkpoint::Open { my_handle, proof } => {
                if let quorum::State::Finished(i) =
                    proof.offer(message).map_err(Error::BadQuorum)?
                {
                    let proof = proof.take_class(i).into_boxed_slice();
                    match my_handle.take() {
                        Some(bytes) if digest_handle(&bytes) == proof[0].inner => {
                            *entry = Checkpoint::Stable {
                                proof,
                                app_data: StableCheckpoint::Own { handle: bytes },
                            };
                            Ok(true)
                        }
                        Some(_) | None => {
                            let to_ask = proof[0].source;
                            *entry = Checkpoint::ProofStable { proof, to_ask: 0 };
                            Err(Error::CheckpointMismatch { to_ask })
                        }
                    }
                } else {
                    Ok(false)
                }
            }
            Checkpoint::ProofStable { .. } => Ok(false),
            Checkpoint::Stable { .. } => Ok(false),
        }
    }

    async fn try_apply_checkpoint(
        proof: &[Message<messages::Checkpoint>],
        message: &Message<messages::FullCheckpoint>,
        comms: &mut super::Comms,
    ) -> bool {
        if message.inner.is_empty() {
            tracing::trace!("Message is empty");
            return false;
        }

        let messages::FullCheckpoint {
            handle,
            data,
            sequence,
        } = &message.inner;

        if digest_handle(handle) != proof[0].inner {
            tracing::trace!("Handle does not match quorum");
            return false;
        }
        match comms
            .try_apply_checkpoint(
                *sequence,
                proof.to_owned(),
                Some((handle.clone(), data.clone())),
            )
            .await
        {
            Err(e) => {
                tracing::error!("{}", e);
                false
            }
            Ok(b) => b,
        }
    }

    pub async fn offer_full_checkpoint(
        &mut self,
        message: Message<messages::FullCheckpoint>,
        comms: &mut super::Comms,
    ) -> Result<(), Error> {
        let sequence = message.sequence();
        let entry = self
            .checkpoints
            .get_mut(&message.sequence())
            .ok_or_else(|| Error::MissingCheckpoint { sequence })?;

        match entry {
            Checkpoint::ProofStable { proof, to_ask } => {
                if Self::try_apply_checkpoint(&*proof, &message, comms).await {
                    *entry = Checkpoint::Stable {
                        app_data: StableCheckpoint::Foreign { message },
                        proof: mem::take(proof),
                    };
                    Ok(())
                } else {
                    *to_ask += 1;
                    *to_ask %= proof.len();
                    Err(Error::CheckpointMismatch {
                        to_ask: proof[*to_ask].source,
                    })
                }
            }
            Checkpoint::Open { .. } => Err(Error::NotFinished),
            Checkpoint::Stable { .. } => Err(Error::NotOpen),
        }
    }

    // might need at some point
    #[allow(unused)]
    pub fn check_complete(&self, sequence: u64) -> Result<(), Error> {
        match self.checkpoints.get(&sequence) {
            Some(Checkpoint::Open { .. }) | None => Err(Error::NotFinished),
            Some(Checkpoint::ProofStable { to_ask, proof }) => Err(Error::CheckpointMismatch {
                to_ask: proof[*to_ask].source,
            }),
            Some(Checkpoint::Stable { .. }) => Ok(()),
        }
    }

    pub fn get_data(&self, sequence: u64) -> Result<&StableCheckpoint, Error> {
        match self.checkpoints.get(&sequence) {
            Some(Checkpoint::Stable { app_data, .. }) => Ok(app_data),
            _ => Err(Error::NotFinished),
        }
    }

    pub fn get_proof(&self, sequence: u64) -> Option<&[Message<messages::Checkpoint>]> {
        match self.checkpoints.get(&sequence) {
            Some(Checkpoint::Open { .. }) | None => None,
            Some(Checkpoint::Stable { proof, .. })
            | Some(Checkpoint::ProofStable { proof, .. }) => Some(proof),
        }
    }

    /// Removes checkpoints according to config
    pub fn clean_up(&mut self, latest_stable: u64) {
        // This could probably be done inline but rust does not have these APIs
        let checkpoints = mem::take(&mut self.checkpoints);
        let mut count = 0;
        self.checkpoints = checkpoints
            .into_iter()
            .rev()
            .filter(|(seq, cp)| {
                if *seq > latest_stable {
                    // newer checkpoints are kept
                    true
                } else if cp.is_stable() && count < self.keep_checkpoints {
                    // keep number of stable checkpoint according to config
                    count += 1;
                    true
                } else {
                    // everything else is discarded
                    false
                }
            })
            .collect();
    }
}

fn digest_handle(handle: &[u8]) -> Digest {
    ring::digest::digest(&SHA256, handle)
}

use core::mem;

pub(crate) type CpError = self::Error;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Checkpoint not open")]
    NotOpen,
    #[error("Checkpoint not finished")]
    NotFinished,
    #[error("Checkpoint {sequence} missing")]
    MissingCheckpoint { sequence: u64 },
    #[error("Checkpoint does not match")]
    CheckpointMismatch { to_ask: u64 },
    #[error("Checkpoint Quorum failure: {0}")]
    BadQuorum(quorum::Error),
}

#[cfg(test)]
mod test {
    use crate::{
        checkpointing::{Checkpoint, Checkpointing, StableCheckpoint},
        config::PBFTConfig,
    };

    use bytes::Bytes;

    #[test]
    fn delete_checkpoints() {
        let pbftconfig = PBFTConfig {
            keep_checkpoints: 2,
            ..Default::default()
        };

        let mut cping = Checkpointing::new(pbftconfig.keep_checkpoints, pbftconfig.faults, 4);

        // easier to create than Checkpoint::Open
        let not_stable = || Checkpoint::ProofStable {
            to_ask: 0,
            proof: Box::default(),
        };
        let stable = || Checkpoint::Stable {
            proof: Box::default(),
            app_data: StableCheckpoint::Own {
                handle: Bytes::new(),
            },
        };

        cping.checkpoints.insert(0, (not_stable)());
        cping.checkpoints.insert(1, (not_stable)());
        cping.checkpoints.insert(1, (stable)());
        cping.checkpoints.insert(2, (not_stable)());
        cping.checkpoints.insert(3, (not_stable)()); // drop^
        cping.checkpoints.insert(4, (stable)()); // keep due to config
        cping.checkpoints.insert(5, (not_stable)()); // drop
        cping.checkpoints.insert(6, (stable)()); // latest stable, keep
        cping.checkpoints.insert(7, (not_stable)()); //keep
        cping.checkpoints.insert(8, (not_stable)()); //keep

        cping.clean_up(6);

        dbg!(&cping.checkpoints);

        for i in &[4, 6, 7, 8] {
            assert!(cping.checkpoints.contains_key(dbg!(i)));
        }
        assert_eq!(cping.checkpoints.len(), 4);
    }
}
