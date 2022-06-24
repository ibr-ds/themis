use bytes::Bytes;
use either::Either;
use fnv::FnvHashMap;
use ring::digest;
use std::{collections::HashMap, mem};
use themis_core::{
    app::{self, Command, Request},
    comms::Sender,
    net::{DisplayBytes, Message, Sequenced},
    protocol::Proposal,
};
use tracing::Span;

use crate::metrics::{request_execution_timer, Timer};

#[derive(Debug, Default)]
pub struct RequestStore<H> {
    requests: HashMap<Bytes, RequestEntry>,
    instances: HashMap<Bytes, Batch>,
    client_states: FnvHashMap<u64, ClientState>,
    _hasher: H,
}

impl<H> RequestStore<H>
where
    H: ProposalHasher,
{
    pub fn new(hasher: H) -> Self {
        Self {
            _hasher: hasher,
            ..Default::default()
        }
    }

    pub fn has_unassigned(&self) -> Option<&Bytes> {
        self.requests
            .iter()
            .find(|(_digest, entry)| entry.sequence.is_none())
            .map(|(digest, _)| digest)
    }

    // Registers an instance to the request store.
    pub fn assign_proposal(
        &mut self,
        sequence: u64,
        proposal_ref: ProposalRef,
        proposal_digest: Bytes,
    ) {
        for digest in &proposal_ref {
            if let Some(entry) = self.requests.get_mut(digest) {
                entry.sequence = Some(sequence)
            }
        }

        self.instances.insert(
            proposal_digest,
            Batch {
                state: BatchState::Open,
                requests: proposal_ref,
                sequence,
            },
        );
    }

    pub fn is_known(&self, request: &[u8]) -> bool {
        self.requests.get(request).is_some()
    }

    // Attempts to find a proposal among known requestrs. Only makes sense without batching
    pub fn try_assign_proposal(&mut self, sequence: u64, digest: &Bytes) -> bool {
        if let Some(request) = self.requests.get_mut(digest.as_ref()) {
            self.instances.insert(
                digest.clone(),
                Batch {
                    state: BatchState::Open,
                    requests: ProposalRef::Single(digest.clone()),
                    sequence,
                },
            );
            request.sequence = Some(sequence);
            true
        } else {
            false
        }
    }

    pub fn build_proposal(&self, digest: &[u8]) -> Option<(u64, Proposal)> {
        let proposal = self.instances.get(digest)?;
        let sequence = proposal.sequence;

        let proposal = match &proposal.requests {
            ProposalRef::Single(request) => {
                let request = self.requests.get(request)?;
                Proposal::Single(request.request.clone())
            }
            ProposalRef::Batch(batch) => {
                let mut proposal = Vec::new();
                for request in batch {
                    let request = self.requests.get(request)?;
                    proposal.push(request.request.clone())
                }
                Proposal::Batch(proposal)
            }
        };
        Some((sequence, proposal))
    }

    pub fn has_proposal(&self, digest: &[u8]) -> bool {
        match self.instances.get(digest) {
            None => false,
            Some(Batch {
                state: BatchState::Missing,
                ..
            }) => false,
            Some(_) => true,
        }
    }

    pub fn get_proposal(&self, digest: &[u8]) -> Option<&Batch> {
        self.instances.get(digest)
    }

    pub fn set_proposal_missing(&mut self, sequence: u64, digest: Bytes) {
        self.instances.insert(
            digest,
            Batch {
                state: BatchState::Missing,
                requests: ProposalRef::default(),
                sequence,
            },
        );
    }

    pub fn has_request(&self, request: &[u8]) -> bool {
        self.requests.contains_key(request)
    }

    pub fn add_request(
        &mut self,
        request: Message<app::Request>,
        sequence: Option<u64>,
    ) -> Option<u64> {
        let digest = H::hash_request(&request);

        let known_sequence = if let Some(Batch {
            state,
            requests,
            sequence,
        }) = self.instances.get_mut(&digest)
        {
            if *state == BatchState::Missing {
                *state = BatchState::Open;
                *requests = ProposalRef::Single(digest.clone());
            }
            Some(*sequence)
        } else {
            sequence
        };

        if !self.requests.contains_key(&digest) {
            let entry = RequestEntry::new(&digest, request, known_sequence);
            let _ = self.requests.insert(digest, entry);
        } else {
            tracing::warn!("Potentially doubled timeout");
        }

        known_sequence
    }

    pub fn set_prepared(&mut self, batch: &[u8]) {
        if let Some(entry) = self.instances.get_mut(batch) {
            entry.state = BatchState::Prepared;
            for digest in &entry.requests {
                let request = self.requests.get(digest.as_ref()).unwrap();
                self.client_states
                    .entry(request.request.source)
                    .or_default()
                    .last_prepare = request.request.sequence()
            }
        }
    }

    /// Returns true if the request is a duplicate
    /// This is determined by
    /// 1. checking the known requests that are not prepared
    /// 2. checking if the sequence number for the client is lower than the last
    /// prepared request
    pub fn is_duplicate(&self, request: &Message<app::Request>) -> bool {
        let digest = H::hash_request(&request);
        self.requests
            .get(&digest)
            .and_then(|e| e.sequence)
            .is_some()
            || self
                .client_states
                .get(&request.source)
                .map(|s| s.last_prepare >= request.sequence())
                .unwrap_or(false)
    }

    // pub fn requests(&self, digest: &[u8]) -> impl Iterator<Item = &Message<Request>> {
    //     self.instances
    //         .get(digest)
    //         .into_iter()
    //         .flat_map(|b| &b.requests)
    //         .map(move |digest| self.requests.get(digest).unwrap())
    //         .map(|e| &e.request)
    // }

    pub async fn execute_requests(
        &mut self,
        digest: &[u8],
        executor: &mut Sender<Command>,
    ) -> Result<(), themis_core::Error> {
        if let Some(instance) = self.instances.get(digest) {
            for request in &instance.requests {
                let entry = self.requests.get_mut(request).unwrap();
                entry.span.take();
                entry.timer.stop_and_record();

                tracing::trace!(
                    "Executing request {} from {}",
                    entry.request.source,
                    entry.request.sequence()
                );

                executor
                    .send(Command::Request(entry.request.clone()))
                    .await?;
            }
        }

        Ok(())
    }

    pub fn remove(&mut self, digest: &[u8]) {
        if let Some(instance) = self.instances.remove(digest) {
            for request in instance.requests {
                self.requests.remove(&request);
            }
        }
    }

    /// Removes all proposals that are not in keep
    fn reset_proposals<'a>(&mut self, keep: impl Iterator<Item = &'a Bytes>) {
        let mut proposals = HashMap::new();
        for proposal in keep {
            if let Some((proposal, mut batch)) = self.instances.remove_entry(proposal) {
                batch.state = BatchState::Open;
                proposals.insert(proposal, batch);
            }
        }
        self.instances = proposals;
    }

    /// Removes all proposals that are not in keep. Clears the request store and resets timeouts for all kept request.
    pub fn reset_request_store<'a>(
        &mut self,
        keep: impl Iterator<Item = &'a Bytes>,
    ) -> Vec<(Bytes, Message<Request>)> {
        self.reset_proposals(keep);
        let mut requests = HashMap::new();
        for proposal in self.instances.values() {
            for digest in &proposal.requests {
                if let Some((digest, request)) = self.requests.remove_entry(digest) {
                    requests.insert(digest, request);
                } else {
                    todo!("missing requests")
                };
            }
        }
        let unproposed = mem::replace(&mut self.requests, requests);

        let mut ret = Vec::new();
        for (digest, mut r) in unproposed {
            r.sequence = None;
            ret.push((digest.clone(), r.request.clone()));
            self.requests.insert(digest, r);
        }

        ret
    }
}

pub trait ProposalHasher: Send + Sync + Default + 'static {
    fn make_ctx() -> ring::digest::Context;

    fn hash_request_to(req: &Message<app::Request>, ctx: &mut digest::Context);
    fn hash_request(req: &Message<app::Request>) -> Bytes {
        let mut cx = Self::make_ctx();
        Self::hash_request_to(req, &mut cx);
        let digest = cx.finish();
        Bytes::copy_from_slice(digest.as_ref())
    }

    fn hash_batch(reqs: &[Message<app::Request>]) -> Bytes {
        let mut ctx = Self::make_ctx();
        for req in reqs {
            Self::hash_request_to(req, &mut ctx);
        }
        Bytes::copy_from_slice(ctx.finish().as_ref())
    }

    fn hash_proposal(prop: &Proposal) -> Bytes {
        match prop {
            Proposal::Batch(batch) => Self::hash_batch(batch),
            Proposal::Single(req) => Self::hash_request(req),
        }
    }
}

#[derive(Default, Debug)]
pub struct DefaultHasher;

impl ProposalHasher for DefaultHasher {
    fn make_ctx() -> digest::Context {
        digest::Context::new(&digest::SHA256)
    }

    /// Calculate the hash for requests
    /// Requests are only used as serialized packets here
    fn hash_request_to(req: &Message<Request>, ctx: &mut digest::Context) {
        //We include the source id and the payload value
        //So the same request sent to different replicas is considered the same
        //But equal requests from different clients are not considered the same
        //Including the tag is pointless so we don't
        ctx.update(&req.source.to_le_bytes());
        ctx.update(&req.inner.sequence.to_le_bytes());
        ctx.update(&req.inner.payload);
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum BatchState {
    Missing,
    Open,
    Prepared,
}

#[derive(Debug)]
pub struct Batch {
    pub state: BatchState,
    pub requests: ProposalRef,
    pub sequence: u64,
}

#[derive(Debug)]
pub enum ProposalRef {
    Single(Bytes),
    Batch(Vec<Bytes>),
}

impl ProposalRef {
    pub fn iter(&self) -> impl Iterator<Item = &'_ Bytes> {
        self.into_iter()
    }
}

impl Default for ProposalRef {
    fn default() -> Self {
        Self::Batch(Vec::new())
    }
}

impl IntoIterator for ProposalRef {
    type Item = Bytes;
    type IntoIter = Either<std::iter::Once<Bytes>, std::vec::IntoIter<Bytes>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ProposalRef::Single(digest) => Either::Left(std::iter::once(digest)),
            ProposalRef::Batch(digest_vec) => Either::Right(digest_vec.into_iter()),
        }
    }
}

impl<'a> IntoIterator for &'a ProposalRef {
    type Item = &'a Bytes;
    type IntoIter = Either<std::iter::Once<&'a Bytes>, std::slice::Iter<'a, Bytes>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            ProposalRef::Single(digest) => Either::Left(std::iter::once(digest)),
            ProposalRef::Batch(digest_vec) => Either::Right(digest_vec.iter()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Default)]
struct ClientState {
    last_prepare: u64,
    last_reply: u64,
}

#[derive(Debug)]
pub struct RequestEntry {
    pub request: Message<app::Request>,
    pub sequence: Option<u64>,
    pub span: Option<Span>,
    pub timer: Timer,
}

impl RequestEntry {
    pub fn new(digest: &[u8], request: Message<app::Request>, sequence: Option<u64>) -> Self {
        let span = Some(
            tracing::info_span!(target: "__measure::request", parent: None, "request", digest = format_args!("{}", DisplayBytes(&digest))),
        );
        let timer = request_execution_timer();

        Self {
            request,
            sequence,
            span,
            timer,
        }
    }
}
