#![feature(type_alias_impl_trait)]
#![allow(clippy::mutable_key_type)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Display,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use bytes::Bytes;
use futures_util::{
    future::Future,
    stream::{FusedStream, Stream},
    task::Context,
    FutureExt,
};
use ring::digest::{digest, SHA256};
use thiserror::Error;
use timeout::Timeout;
use tokio::sync::oneshot;

use themis_core::{
    app::{self, ApplyCheckpoint, Client, Response},
    authentication::Authenticator,
    comms::Sender,
    config::Config,
    net::{DisplayBytes, Message, NetworkMessage, Raw, Sequenced},
    protocol::{self, Event, Proposal, Protocol, Protocol2, Rejection, Result},
};

use crate::{
    checkpointing::{Checkpointing, CpError},
    config::{ForwardingMode, PBFTConfig, ReplyMode},
    message_log::{Check, Committed, OrderingLog, Prepared},
    messages::{
        Assign, Checkpoint, Commit, Forward, FullCheckpoint, GetCheckpoint, GetProposal,
        GetResponse, PBFTMessage, PrePrepare, Prepare, ViewChange, Viewed,
    },
    metrics::{
        record_high_mark, record_is_primary, record_last_commit, record_low_mark,
        record_next_sequence, record_view_number, view_change_timer, Timer,
    },
    requests::{BatchState, ProposalRef, RequestStore},
};
pub use requests::{Batch, DefaultHasher, ProposalHasher};

#[cfg(test)]
mod test;

mod checkpointing;
pub mod config;
mod message_log;
pub mod messages;
pub mod metrics;
mod requests;
mod timeout;
mod verification;
mod view_change;

#[derive(Debug)]
struct Comms {
    replicas: Sender<PBFTMessage>,
    clients: Sender<Message<Raw<Client>>>,
    app: Sender<app::Command>,
}

impl Comms {
    fn new(
        replicas: Sender<PBFTMessage>,
        clients: Sender<Message<Raw<Client>>>,
        app: Sender<app::Command>,
    ) -> Self {
        Self {
            replicas,
            clients,
            app,
        }
    }

    async fn resolve_checkpoint(&mut self, handle: Bytes) -> themis_core::Result<Bytes> {
        let (tx, rx) = oneshot::channel();
        self.app
            .send(app::Command::ResolveCheckpoint { handle, tx })
            .await?;
        let data = rx
            .await
            .map_err(|e| themis_core::Error::protocol(Box::new(e)))?;
        Ok(data)
    }

    async fn try_apply_checkpoint(
        &mut self,
        sequence: u64,
        quorum: Vec<Message<Checkpoint>>,
        apply: Option<(Bytes, Bytes)>,
    ) -> Result<bool> {
        if let Some((handle, data)) = apply {
            let (tx, rx) = oneshot::channel();
            self.app
                .send(app::Command::CheckpointStable {
                    sequence,
                    quorum,
                    apply: Some(ApplyCheckpoint { handle, data, tx }),
                })
                .await?;
            let result = rx
                .await
                .map_err(|e| themis_core::Error::protocol(Box::new(e)))?;
            Ok(result)
        } else {
            self.app
                .send(app::Command::CheckpointStable {
                    sequence,
                    quorum,
                    apply: None,
                })
                .await?;
            Ok(true)
        }
    }
}

#[derive(Debug)]
enum ViewState {
    Regular,
    ViewChange {
        new_view: u64,
        _span: tracing::Span,
        _timer: Timer,
    },
}

impl ViewState {
    fn view_change(new_view: u64, old_view: u64) -> Self {
        ViewState::ViewChange {
            new_view,
            _span: tracing::trace_span!(target: "__measure::vc", parent: None, "vc", old_view, new_view),
            _timer: view_change_timer(),
        }
    }

    fn is_regular(&self) -> bool {
        matches!(self, ViewState::Regular)
    }

    fn is_view_change(&self) -> bool {
        !self.is_regular()
    }
}

#[derive(Debug)]
pub struct PBFT<RH = DefaultHasher> {
    id: u64,

    low_mark: u64,
    next_sequence: u64,
    last_commit: u64,
    view: u64,
    state: ViewState,

    requests: RequestStore<RH>,
    log: OrderingLog,
    view_changes: BTreeMap<u64, Vec<Message<ViewChange>>>,
    checkpointing: Checkpointing,

    timeout: Timeout,

    comms: Comms,
    pub reorder_buffer: VecDeque<Message<app::Request>>,
    verifier: Box<dyn Authenticator<messages::PBFT> + Send + Sync>,
    config: Arc<PBFTConfig>,

    delay: Option<Pin<Box<tokio::time::Sleep>>>,
}

impl PBFT<DefaultHasher> {
    pub fn new(
        id: u64,
        replicas: Sender<PBFTMessage>,
        clients: Sender<Message<Raw<Client>>>,
        app: Sender<app::Command>,
        verifier: Box<dyn Authenticator<messages::PBFT> + Send + Sync>,
        themis_config: &Config,
    ) -> Self {
        Self::new_with_hasher(
            id,
            replicas,
            clients,
            app,
            verifier,
            themis_config,
            DefaultHasher::default(),
        )
    }
}

impl<RH> PBFT<RH>
where
    RH: ProposalHasher,
{
    pub fn app(&self) -> Sender<app::Command> {
        self.comms.app.clone()
    }

    pub fn is_view_change(&self) -> bool {
        self.state.is_view_change()
    }

    pub fn new_with_hasher(
        id: u64,
        replicas: Sender<PBFTMessage>,
        clients: Sender<Message<Raw<Client>>>,
        app: Sender<app::Command>,
        verifier: Box<dyn Authenticator<messages::PBFT> + Send + Sync>,
        themis_config: &Config,
        rh: RH,
    ) -> Self {
        let pbft_config = Arc::new(PBFTConfig::from_config(&themis_config));

        tracing::info!("{:#?}", *pbft_config);

        //set initial prometheus metric values
        record_low_mark(0);
        record_high_mark(pbft_config.high_mark_delta as u64);
        record_next_sequence(1);
        record_last_commit(0);
        record_view_number(0);
        record_is_primary(id == 0);

        Self {
            id,
            low_mark: 0,
            next_sequence: 1,
            last_commit: 0,
            view: 0,
            state: ViewState::Regular,

            requests: RequestStore::new(rh),
            log: OrderingLog::new(pbft_config.high_mark_delta),
            view_changes: Default::default(),

            checkpointing: Checkpointing::new(
                pbft_config.keep_checkpoints,
                pbft_config.faults,
                pbft_config.num_peers,
            ),
            timeout: Timeout::default(),
            comms: Comms::new(replicas, clients, app),
            reorder_buffer: Default::default(),
            verifier,
            config: pbft_config,

            delay: None,
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn low_mark(&self) -> u64 {
        self.low_mark
    }

    pub fn config(&self) -> &PBFTConfig {
        &self.config
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_known(&self, request: &[u8]) -> bool {
        self.requests.is_known(request)
    }

    pub fn primary(&self) -> u64 {
        self.view % self.config.num_peers
    }

    fn high_mark(&self) -> u64 {
        self.low_mark + (self.config.high_mark_delta as u64)
    }

    fn range_check(&self, sequence: u64) -> std::result::Result<(), Rejection> {
        if self.low_mark < sequence && sequence < self.high_mark() as u64 {
            Ok(())
        } else {
            Err(Rejection::Range {
                actual: sequence,
                expected: self.low_mark..self.high_mark(),
                field: "sequence".into(),
            })
        }
    }

    fn view_check(&self, view: u64) -> std::result::Result<(), Rejection> {
        if self.view == view {
            Ok(())
        } else {
            Err(Rejection::Range {
                expected: self.view..self.view + 1,
                actual: view,
                field: "view".into(),
            })
        }
    }

    fn check<M>(&self, message: &M) -> std::result::Result<(), Rejection>
    where
        M: Sequenced + Viewed,
    {
        self.view_check(message.view())?;
        self.range_check(message.sequence())?;
        Ok(())
    }

    fn check_order_message<M>(&self, message: &M) -> std::result::Result<(), Rejection>
    where
        M: Sequenced + Viewed + Display,
    {
        if self.state.is_view_change() {
            tracing::warn!("{} message rejected due to active view change", message);
            return Err(Rejection::State {
                reason: "active view change".into(),
            });
        }

        self.check(message)?;
        Ok(())
    }

    fn check_pre_prepare(
        &self,
        message: &Message<PrePrepare>,
    ) -> std::result::Result<(), Rejection> {
        // Check sequence window and view
        self.check_order_message(message)?;

        // Check that pre_prepare is really from the primary in the current view
        // Malicious replicas can send pre-prepares for unassigned seqs, so the check below is not sufficient
        if message.source != self.primary() {
            return Err(Rejection::State {
                reason: "message is not from primary".into(),
            });
        }

        // Discard, if we already have a pre-prepare.
        if self.log.has_pre_prepare(message.sequence()).is_some() {
            return Err(Rejection::State {
                reason: "already pre-prepared".into(),
            });
        }
        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self, message), fields(source=message.source,sequence=message.sequence()))]
    pub async fn handle_pre_prepare(&mut self, message: Message<PrePrepare>) -> Result<()> {
        self.check_pre_prepare(&message)?;

        let sequence = message.sequence();
        let request = message.inner.request.clone();
        tracing::trace!("Logging pre-prepare: {}", DisplayBytes(&request));
        self.log
            .pre_prepare(message, self.config.faults as usize * 2 + 1);

        // Don't prepare when we don't have the request
        if !request.is_empty() && !self.requests.has_proposal(&request) {
            if !self.requests.try_assign_proposal(sequence, &request) {
                tracing::debug!("Not preparing because request is missing");
                self.requests
                    .set_proposal_missing(sequence, request.clone());

                // If we are missing the request, request it from the system.
                // TODO make this conditional on config
                if self.config.request_proposals {
                    tracing::debug!("Sending Get for Proposal: {}", DisplayBytes(&request));
                    let get = Message::new(self.id, self.primary(), GetProposal::new(request));
                    self.comms.replicas.send(get.pack()?).await?;
                }

                return Ok(());
            } else {
                tracing::trace!("Request found in local storage.");
            }
        }

        let prepare = Message::broadcast(self.id, Prepare::new(sequence, self.view, request));
        self.comms.replicas.send(prepare.pack()?).await?;
        self.log
            .prepare(prepare, self.config.faults as usize * 2 + 1);

        // edge case, if we somehow received enough prepares before getting the pre-prepare
        // we need to send the commit here
        if let Some(Prepared { pre_prepare, .. }) =
            self.log.get_if_just_prepared(sequence, self.config.votes())
        {
            tracing::debug!("Prepared (after pre-prepare)",);
            let request = pre_prepare.inner.request.clone();
            self.send_commit(sequence, request).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self, message), fields(source=message.source,sequence=message.sequence(),view=message.inner.view))]
    async fn handle_prepare(&mut self, message: Message<Prepare>) -> Result<()> {
        self.check_order_message(&message)?;

        let sequence = message.sequence();
        let missing = !message.inner.request.is_empty()
            && !self.requests.has_proposal(&message.inner.request);

        tracing::trace!("Logging prepare: {}", DisplayBytes(&message.inner.request));
        self.log.prepare(message, self.config.num_peers as usize);
        if let Some(Prepared { pre_prepare, .. }) =
            self.log.get_if_just_prepared(sequence, self.config.votes())
        {
            if missing {
                tracing::trace!("Prepared for unknown request");
            } else {
                tracing::debug!("Prepared {}", DisplayBytes(&pre_prepare.inner.request));
                let request = pre_prepare.inner.request.clone();
                self.send_commit(sequence, request).await?;
            }
        }

        Ok(())
    }

    async fn send_commit(&mut self, sequence: u64, req: Bytes) -> Result<()> {
        self.requests.set_prepared(&req);

        let commit = Message::broadcast(self.id, Commit::new(sequence, self.view, req));
        let commit_se = commit.pack()?;

        self.comms.replicas.send(commit_se).await?;
        self.handle_commit(commit).await?;
        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self, message), fields(source=message.source,sequence=message.sequence(),view=message.inner.view))]
    async fn handle_commit(&mut self, message: Message<Commit>) -> Result<()> {
        if self.state.is_view_change() {
            tracing::warn!("{} message rejected due to active view change", message);
            return Err(Rejection::State {
                reason: "active view change".into(),
            }
            .into());
        }
        self.check(&message)?;

        tracing::trace!("Logging commit: {}", DisplayBytes(&message.inner.request));
        self.log.commit(message, self.config.num_peers as usize);

        self.process_commits().await?;

        Ok(())
    }

    async fn process_commits(&mut self) -> Result<()> {
        loop {
            let last_commit = self.last_commit + 1;
            tracing::trace!(next_commit = last_commit, "committing");

            let pre_prepare = match self.log.get_if_committed(last_commit, self.config.votes()) {
                Some(Committed { pre_prepare, .. }) => pre_prepare,
                _ => {
                    tracing::trace!("not committed");
                    break;
                }
            };
            let sequence = pre_prepare.sequence();

            let empty = pre_prepare.inner.request.is_empty();
            if !empty && !self.requests.has_proposal(&pre_prepare.inner.request) {
                tracing::trace!(
                    "Not committing {} because request is missing",
                    pre_prepare.inner
                );
                return Ok(());
            }

            tracing::debug!("Committed {}", DisplayBytes(&pre_prepare.inner.request));

            let batch = &pre_prepare.inner.request.clone();
            // Empty payload indicates an empty round as a result of a view change
            // Nothing to be done in that case
            if !empty {
                self.requests
                    .execute_requests(batch, &mut self.comms.app)
                    .await?;
            } else {
                tracing::debug!("Empty request for batch {}", sequence);
            }

            self.last_commit = last_commit;
            record_last_commit(self.last_commit);

            //if its time to checkpoint and we have completed at least one full interval, so we don't checkpoint at sequence 0
            if last_commit % self.config.checkpoint_interval == 0 {
                tracing::trace!(
                    "Commit {} hit checkpoint interval, checkpointing ...",
                    last_commit
                );
                let cp_channel = oneshot::channel();
                self.comms
                    .app
                    .send(app::Command::TakeCheckpoint(cp_channel.0))
                    .await?;

                let cp = cp_channel
                    .1
                    .await
                    .map_err(|_| Error::CouldNotGetCheckpoint {
                        sequence: last_commit,
                    })?;
                self.handle_app_checkpoint(cp).await?;
            }

            // We made progress, reset timeout
            self.timeout.clear();
            self.set_timeout();
        }
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self, requests))]
    pub async fn handle_new_proposal(&mut self, mut requests: Proposal) -> Result<()> {
        if self.state.is_view_change() {
            return Err(Rejection::State {
                reason: "view change".into(),
            }
            .into());
        }

        match &mut requests {
            Proposal::Batch(requests) => {
                requests.retain(|request| {
                    if self.requests.is_duplicate(request) {
                        tracing::trace!(
                            "Request {} from {} discarded as duplicate",
                            request.sequence(),
                            request.source
                        );
                        false
                    } else {
                        true
                    }
                });
                if requests.is_empty() {
                    Err(Rejection::Duplicate)?;
                }
            }
            Proposal::Single(request) => {
                if self.requests.is_duplicate(request) {
                    Err(Rejection::Duplicate)?;
                }
            }
        };

        if self.is_primary() {
            let sequence = self.next_sequence;

            if self.view == self.config.debug_force_vc.in_view
                && sequence == self.config.debug_force_vc.at_seq
            {
                tracing::error!(target: "__measure::vc", parent: None, "force");

                return Ok(());
            }

            self.next_sequence += 1;
            record_next_sequence(self.next_sequence);

            tracing::debug!(
                "new batch of {}, assigning sequence {}",
                requests.len(),
                sequence
            );

            let hash = RH::hash_proposal(&requests);
            self.store_requests(&requests, hash.clone(), sequence);

            if let ForwardingMode::Full = self.config.primary_forwarding {
                let forward = Assign::new(sequence, requests);
                let forward = Message::broadcast(self.id, forward);
                self.comms.replicas.send(forward.pack()?).await?;
            }

            let pre_prepare =
                Message::broadcast(self.id, PrePrepare::new(sequence, self.view, hash));
            self.comms.replicas.send(pre_prepare.pack()?).await?;

            self.log
                .pre_prepare(pre_prepare, self.config.num_peers as usize);

            Ok(())
        } else {
            for request in &requests {
                let digest = RH::hash_request(&request);
                if let Some(sequence) = self.requests.add_request(request.clone(), None) {
                    let prepare = Message::broadcast(
                        self.id,
                        Prepare::new(sequence, self.view, digest.clone()),
                    );
                    self.comms.replicas.send(prepare.pack()?).await?;
                    self.log.prepare(prepare, self.config.num_peers as usize);

                    if let Some(Prepared { pre_prepare, .. }) =
                        self.log.get_if_just_prepared(sequence, self.config.votes())
                    {
                        tracing::debug!(
                            "Prepared for {} in view {} (after assign)",
                            sequence,
                            self.view
                        );
                        let request = pre_prepare.inner.request.clone();
                        self.send_commit(sequence, request).await?;
                    }
                }
            }

            if let ForwardingMode::Full = self.config.backup_forwarding {
                tracing::trace!("forwarding requests to primary {}", self.primary());
                // If we have forwarding, we send all the requests to the primary
                let forward = Forward(requests);
                let forward = Message::new(self.id, self.primary(), forward);
                self.comms.replicas.send(forward.pack()?).await?;
            }

            if self.state.is_regular() {
                self.set_timeout();
            }

            Ok(())
        }
    }

    #[tracing::instrument(level="debug", skip(self,message), fields(source=message.source))]
    async fn handle_forward(&mut self, message: Message<Forward>) -> Result<()> {
        if self.state.is_view_change() {
            tracing::trace!("In view change discarding forward message");
            Err(Rejection::State {
                reason: "view change".into(),
            })?;
        }

        //if primary we will order the request
        if self.is_primary() {
            match self.config.primary_forwarding {
                ForwardingMode::Full => {
                    tracing::trace!("Adding {} forwarded requests", message.inner.0.len());
                    self.reorder_buffer.extend(message.inner.0.into_iter());
                }
                _ => tracing::warn!("{:?}", self.config.primary_forwarding),
            }
        }
        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self,message), fields(source=message.source, sequence=message.sequence()))]
    async fn handle_checkpoint(&mut self, message: Message<Checkpoint>) -> Result<()> {
        let sequence = message.sequence();
        self.range_check(sequence)?;

        // clean log, if stable che  let cp_channel = oneshot::channel();
        //                self.comms.app.send(app::Command::Checkpoint(cp_channel.0)).await?;
        //                let cp = cp_channel.1.await.map_err(|_| {
        //                    themis_core::Error::protocol("Failed to get checkpoint from Application".to_owned())
        //                })?;
        //                await!ckpoint
        // TODO do we checkpoint even if our own checkpoint message isn't there yet?
        // Option: do checkpoint, abandon all outstanding instances. This would trigger recovery
        // Option: wait until own checkpoint is triggered. What to do if own checkpoint doesn't match majority? Recover..
        tracing::trace!(
            "Logging checkpoint: {}",
            DisplayBytes(&message.inner.digest)
        );
        match self.checkpointing.offer_proof(message) {
            Ok(true) => {
                self.checkpoint_stable(sequence).await?;
                self.process_commits().await?;
            }
            Ok(false) => (),
            Err(CpError::CheckpointMismatch { to_ask }) => {
                let get = Message::new(self.id, to_ask, GetCheckpoint::new(sequence));
                let get = get.pack()?;

                self.comms.replicas.send(get).await?;
            }
            Err(e) => tracing::warn!("Checkpoint {} error: {:?}", sequence, e),
        }

        Ok(())
    }

    async fn checkpoint_stable(&mut self, stable: u64) -> Result<()> {
        tracing::warn!(
            "checkpoint at {} stable. Last commit: {:?}",
            stable,
            self.last_commit
        );

        if let Some(own_proof) = self.checkpointing.get_own_stable(stable) {
            self.comms
                .try_apply_checkpoint(stable, own_proof.to_owned(), None)
                .await?;
        }

        for pre_prepare in self.log.checkpoint(stable) {
            self.requests.remove(&pre_prepare.inner.request);
        }

        if stable > self.last_commit {
            self.last_commit = stable;
            record_last_commit(self.last_commit);
        }
        // Next sequence number has to be after checkpoint
        if stable >= self.next_sequence {
            self.next_sequence = stable + 1;
            record_next_sequence(self.next_sequence);
        }

        self.checkpointing.clean_up(stable);
        self.low_mark = stable;

        record_low_mark(self.low_mark);
        record_high_mark(self.high_mark());

        Ok(())
    }

    async fn handle_get_checkpoint(&mut self, message: Message<GetCheckpoint>) -> Result<()> {
        let msg = match self.checkpointing.get_data(message.sequence()) {
            Ok(cp) => {
                let data = self.comms.resolve_checkpoint(cp.handle().clone()).await?;
                FullCheckpoint::new(message.sequence(), cp.handle().clone(), data)
            }
            Err(_) => FullCheckpoint::empty(message.sequence()),
        }
        .pack()?;
        let msg = Message::new(self.id, message.source, msg);
        self.comms.replicas.send(msg).await?;
        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self,message), fields(source=message.source, sequence=message.sequence()))]
    async fn handle_full_checkpoint(&mut self, message: Message<FullCheckpoint>) -> Result<()> {
        let sequence = message.sequence();
        match self
            .checkpointing
            .offer_full_checkpoint(message, &mut self.comms)
            .await
        {
            Ok(()) => {
                tracing::trace!("Loggin full checkpoint.");
                self.checkpoint_stable(sequence).await?;

                // if we apply a foreign checkpoint, we need to replay commits on top
                if sequence < self.last_commit {
                    self.last_commit = sequence;
                    record_last_commit(self.last_commit);
                    self.process_commits().await?;
                }
            }
            Err(CpError::CheckpointMismatch { to_ask }) => {
                let get_cp = Message::new(self.id, to_ask, GetCheckpoint::new(sequence));
                self.comms.replicas.send(get_cp.pack()?).await?;
            }
            Err(e) => {
                tracing::warn!("Full Checkpoint for sequence {}: {}", sequence, e);
            }
        }
        Ok(())
    }

    fn add_if_not_present(
        &mut self,
        request: Message<app::Request>,
        digest: Bytes,
        _sequence: Option<u64>,
    ) {
        if !self.requests.has_request(&digest) {
            self.requests.add_request(request, None);
        }
    }

    fn store_requests(&mut self, proposal: &Proposal, digest: Bytes, sequence: u64) {
        let proposal_ref = match proposal {
            Proposal::Batch(batch) => {
                let mut proposal_ref = Vec::new();
                for request in batch {
                    let digest = RH::hash_request(request);
                    proposal_ref.push(digest.clone());
                    self.add_if_not_present(request.clone(), digest, Some(sequence));
                }
                ProposalRef::Batch(proposal_ref)
            }
            Proposal::Single(request) => {
                let digest = RH::hash_request(request);
                self.add_if_not_present(request.clone(), digest.clone(), Some(sequence));
                ProposalRef::Single(digest)
            }
        };

        self.requests
            .assign_proposal(sequence, proposal_ref, digest);
    }

    #[tracing::instrument(level="debug", skip(self, message), fields(source=message.source,sequence=message.sequence()))]
    pub async fn handle_assign(&mut self, message: Message<Assign>) -> Result<()> {
        if message.source != self.primary() {
            tracing::trace!("Rejecting assign from backup replica");
            Err(Rejection::Structure {
                source: anyhow::anyhow!("Rejecting assign from backup replica"),
            })?;
        }

        let Assign { sequence, batch } = message.inner;

        match self.config.primary_forwarding {
            ForwardingMode::Full => {
                let hash = RH::hash_proposal(&batch);
                let missing = if let Some(Batch {
                    state: BatchState::Missing,
                    ..
                }) = self.requests.get_proposal(&hash)
                {
                    true
                } else {
                    false
                };

                tracing::trace!("Logging assign");
                self.store_requests(&batch, hash.clone(), sequence);

                if missing {
                    let prepare = Message::broadcast(
                        self.id,
                        Prepare::new(sequence, self.view, hash.clone()),
                    );
                    self.comms.replicas.send(prepare.pack()?).await?;
                    self.log.prepare(prepare, self.config.num_peers as usize);

                    if self.log.check_prepared(sequence, self.config.votes()) >= Check::Changed {
                        tracing::debug!("Prepared after assign",);
                        self.send_commit(sequence, hash).await?;
                    }
                }
            }
            _ => tracing::warn!(
                "Received Forward message but {:?}",
                self.config.backup_forwarding
            ),
        }

        Ok(())
    }

    async fn handle_get_proposal(&mut self, message: Message<GetProposal>) -> Result<()> {
        let digest = message.inner.digest;

        if let Some((_, proposal)) = self.requests.build_proposal(&digest) {
            let forward = Message::new(self.id, message.source, GetResponse::new(proposal));
            self.comms.replicas.send(forward.pack()?).await?;
        }

        Ok(())
    }

    #[tracing::instrument(level="debug", skip(self, message), fields(source=message.source))]
    async fn handle_get_response(&mut self, message: Message<GetResponse>) -> Result<()> {
        let digest = RH::hash_proposal(&message.inner.proposal);
        let proposal = self.requests.get_proposal(&digest);

        if let Some(proposal) = proposal {
            let sequence = proposal.sequence;
            if BatchState::Missing == proposal.state {
                let sequence = proposal.sequence;
                tracing::trace!("Storing proposal {}", DisplayBytes(&digest));
                self.store_requests(&message.inner.proposal, digest.clone(), sequence);
            }

            if self.log.check_prepared(sequence, self.config.votes()) >= Check::Changed {
                tracing::trace!("Prepared after get response");
                self.requests.set_prepared(&digest);

                let prepare =
                    Message::broadcast(self.id, Prepare::new(sequence, self.view, digest.clone()));
                self.comms.replicas.send(prepare.pack()?).await?;

                self.send_commit(sequence, digest.clone()).await?;
            }
        }

        Ok(())
    }

    pub fn view(&self) -> u64 {
        self.view
    }

    #[tracing::instrument(level="debug", skip(self,message),fields(source=message.source,tag=?message.inner.tag))]
    pub async fn on_message(&mut self, message: PBFTMessage) -> Result<()> {
        use messages::PBFTTag;
        use themis_core::protocol::ProtocolTag;

        macro_rules! match_pat {
            (CHECKPOINT) => {
                ProtocolTag::Checkpoint
            };
            ($tag:ident) => {
                ProtocolTag::Protocol(PBFTTag::$tag)
            };
        }

        macro_rules! dispatch {
            ($($tag:tt => $function:tt,)+) => {
                match message.inner.tag {
                    $(match_pat!($tag) => self.$function(message.unpack()?).await),+
                }
            };
        }

        dispatch! {
            // request management
            FORWARD => handle_forward,
            GET_PROPOSAL => handle_get_proposal,
            GET_RESPONSE => handle_get_response,

            // checkpointing
            CHECKPOINT => handle_checkpoint,
            GET_CHECKPOINT => handle_get_checkpoint,
            FULL_CHECKPOINT => handle_full_checkpoint,

            // view change
            VIEW_CHANGE => handle_view_change,
            NEW_VIEW => handle_new_view,
            // ordering
            PRE_PREPARE => handle_pre_prepare,
            ASSIGN => handle_assign,
            PREPARE => handle_prepare,
            COMMIT => handle_commit,

        }
    }

    /// Handles outgoing messages generated by an application
    /// In this case, do nothing, just send to client
    #[tracing::instrument(level="debug", skip(self,response),fields(client=response.destination,sequence=response.sequence()))]
    pub async fn on_response(&mut self, mut response: Message<Response>) -> Result<()> {
        if let ReplyMode::None = self.config.reply_mode {
            return Ok(());
        }

        tracing::trace!("response");

        let primary = self.primary();
        response.inner.contact = if response.inner.contact == Some(primary) {
            None
        } else {
            tracing::trace!(primary, "new primary");
            Some(primary)
        };

        self.comms.clients.send(response.pack()?).await?;
        Ok(())
    }

    /// Timeout, waiting too long on quorum
    /// Initiate view change
    #[tracing::instrument(level = "debug", skip(self,timeout), fields(timeout=format_args!("{}", timeout)))]
    pub async fn on_timeout(&mut self, timeout: timeout::Info) -> Result<()> {
        if let timeout::Info::Spin = timeout {
            return Ok(())
        }

        if let timeout::Info::ViewChange { to } = timeout {
            // don't handle view change timeout when we are not in a view change for that view
            if !matches!(self.state, ViewState::ViewChange { new_view, ..} if new_view == to) {
                return Ok(());
            }
        }

        tracing::info!(target: "__measure::timeout", parent: None, reason=%timeout, "timeout");

        let new_view = match &mut self.state {
            ViewState::Regular => {
                self.state = ViewState::view_change(self.view + 1, self.view);
                self.view + 1
            }
            ViewState::ViewChange { new_view, .. } => {
                *new_view += 1;
                *new_view
            }
        };
        tracing::debug!("Timed out. Sending view change for view {}", new_view);

        let vc_msg = self.compute_view_change(new_view);
        let vc_msg = Message::broadcast(self.id, vc_msg);

        self.comms.replicas.send(vc_msg.pack()?).await?;
        self.handle_view_change(vc_msg).await?;
        Ok(())
    }

    async fn handle_app_checkpoint(&mut self, checkpoint: Bytes) -> Result<()> {
        tracing::trace!("Handling local checkpoint");
        let digest = digest(&SHA256, &checkpoint);
        let cp_message = Message::broadcast(
            self.id,
            Checkpoint::new(self.last_commit, Bytes::copy_from_slice(digest.as_ref())),
        );

        self.comms.replicas.send(cp_message.pack()?).await?;
        // self.handle_checkpoint(cp_message).await?;

        match self
            .checkpointing
            .new_checkpoint(self.last_commit, checkpoint, cp_message)
        {
            Ok(true) => self.checkpoint_stable(self.last_commit).await?,
            Ok(false) => (),
            Err(CpError::CheckpointMismatch { to_ask }) => {
                let get_cp = Message::new(self.id, to_ask, GetCheckpoint::new(self.last_commit));
                self.comms.replicas.send(get_cp.pack()?).await?;
            }
            Err(e) => {
                tracing::warn!("Checkpoint {}: {:?}", self.last_commit, e);
            }
        };

        Ok(())
    }

    /// check if I am leader, i.e. my id == leader id
    pub fn is_primary(&self) -> bool {
        self.id == self.primary()
    }

    fn set_timeout(&mut self) {
        if self.is_primary() {
            // no timeouts on primary
            return;
        }

        if self.timeout.is_running() {
            return;
        }

        let waiting_for = self.last_commit + 1;
        if let Some(digest) = self.log.has_pre_prepare(waiting_for) {
            self.timeout = Timeout::instance(
                Duration::from_millis(self.config.request_timeout),
                digest.clone(),
                waiting_for,
            );
            return;
        }

        if let Some(digest) = self.requests.has_unassigned() {
            self.timeout = Timeout::request(
                Duration::from_millis(self.config.request_timeout),
                digest.clone(),
            )
        }
    }
}

impl<'f> Protocol2<'f> for PBFT {
    type OnMessage = impl Future<Output = Result<()>> + Send + 'f;
    fn on_message(&'f mut self, message: PBFTMessage) -> Self::OnMessage {
        self.on_message(message)
    }

    type OnRequest = impl Future<Output = Result<()>> + Send + 'f;
    fn on_request(&'f mut self, requests: Proposal) -> Self::OnRequest {
        self.handle_new_proposal(requests)
    }

    fn can_accept_requests(&self) -> bool {
        if self.delay.is_some() {
            return false;
        }

        self.next_sequence < self.high_mark()
    }

    type OnEvent = impl Future<Output = Result<()>> + Send + 'f;
    fn on_event(&'f mut self, event: Self::Event) -> Self::OnEvent {
        self.on_timeout(event)
    }

    type OnResponse = impl Future<Output = Result<()>> + Send + 'f;
    fn on_response(&'f mut self, message: Message<Response>) -> Self::OnResponse {
        self.on_response(message)
    }
}

impl<RH> Protocol for PBFT<RH> {
    type Messages = messages::PBFT;
    type Event = timeout::Info;
}

impl<RH> Stream for PBFT<RH>
where
    RH: ProposalHasher + Unpin,
{
    type Item = Event<<PBFT<RH> as Protocol>::Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(item) = self.timeout.poll_unpin(cx) {
            return Poll::Ready(Some(Event::Protocol(item)));
        } else if !self.reorder_buffer.is_empty() {
            // let buffer = std::mem::take(&mut self.reorder_buffer);
            // let proposal = buffer.try_into().unwrap_or_default();
            // return Poll::Ready(Some(Event::Reorder(proposal)));
            if let Some(request) = self.reorder_buffer.pop_front() {
                return Poll::Ready(Some(Event::Reorder(Proposal::Single(request))));
            }
        }
        Poll::Pending
    }
}

impl<RH> FusedStream for PBFT<RH>
where
    RH: ProposalHasher + Unpin,
{
    fn is_terminated(&self) -> bool {
        false
    }
}

#[derive(Debug, Error)]
enum Error {
    #[error("Could not get Checkpoint")]
    CouldNotGetCheckpoint { sequence: u64 },
}

impl From<Error> for protocol::Error {
    fn from(e: Error) -> Self {
        protocol::Error::abort(e)
    }
}
