use crate::{
    message_log::Prepared,
    messages::*,
    metrics::{record_is_primary, record_next_sequence, record_view_number, view_change_timer},
    requests::ProposalHasher,
    timeout::Timeout,
    verification::{verify_new_view, verify_view_change},
    ViewState, PBFT,
};
use bytes::Bytes;
use std::{cmp::Ord, collections::BTreeMap, time::Duration};
use themis_core::{
    authentication::AuthenticatorExt,
    net::{Message, Sequenced},
    protocol::{Match, Rejection, Result},
};

impl<RH> PBFT<RH>
where
    RH: ProposalHasher,
{
    pub(super) fn compute_view_change(&self, new_view: u64) -> ViewChange {
        let mut checkpoint_proof = Vec::new();
        let mut prepare_proof = Vec::new();

        if let Some(checkpoints) = self.checkpointing.get_proof(self.low_mark) {
            // if we view change before the first checkpoint, this stays empty
            checkpoint_proof.extend_from_slice(checkpoints)
        }

        for Prepared {
            pre_prepare,
            prepares,
        } in self.log.prepared_instances()
        {
            prepare_proof.push(PrepareProof(
                pre_prepare.clone(),
                prepares.to_owned().into_boxed_slice(),
            ));
        }

        ViewChange::new(
            new_view,
            self.low_mark,
            checkpoint_proof.into(),
            prepare_proof.into(),
        )
    }

    fn primary_for(&self, view: u64) -> u64 {
        view % self.config.num_peers
    }

    async fn compute_new_view(
        &mut self,
        msgs: Box<[Message<ViewChange>]>,
    ) -> Result<Message<NewView>> {
        let new_view = msgs[0].view();
        // latest checkpoint
        let latest_checkpoint = msgs.iter().max_by_key(|vc| vc.inner.checkpoint);
        let mut min_s = 0;
        // there might be no checkpoints, if we view change before ever checkpointing
        if let Some(latest_checkpoint) = latest_checkpoint {
            min_s = latest_checkpoint.inner.checkpoint;
            // if our checkpoint is smaller than included in view changes, we log the checkpoint proof
            // and clean up accordingly
            if self.low_mark < min_s {
                for c in latest_checkpoint.inner.checkpoint_proof.iter().cloned() {
                    let _ = self.checkpointing.offer_proof(c);
                }
                self.checkpoint_stable(min_s).await?;
            }
        }

        // highest request that is prepared somewhere
        let max_s = msgs
            .iter()
            .flat_map(|vc| vc.inner.prepares.iter())
            .map(|p| p.0.sequence())
            .filter(|&s| s > min_s)
            .max()
            .unwrap_or(min_s);
        let mut pre_preps = Vec::with_capacity((max_s - min_s) as usize);

        // for each sequence number broadcast pre-prepare with new view, if it is prepared somewhere
        // otherwise put null request
        // Paper says we should check all Prepares for the sequence number, but for a view-change message to be valid,
        // the prepares have to match the pre-prepare, so we only check those
        for n in min_s + 1..=max_s {
            let mut prepares = msgs.iter().flat_map(|vc| vc.inner.prepares.iter());
            if let Some(proof) = prepares.find(|p| p.0.sequence() == n) {
                pre_preps.push(Message::broadcast(
                    self.id,
                    PrePrepare::new(n, new_view, proof.0.inner.request.clone()),
                ))
            } else {
                let pre_prepare =
                    Message::broadcast(self.id, PrePrepare::new(n, new_view, Bytes::new()));
                pre_preps.push(pre_prepare);
            }
        }

        Ok(Message::broadcast(
            self.id,
            NewView::new(msgs[0].view(), msgs, pre_preps.into()),
        ))
    }

    pub(super) async fn handle_new_view(&mut self, message: Message<NewView>) -> Result<()> {
        if let ViewState::ViewChange { new_view, .. } = self.state {
            if message.view() < new_view {
                tracing::debug!("Rejecting NewView for abandoned view");
                Err(Rejection::State {
                    reason: "abandoned view".into(),
                })?;
            }
        }

        if message.source != self.id {
            if let Err(err) = verify_new_view(self.verifier.as_mut(), &message) {
                tracing::warn!("Rejecting NewView. {}", err);
                // println!("{:?}", err);
                Err(Rejection::Authenticator {
                    sender: message.source,
                    source: err.into(),
                })?;
            }
        }

        let new_view = message.view();
        let stable_checkpoint = message
            .inner
            .view_changes
            .iter()
            .map(|vc| vc.inner.checkpoint)
            .max()
            .unwrap_or(0);
        // verify pre-prepare computation done by new primary
        let my_new_view = self.compute_new_view(message.inner.view_changes).await?;
        // resulting vec have to be same length, and all pre-prepares must match
        let my_count = my_new_view.inner.pre_prepares.len();
        let their_count = message.inner.pre_prepares.len();
        if my_count != their_count {
            tracing::warn!(
                "Rejecting NewView. Invalid pre-prepare set. {:?}",
                my_new_view
            );
            Err(Rejection::Structure {
                source: anyhow::anyhow!("invalid pre-prepare set"),
            })?;
        }
        let my_new_view_different = my_new_view
            .inner
            .pre_prepares
            .iter()
            .zip(message.inner.pre_prepares.iter())
            .any(|(mine, received)| {
                if !mine.matches(received) {
                    tracing::trace!("{:?}", mine);
                    tracing::trace!("does not match");
                    tracing::trace!("{:?}", received);
                    true
                } else {
                    false
                }
            });
        if my_new_view_different {
            tracing::warn!("Rejecting NewView. Invalid pre-prepare set.");
            Err(Rejection::Structure {
                source: anyhow::anyhow!("invalid pre-prepare set"),
            })?;
        }

        self.next_sequence = message
            .inner
            .pre_prepares
            .last()
            .map_or(stable_checkpoint, Sequenced::sequence)
            + 1;
        record_next_sequence(self.next_sequence);

        self.state = ViewState::Regular;
        self.view = new_view;

        record_view_number(self.view);
        record_is_primary(self.is_primary());

        self.log
            .advance_view(self.low_mark, self.config.high_mark_delta);
        tracing::debug!("Entered View {}", new_view);
        tracing::debug!(
            "View {} has {} pre-prepares",
            new_view,
            message.inner.pre_prepares.len()
        );
        tracing::trace!(
            target: "__measure::vc", parent: None,
            prepares=message.inner.pre_prepares.len(),
            next_sequence=self.next_sequence, "nv"
        );

        let mut unassigned_requests = self.requests.reset_request_store(
            message
                .inner
                .pre_prepares
                .iter()
                .map(|pp| &pp.inner.request),
        );

        for pre_prepare in message.inner.pre_prepares.into_vec() {
            if self.is_primary() {
                // if we are primary, log the pre_prepares
                self.log
                    .pre_prepare(pre_prepare, self.config.num_peers as usize);
            } else {
                // if backup, follow protocol for each pre_prepare in new view
                self.handle_pre_prepare(pre_prepare).await?;
            }
        }

        let is_primary = self.is_primary();
        // if we are primary in the new view, we send new pre-prepares for known requests.
        // As only prepared reqeusts are included in the view change messages, these request would otherwise be forgotten.
        // If we are not the primary, we forget about these. It is assumed that either the new primary knows about the request or the client will eventuall re-send its request.
        if is_primary {
            unassigned_requests.sort_unstable_by(|(_, req1), (_, req2)| {
                Ord::cmp(&req1.sequence(), &req2.sequence())
            });
            self.reorder_buffer
                .extend(unassigned_requests.into_iter().map(|r| r.1));
            self.timeout.clear();
        } else {
            self.timeout.clear();
        }
        // self.set_timeout();

        Ok(())
    }

    pub async fn handle_view_change(&mut self, message: Message<ViewChange>) -> Result<()> {
        if let ViewState::ViewChange { new_view, .. } = self.state {
            if message.view() < new_view {
                tracing::debug!("Rejecting ViewChange for abandoned view");
                Err(Rejection::State {
                    reason: "abandoned view".into(),
                })?;
            }
        }

        // if message is from another replica, we need to verify its contents
        // #![feature(let_chains)] :/
        if message.source != self.id {
            if let Err(err) = verify_view_change(self.verifier.as_mut(), &message) {
                tracing::warn!("Rejecting ViewChange.\n{}", err);
                Err(Rejection::Authenticator {
                    sender: message.source,
                    source: err.into(),
                })?;
            }
        }

        let new_view = message.view();
        let new_primary = self.primary_for(new_view);
        let faults = self.config.faults as usize;
        let entry = self.view_changes.entry(new_view).or_insert_with(Vec::new);

        entry.push(message);

        tracing::trace!(
            "currently {} vc messages for view {}",
            entry.len(),
            new_view
        );

        let current_view = match self.state {
            ViewState::Regular => self.view,
            ViewState::ViewChange { new_view: v, .. } => v,
        };

        let num_msgs = entry.len();

        let completed =
            if self.id == new_primary && current_view < new_view && entry.len() == faults * 2 {
                // Special case: if we are primary and there are 2f but haven't send a view change message, we need to generate one
                let local_vc = self.compute_view_change(new_view);
                let local_vc = Message::broadcast(self.id, local_vc);
                //get log entry again to please evil borrow checker
                self.view_changes.get_mut(&new_view).unwrap().push(local_vc);
                self.state = ViewState::view_change(new_view, self.view);
                self.timeout.clear();
                true
            } else {
                num_msgs > faults * 2
            };

        if completed {
            if self.id == new_primary {
                // if we are the new primary, send new view
                self.on_view_changed(new_view).await?;
            } else {
                // if we are not the new primary but have 2f+1 view change messages,
                // we expect a new view soon and start the timeout.

                self.timeout = Timeout::view_change(
                    Duration::from_millis(3 * self.config.request_timeout),
                    new_view,
                );
            }
        }

        // Special case: If there are f+1 view change messages for views we have not entered or send a view change for,
        // We send a view change for the smallest view number in the set
        if let Some(smallest) = greater_views(current_view, &self.view_changes, faults) {
            tracing::debug!(
                "participating in view change to {} due to peer pressure",
                smallest
            );
            self.send_view_change(smallest).await?;

            // this can push us over the threshold for f = 1#
            let entry = self.view_changes.entry(smallest).or_default();
            if entry.len() == faults + 1 && self.id == self.primary_for(smallest) {
                self.on_view_changed(smallest).await?;
            }
        }

        Ok(())
    }

    async fn send_view_change(&mut self, view: u64) -> Result<()> {
        self.timeout.clear();
        self.state = ViewState::ViewChange {
            new_view: view,
            _span: tracing::trace_span!(target: "__measure::vc", parent: None, "vc", old_view=self.view, new_view=view),
            _timer: view_change_timer(),
        };

        let vc = self.compute_view_change(view);
        let vc = Message::broadcast(self.id, vc);
        self.comms.replicas.send(vc.pack()?).await?;

        self.view_changes.entry(view).or_default().push(vc);

        Ok(())
    }

    async fn on_view_changed(&mut self, new_view: u64) -> Result<()> {
        // if we are primary of new view, send new_view message
        let entry = self
            .view_changes
            .remove(&new_view)
            .expect("remove completed view");

        let mut new_view = self.compute_new_view(entry.into()).await?;
        for pre_prepare in new_view.inner.pre_prepares.iter_mut() {
            self.verifier.sign_unpacked(pre_prepare)?;
        }
        let new_view_se = new_view.pack()?;

        self.comms.replicas.send(new_view_se).await?;
        self.handle_new_view(new_view).await?;

        Ok(())
    }
}

fn greater_views(
    current_view: u64,
    views: &BTreeMap<u64, Vec<Message<ViewChange>>>,
    f: usize,
) -> Option<u64> {
    let mut greater = views.range(current_view + 1..);

    // we're not yielding while accessing this, so it's fine
    thread_local! {
        static MEMORY: std::cell::RefCell<fnv::FnvHashSet<u64>> = Default::default();
    }

    fn reset() {
        MEMORY.with(|memory| {
            memory.borrow_mut().clear();
        })
    }

    fn count(source: u64) -> bool {
        MEMORY.with(|memory| {
            let mut memory = memory.borrow_mut();
            if memory.contains(&source) {
                false
            } else {
                memory.insert(source);
                true
            }
        })
    }

    // Count view change message greater than our current view
    // Multiple message from one replica don't count. We use MEMORY to remember.
    let count = greater
        .clone()
        .flat_map(|(_, vcs)| vcs.iter())
        .filter(|vc| count(vc.source))
        .count();
    reset();

    // If we have more than f+1, we participate
    if count > f + 1 {
        let smallest_view = greater.next().unwrap().0; //cannot be empty, we just counted
        Some(*smallest_view)
    } else {
        None
    }
}
