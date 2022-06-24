use crate::messages::*;
use bytes::Bytes;
use std::mem;
use themis_core::{
    net::{Message, Sequenced},
    protocol::Slots,
};

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum OrderingState {
    Open,
    Prepared,
    Committed,
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
pub enum Check {
    False,
    Changed,
    True,
}

#[derive(Debug)]
pub struct OrderInstance {
    pre_prepare: Option<Message<PrePrepare>>,
    prepares: Vec<Message<Prepare>>,
    commits: Vec<Message<Commit>>,
    state: OrderingState,
}

impl OrderInstance {
    pub fn new(capacity: usize) -> Self {
        Self {
            pre_prepare: None,
            prepares: Vec::with_capacity(capacity - 1),
            commits: Vec::with_capacity(capacity),
            state: OrderingState::Open,
        }
    }

    pub fn is_prepared(&mut self, need_votes: usize) -> Check {
        use themis_core::protocol::Match;

        if self.state >= OrderingState::Prepared {
            Check::True
        } else if let Some(pre_prepare) = &self.pre_prepare {
            let votes = self
                .prepares
                .iter()
                .filter(|prepare| prepare.matches(pre_prepare))
                .take(need_votes - 1)
                .count();

            if (votes + 1) >= need_votes {
                self.state = OrderingState::Prepared;
                Check::Changed
            } else {
                Check::False
            }
        } else {
            Check::False
        }
    }

    pub fn is_committed(&mut self, need_votes: usize) -> Check {
        use themis_core::protocol::Match;
        if self.state == OrderingState::Committed {
            Check::True
        } else if let Some(pre_prepare) = &self.pre_prepare {
            let votes = self
                .commits
                .iter()
                .filter(|commit| commit.matches(pre_prepare))
                .take(need_votes)
                .count();

            if votes >= need_votes {
                self.state = OrderingState::Committed;
                Check::Changed
            } else {
                Check::False
            }
        } else {
            Check::False
        }
    }

    pub fn insert_pre_prepare(&mut self, pre_prepare: Message<PrePrepare>) {
        assert!(self.pre_prepare.is_none());

        self.pre_prepare = Some(pre_prepare);
    }

    pub fn insert_prepare(&mut self, prepare: Message<Prepare>) {
        if self.prepares.iter().all(|p| p.source != prepare.source) {
            self.prepares.push(prepare);
        }
    }

    pub fn insert_commit(&mut self, commit: Message<Commit>) {
        if self.commits.iter().all(|p| p.source != commit.source) {
            self.commits.push(commit);
        }
    }
}

#[derive(Debug)]
pub struct OrderingLog {
    current_view: Slots<OrderInstance>,
    old_views: Vec<Slots<OrderInstance>>,
}

impl OrderingLog {
    pub fn new(order_capacity: usize) -> Self {
        Self {
            current_view: Slots::with_first(1, order_capacity),
            old_views: Vec::new(),
        }
    }

    pub fn advance_view(&mut self, low_mark: u64, order_capacity: usize) {
        let old_view = mem::replace(
            &mut self.current_view,
            Slots::with_first(low_mark, order_capacity),
        );
        self.old_views.push(old_view);
    }

    pub fn checkpoint(
        &mut self,
        stable_instance: u64,
    ) -> impl Iterator<Item = Message<PrePrepare>> + '_ {
        self.old_views.clear();
        self.current_view
            .advance_iter(stable_instance - self.current_view.first_idx() + 1)
            .filter_map(|instance| instance.pre_prepare)
    }

    pub fn pre_prepare(&mut self, message: Message<PrePrepare>, capacity: usize) {
        let instance = self
            .current_view
            .get_or_insert_with(message.sequence(), || OrderInstance::new(capacity));
        if let Some(instance) = instance {
            instance.insert_pre_prepare(message);
        }
    }

    pub fn prepare(&mut self, message: Message<Prepare>, capacity: usize) {
        let instance = self
            .current_view
            .get_or_insert_with(message.sequence(), || OrderInstance::new(capacity));
        if let Some(instance) = instance {
            instance.insert_prepare(message);
        }
    }

    pub fn commit(&mut self, message: Message<Commit>, capacity: usize) {
        let instance = self
            .current_view
            .get_or_insert_with(message.sequence(), || OrderInstance::new(capacity));
        if let Some(instance) = instance {
            instance.insert_commit(message);
        }
    }

    pub fn check_prepared(&mut self, sequence: u64, votes: usize) -> Check {
        let instance = self.current_view.get_mut(sequence);
        instance
            .map(|i| i.is_prepared(votes))
            .unwrap_or(Check::False)
    }

    #[allow(unused)]
    pub fn check_committed(&mut self, sequence: u64, votes: usize) -> Check {
        let instance = self.current_view.get_mut(sequence);
        instance
            .map(|i| i.is_committed(votes))
            .unwrap_or(Check::False)
    }

    pub fn has_pre_prepare(&self, sequence: u64) -> Option<&Bytes> {
        let instance = self.current_view.get(sequence);
        instance
            .and_then(|i| i.pre_prepare.as_ref())
            .map(|p| &p.inner.request)
    }

    #[allow(unused)]
    pub fn get(&self, sequence: u64) -> Option<&OrderInstance> {
        self.current_view.get(sequence)
    }

    pub fn get_mut(&mut self, sequence: u64) -> Option<&mut OrderInstance> {
        self.current_view.get_mut(sequence)
    }

    pub fn get_if_just_prepared(&mut self, sequence: u64, votes: usize) -> Option<Prepared<'_>> {
        match self.get_mut(sequence) {
            Some(instance) => {
                if instance.is_prepared(votes) == Check::Changed {
                    Some(Prepared {
                        pre_prepare: instance.pre_prepare.as_ref().unwrap(),
                        prepares: instance.prepares.as_slice(),
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn get_if_committed(&mut self, sequence: u64, votes: usize) -> Option<Committed<'_>> {
        match self.get_mut(sequence) {
            Some(instance) => {
                if instance.is_committed(votes) >= Check::Changed {
                    Some(Committed {
                        pre_prepare: instance.pre_prepare.as_ref().unwrap(),
                        prepares: instance.prepares.as_slice(),
                        commits: instance.commits.as_slice(),
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    pub fn prepared_instances(&self) -> impl Iterator<Item = Prepared<'_>> {
        self.current_view
            .slot_iter()
            .filter_map(move |(index, item)| {
                if item.state >= OrderingState::Prepared {
                    Some(Prepared {
                        pre_prepare: item.pre_prepare.as_ref().unwrap(),
                        prepares: item.prepares.as_slice(),
                    })
                } else {
                    find_prepared_in_stack(index, &self.old_views)
                }
            })
    }
}

fn find_prepared_in_stack(
    sequence: u64,
    view_stack: &[Slots<OrderInstance>],
) -> Option<Prepared<'_>> {
    let views = view_stack.iter().rev();
    views
        .filter_map(|view| view.get(sequence))
        .filter_map(|instance| {
            if instance.state >= OrderingState::Prepared {
                Some(Prepared {
                    pre_prepare: instance.pre_prepare.as_ref().unwrap(),
                    prepares: instance.prepares.as_slice(),
                })
            } else {
                None
            }
        })
        .next()
}

#[derive(Debug)]
pub struct Prepared<'a> {
    pub pre_prepare: &'a Message<PrePrepare>,
    pub prepares: &'a [Message<Prepare>],
}

#[derive(Debug)]
pub struct Committed<'a> {
    pub pre_prepare: &'a Message<PrePrepare>,
    pub prepares: &'a [Message<Prepare>],
    pub commits: &'a [Message<Commit>],
}
