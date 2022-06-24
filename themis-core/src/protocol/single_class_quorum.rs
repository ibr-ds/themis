use std::fmt::Debug;

use crate::protocol::Match;

#[derive(Debug)]
pub struct SingleClassQuorum<Master, Other>
where
    Master: Match<Other>,
    Other: PartialEq + Debug,
{
    capacity: usize,
    quorum: usize,
    master: Option<Master>,
    others: Vec<Other>,
    open: bool,
}

impl<Master, Other> SingleClassQuorum<Master, Other>
where
    Master: Match<Other>,
    Other: PartialEq + Debug,
{
    pub fn new(quorum: usize, capacity: usize) -> Self {
        Self {
            quorum: quorum - 1,
            capacity: capacity - 1,
            master: Option::None,
            others: Vec::new(),
            open: true,
        }
    }

    pub fn winning_messages(&self) -> impl Iterator<Item = &Other> + '_ {
        let master = self.master.as_ref();
        self.others.iter().filter(move |i| {
            if let Some(master) = master {
                master.matches(i)
            } else {
                false
            }
        })
    }

    pub fn offer_master(&mut self, master: Master) -> bool {
        if self.master.is_none() {
            tracing::trace!("inserting master");
            self.master = Some(master);
        }

        if !self.open || !self.is_complete() {
            false
        } else {
            self.open = false;
            true
        }
    }

    pub fn offer(&mut self, message: Other) -> bool {
        if self.others.len() < self.capacity && !self.others.contains(&message) {
            tracing::trace!("inserting {:?} into qurom", message);
            self.others.push(message)
        }

        if !self.open || !self.is_complete() {
            tracing::trace!(
                "quorum still open: has {}, wants {}",
                self.others.len(),
                self.quorum
            );
            false
        } else {
            tracing::trace!(
                "quorum complete: has {}, wants {}",
                self.others.len(),
                self.quorum
            );
            self.open = false;
            true
        }
    }

    pub fn is_complete(&self) -> bool {
        if let Some(ref master) = self.master {
            let count = self.others.iter().filter(|i| master.matches(i)).count();
            count >= self.quorum
        } else {
            false
        }
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    pub fn master(&self) -> Option<&Master> {
        self.master.as_ref()
    }

    pub fn into_master(self) -> Option<Master> {
        self.master
    }

    pub fn has_master(&self) -> bool {
        self.master.is_some()
    }

    pub fn others(&self) -> &[Other] {
        &self.others
    }

    pub fn quorum(&self) -> u64 {
        self.quorum as u64 + 1
    }

    pub fn capacity(&self) -> u64 {
        self.capacity as u64 + 1
    }

    /// Completes the Quorum and returns the Messages that prove completion
    ///
    /// # Panics
    /// Panics when the Qurom is not complete, or in otherwise invalid state like Master missing
    /// or not enough Others
    pub fn take(&mut self) -> (Master, Box<[Other]>) {
        debug_assert!(!self.open);
        debug_assert!(self.is_complete());

        let master = self.master.take().unwrap();
        let mut others = std::mem::take(&mut self.others);
        others.retain(|i| master.matches(i));
        others.truncate(self.quorum);

        debug_assert_eq!(self.quorum, others.len());

        (master, others.into_boxed_slice())
    }
}
