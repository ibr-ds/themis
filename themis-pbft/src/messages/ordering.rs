#![allow(unused)]

use std::{
    any::type_name,
    fmt::{self, Debug, Formatter},
    marker::PhantomData,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use themis_core::net::{DisplayBytes, NetworkMessage, Sequenced};

use super::{PBFTTag, Viewed, PBFT};
use fmt::Display;

#[derive(Serialize, Deserialize)]
pub struct OrderingMessage<T> {
    pub sequence: u64,
    pub view: u64,
    pub request: Bytes,
    _kind: PhantomData<T>,
}

impl<T> OrderingMessage<T> {
    pub fn new(sequence: u64, view: u64, request: Bytes) -> Self {
        Self {
            sequence,
            view,
            request,
            _kind: PhantomData,
        }
    }

    // Specialization of From<T> for T :/
    pub fn from<U>(other: OrderingMessage<U>) -> Self {
        Self::new(other.sequence, other.view, other.request)
    }

    // Specialization of Into<U> for T :/
    pub fn into<U>(self) -> OrderingMessage<U> {
        OrderingMessage::new(self.sequence, self.view, self.request)
    }

    pub fn make<U>(&self) -> OrderingMessage<U> {
        self.clone().into()
    }
}

impl<T> Display for OrderingMessage<T>
where
    Self: NetworkMessage<PBFT>,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "<{:?}|{},{}|{}>",
            Self::TAG,
            self.view,
            self.sequence,
            DisplayBytes(&self.request)
        )
    }
}

impl<T, U> PartialEq<OrderingMessage<U>> for OrderingMessage<T> {
    fn eq(&self, other: &OrderingMessage<U>) -> bool {
        self.sequence == other.sequence && self.view == other.view && self.request == other.request
    }
}

impl<T> Clone for OrderingMessage<T> {
    fn clone(&self) -> Self {
        Self {
            sequence: self.sequence,
            view: self.view,
            request: self.request.clone(),
            _kind: PhantomData,
        }
    }
}

impl<T> Debug for OrderingMessage<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct(type_name::<Self>())
            .field("sequence", &self.sequence)
            .field("view", &self.view)
            .field("request", &self.request)
            .finish()
    }
}

impl<T> Default for OrderingMessage<T> {
    fn default() -> Self {
        Self::new(0, 0, Bytes::default())
    }
}

impl<T> Sequenced for OrderingMessage<T> {
    fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl<T> Viewed for OrderingMessage<T> {
    fn view(&self) -> u64 {
        self.view
    }
}

pub type PrePrepare = OrderingMessage<kinds::PrePrepare>;
pub type Prepare = OrderingMessage<kinds::Prepare>;
pub type Commit = OrderingMessage<kinds::Commit>;

mod kinds {
    pub struct PrePrepare;

    pub struct Prepare;

    pub struct Commit;
}
