use std::{borrow::Cow, cmp::max, mem::size_of};

use fnv::FnvHashMap;
use futures_util::future::{ready, Ready};
use serde::{Deserialize, Serialize};

use themis_core::{
    app::{self, ApplyError, Request, Response},
    config::Config,
    net::Message,
    Result,
};

#[derive(Serialize, Deserialize, Clone)]
pub struct Counter {
    counter: u64,
}

pub struct CounterApp {
    id: u64,
    counters: FnvHashMap<u64, Counter>,

    reply_size: usize,
}

impl CounterApp {
    pub fn new(uuid: u64, config: &Config) -> Self {
        let reply_size: usize = config.get("reply_size").expect("config: reply_size");
        Self {
            id: uuid,
            counters: FnvHashMap::default(),
            reply_size,
        }
    }
}

impl<'app> app::Application<'app> for CounterApp {
    type ExecuteFut = Ready<Result<Message<Response>>>;

    fn execute(&mut self, request: Message<Request>) -> Self::ExecuteFut {
        let counter = self
            .counters
            .entry(request.source)
            .or_insert_with(|| Counter { counter: 0 });
        counter.counter += 1;
        log::debug!(
            "Request from client {}. Counter {}.",
            request.source,
            counter.counter
        );

        let size = max(self.reply_size, size_of::<u64>());

        let mut bytes = Vec::with_capacity(size as usize);
        bytes.extend_from_slice(&counter.counter.to_le_bytes());
        if self.reply_size > size_of::<usize>() {
            bytes.extend_from_slice(&vec![0; self.reply_size - size_of::<usize>()]);
        }

        ready(Ok(Message::new(
            self.id,
            request.source,
            Response::with_contact(request.inner.sequence, bytes.into(), request.destination),
        )))
    }

    type CheckpointHandle = Cow<'app, FnvHashMap<u64, Counter>>;
    type CheckpointData = ();
    type TakeFut = Ready<Result<Self::CheckpointHandle>>;

    fn take_checkpoint(&'app mut self) -> Self::TakeFut {
        ready(Ok(Cow::Borrowed(&self.counters)))
    }

    type ApplyFut = Ready<std::result::Result<(), ApplyError>>;
    fn apply_checkpoint(
        &'app mut self,
        handle: Self::CheckpointHandle,
        _data: Self::CheckpointData,
    ) -> Self::ApplyFut {
        self.counters = handle.into_owned();

        ready(Ok(()))
    }

    type ResolveFut = Ready<Result<()>>;

    fn resolve_checkpoint(&'app mut self, _handle: Self::CheckpointHandle) -> Self::ResolveFut {
        ready(Ok(()))
    }
}
