use futures_util::future::{ready, Ready};

use bytes::Bytes;
use kvs_core::Store;
use themis_core::{
    app::{self, ApplyError, Request, Response},
    config::Config,
    net::Message,
    Result,
};

pub struct StoreApp {
    id: u64,
    store: Store,
}

impl StoreApp {
    pub fn new(uuid: u64, _config: &Config) -> Self {
        Self {
            id: uuid,
            store: Store::default(),
        }
    }
}

impl<'app> app::Application<'app> for StoreApp {
    type ExecuteFut = Ready<Result<Message<Response>>>;

    fn execute(&mut self, request: Message<Request>) -> Self::ExecuteFut {
        log::debug!("Request from client {}.", request.source,);

        let response: Bytes = self.store.execute(request.inner.payload);

        ready(Ok(Message::new(
            self.id,
            request.source,
            Response::with_contact(request.inner.sequence, response, request.destination),
        )))
    }

    type CheckpointHandle = Bytes;
    type CheckpointData = ();
    type TakeFut = Ready<Result<Self::CheckpointHandle>>;

    fn take_checkpoint(&'app mut self) -> Self::TakeFut {
        ready(Ok(self.store.export_state()))
    }

    type ApplyFut = Ready<std::result::Result<(), ApplyError>>;
    fn apply_checkpoint(
        &'app mut self,
        handle: Self::CheckpointHandle,
        _data: Self::CheckpointData,
    ) -> Self::ApplyFut {
        self.store
            .import_state(handle)
            .expect("Error importing state");

        ready(Ok(()))
    }

    type ResolveFut = Ready<Result<()>>;

    fn resolve_checkpoint(&'app mut self, _handle: Self::CheckpointHandle) -> Self::ResolveFut {
        ready(Ok(()))
    }
}
