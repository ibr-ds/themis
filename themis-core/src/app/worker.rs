use crate::comms::{Receiver, Sender};
use app::ApplyError;
use fnv::FnvHashMap;
use futures_util::stream::StreamExt;
use ring::digest::{digest, SHA256};
use serde::{Deserialize, Serialize};

use crate::{
    app::{self, Application, Command, Request, RequestFlags, Response, ResponseFlags},
    net::{Message, Sequenced},
};
use bytes::Bytes;
use std::borrow::Cow;

use super::ApplyCheckpoint;

/// Execute Requests
pub struct ApplicationWorker<T> {
    uuid: u64,
    rx: Receiver<Command>,
    tx: Sender<Message<Response>>,
    app: T,
}

impl<T> ApplicationWorker<T> {
    pub fn new(uuid: u64, rx: Receiver<Command>, tx: Sender<Message<Response>>, app: T) -> Self {
        Self { uuid, rx, tx, app }
    }
}

impl<T> ApplicationWorker<T>
where
    T: for<'app> Application<'app>,
{
    pub async fn execute(mut self) -> crate::Result<()> {
        while let Some(message) = self.rx.next().await {
            match message {
                Command::ReadOnlyRequst(request) => {
                    if request.inner.flags.contains(RequestFlags::READ_ONLY) {
                        self.execute_request(request).await?;
                    }
                }
                Command::Request(request) => {
                    self.execute_request(request).await?;
                }
                Command::TakeCheckpoint(sender) => {
                    let checkpoint = self.app.take_checkpoint().await?;

                    let checkpoint = rmp_serde::to_vec(&checkpoint)?;
                    if let Err(_cp) = sender.send(checkpoint.into()) {
                        tracing::warn!("Could not return checkpoint to protocol");
                    }
                }
                Command::ResolveCheckpoint { handle, tx } => {
                    let handle: T::CheckpointHandle =
                        rmp_serde::from_slice(&handle).map_err(|e| crate::Error::Deserialize {
                            source: e,
                            reason: "cp handle".into(),
                        })?;
                    let full = self.app.resolve_checkpoint(handle).await?;
                    let full = rmp_serde::to_vec(&full)?;
                    let _ = tx.send(full.into());
                }
                Command::CheckpointStable {
                    sequence,
                    quorum,
                    apply,
                } => {
                    if let Some(ApplyCheckpoint { handle, data, tx }) = apply {
                        tracing::trace!("Applying checkpoint {} to application", sequence);
                        let handle = rmp_serde::from_slice(&handle).map_err(|e| {
                            crate::Error::Deserialize {
                                source: e,
                                reason: "cp handle".into(),
                            }
                        })?;
                        let data = rmp_serde::from_slice(&data).map_err(|e| {
                            crate::Error::Deserialize {
                                source: e,
                                reason: "cp data".into(),
                            }
                        })?;
                        match self.app.apply_checkpoint(handle, data).await {
                            Ok(()) => {
                                tx.send(true).unwrap();
                                self.app.checkpoint_stable(sequence, quorum)?;
                            }
                            Err(ApplyError::Mismatch) => tx.send(false).unwrap(),
                            Err(ApplyError::Fatal(f)) => return Err(f),
                        }
                    } else {
                        self.app.checkpoint_stable(sequence, quorum)?;
                    }
                }
            }
        }
        Err(crate::Error::EndOfStream {})
    }

    async fn execute_request(&mut self, request: Message<Request>) -> crate::Result<()> {
        let request_flags = request.inner.flags;
        tracing::debug!(
            "Executing request {}, flags: {:?} for client {}.",
            request.sequence(),
            request.inner.flags,
            request.source,
        );
        let response = self.app.execute(request).await?;
        self.process_response(request_flags, response).await?;

        Ok(())
    }

    async fn process_response(
        &mut self,
        request_flags: RequestFlags,
        response: Message<Response>,
    ) -> crate::Result<()> {
        if request_flags.contains(RequestFlags::NO_REPLY) {
            return Ok(());
        }

        if request_flags.contains(RequestFlags::HASH_REPLY)
            && !response.inner.flags.contains(ResponseFlags::HASHED)
        {
            // default hashing
            let mut response = response;
            let digest = digest(&SHA256, &response.inner.payload);
            response.inner.payload = Bytes::copy_from_slice(digest.as_ref());
            self.tx.send(response).await?;
        } else {
            self.tx.send(response).await?;
        }
        Ok(())
    }

    // pub fn log_response(&mut self, message: &Message<app::Response>) {
    //     self.client_replies
    //         .entry(message.destination)
    //         .and_modify(|last_response| {
    //             if message.sequence() > *last_response {
    //                 *last_response = message.sequence();
    //             }
    //         })
    //         .or_insert(message.inner.sequence);
    // }

    // pub fn last_response(&mut self, client: u64) -> Option<u64> {
    //     self.client_replies.get(&client).cloned()
    // }
}

/// The reply map has to be saved with the application data,
/// so we can actually re-execute requests
#[derive(Debug, PartialEq, Deserialize, Serialize)]
struct Checkpoint<'a> {
    // borrowing does not work without this ...
    #[serde(with = "serde_bytes")]
    app_data: &'a [u8],
    replies: Cow<'a, FnvHashMap<u64, u64>>,
}

impl<'a> Checkpoint<'a> {
    fn new(app_data: &'a [u8], replies: &'a FnvHashMap<u64, u64>) -> Self {
        Self {
            app_data,
            replies: Cow::Borrowed(replies),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::app::worker::Checkpoint;
    use bytes::Bytes;
    use fnv::FnvHashMap;

    #[test]
    fn checkpoint_serde() {
        let replies: FnvHashMap<u64, u64> = [(1, 3), (2, 5), (0, 10)].iter().cloned().collect();
        let app_data = Bytes::from_static(&[1, 1]);
        let cp = Checkpoint::new(&app_data, &replies);

        //        let mut bytes = rmp_serde::to_vec(&cp).expect("ser");
        let bytes = dbg!(rmp_serde::to_vec(&cp).expect("ser"));

        let cp2: Checkpoint<'_> = rmp_serde::from_slice(&bytes).expect("de");

        assert_eq!(cp, cp2);
    }
}
