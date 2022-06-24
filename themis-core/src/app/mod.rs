use std::{
    error::Error,
    fmt::{self, Display, Formatter},
    future::Future,
};

use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};
use tokio::sync::oneshot;

use crate::{
    net::{AuthenticationKind, Message, Tag},
    protocol::{self, Checkpoint},
};

mod worker;
pub use worker::*;
mod messages;
pub use messages::*;

#[repr(u8)]
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone, Copy, PartialEq, Eq)]
pub enum Client {
    Request = 0,
    Response = 1,
}

impl Tag for Client {
    fn requires_signing(&self) -> Option<AuthenticationKind> {
        Some(AuthenticationKind::Framework)
    }
    fn requires_validation(&self) -> Option<AuthenticationKind> {
        Some(AuthenticationKind::Framework)
    }
}

#[derive(Debug)]
pub enum Command {
    Request(Message<Request>),
    ReadOnlyRequst(Message<Request>),
    TakeCheckpoint(oneshot::Sender<Bytes>),
    ResolveCheckpoint {
        handle: Bytes,
        tx: oneshot::Sender<Bytes>,
    },
    CheckpointStable {
        sequence: u64,
        quorum: Vec<Message<protocol::Checkpoint>>,
        apply: Option<ApplyCheckpoint>,
    },
}

#[derive(Debug)]
pub struct ApplyCheckpoint {
    pub handle: Bytes,
    pub data: Bytes,
    pub tx: oneshot::Sender<bool>,
}

impl PartialEq for ApplyCheckpoint {
    fn eq(&self, other: &Self) -> bool {
        self.handle == other.handle && self.data == other.data
    }
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Command::Request(r1), Command::Request(r2)) => r1 == r2,
            (
                Command::CheckpointStable {
                    sequence, apply, ..
                },
                Command::CheckpointStable {
                    sequence: s2,
                    apply: apply2,
                    ..
                },
            ) => sequence == s2 && apply == apply2,
            (Command::ReadOnlyRequst(r1), Command::ReadOnlyRequst(r2)) => r1 == r2,
            (
                Command::ResolveCheckpoint { handle, .. },
                Command::ResolveCheckpoint { handle: h2, .. },
            ) => handle == h2,
            (_, _) => false,
        }
    }
}

/// Errortype for applications
#[derive(Debug)]
pub struct AppError {
    //what should be in here?
}

impl Display for AppError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Application Error")
    }
}

impl Error for AppError {}

// /// Request-Reply application
// pub trait Application<'app>: Send {
//     type E: Future<Output = Result<Message<Response>>> + Send + 'app;
//     fn execute(&'app mut self, request: Message<Request>) -> Self::E;

//     type C: Future<Output = Result<Bytes>> + Send + 'app;
//     fn get_checkpoint(&'app self) -> Self::C;

//     type ApplyFuture: Future<Output = Result<()>> + Send + 'app;
//     fn apply_checkpoint(&'app mut self, checkpoint: &[u8]) -> Self::ApplyFuture;
// }

pub enum ApplyError {
    Mismatch,
    Fatal(crate::Error),
}

pub trait Application<'f>: Send {
    type ExecuteFut: Future<Output = crate::Result<Message<Response>>> + Send + 'f;
    fn execute(&'f mut self, request: Message<Request>) -> Self::ExecuteFut;

    type CheckpointHandle: Serialize + DeserializeOwned + Send;
    type CheckpointData: Serialize + DeserializeOwned + Send;

    type TakeFut: Future<Output = crate::Result<Self::CheckpointHandle>> + Send + 'f;
    fn take_checkpoint(&'f mut self) -> Self::TakeFut;

    type ApplyFut: Future<Output = std::result::Result<(), ApplyError>> + Send + 'f;
    fn apply_checkpoint(
        &'f mut self,
        handle: Self::CheckpointHandle,
        checkpoint: Self::CheckpointData,
    ) -> Self::ApplyFut;

    type ResolveFut: Future<Output = crate::Result<Self::CheckpointData>> + Send + 'f;
    fn resolve_checkpoint(&'f mut self, handle: Self::CheckpointHandle) -> Self::ResolveFut;

    fn checkpoint_stable(
        &mut self,
        _sequence: u64,
        _quorum: Vec<Message<Checkpoint>>,
    ) -> crate::Result<()> {
        Ok(())
    }
}
