/// This module defines an Error type for the framework
/// Most Errors will be either IO or channel SendErrors.
use std::{borrow::Cow, error::Error as StdError};
use std::{
    fmt::{self, Display, Formatter},
    future::Future,
    io,
    pin::Pin,
    result::Result as StdResult,
    task::Poll,
};

use futures_util::{ready, task::Context};
use thiserror::Error;
use tracing::error;

use pin_project::pin_project;

use crate::{comms::SendError, protocol};

///Error type for Errors that can occur in an application running on the framework
/// Placeholder WIP
#[derive(Debug)]
pub struct ApplicationError {
    //TODO
    inner: anyhow::Error,
}

impl From<anyhow::Error> for ApplicationError {
    fn from(inner: anyhow::Error) -> Self {
        Self { inner }
    }
}

impl Display for ApplicationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl StdError for ApplicationError {}

///Framework Error type that Supports four main kind of Errors:
/// IO Errors from Sockets etc
/// SendErrors from Channels
/// Protocol and Application specific Errors
/// This Type is designed to be used with the ? Operator so it implements all the necessary
/// From<> Traits
#[derive(Error, Debug)]
pub enum Error {
    #[error("{} connection to {} closed: {}", name, id, source)]
    Io {
        source: io::Error,
        name: &'static str,
        id: u64,
    },
    #[error("channel closed: {}", source)]
    Channel { source: SendError },
    #[error("stream closed.")]
    EndOfStream {},
    #[error("protocol error: {}", source)]
    Protocol { source: protocol::Error },
    #[error("application error: {}", source)]
    Application { source: ApplicationError },
    #[error("failed to join module {}: {}", module, source)]
    Join {
        source: tokio::task::JoinError,
        module: String,
    },
    #[error("deserialize {}: {}", reason, source)]
    Deserialize {
        reason: Cow<'static, str>,
        source: rmp_serde::decode::Error,
    },
}

impl Error {
    //does not work without Sync bound for some reason
    pub fn protocol<E>(e: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Error::Protocol {
            source: protocol::Error::Abort { source: e.into() },
        }
    }

    pub fn application<E>(e: E) -> Self
    where
        E: StdError + Send + Sync + 'static,
    {
        Error::Application {
            source: ApplicationError { inner: e.into() },
        }
    }
}

impl From<SendError> for Error {
    fn from(e: SendError) -> Self {
        Error::Channel { source: e }
    }
}

impl From<ApplicationError> for Error {
    fn from(e: ApplicationError) -> Self {
        Error::Application { source: e }
    }
}

impl From<protocol::Error> for Error {
    fn from(e: protocol::Error) -> Self {
        Error::Protocol { source: e }
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(e: rmp_serde::encode::Error) -> Self {
        Error::Io {
            source: io::Error::new(io::ErrorKind::InvalidData, e),
            name: "encode",
            id: 0,
        }
    }
}

impl From<toml::de::Error> for Error {
    fn from(e: toml::de::Error) -> Self {
        Error::Io {
            source: io::Error::new(io::ErrorKind::InvalidData, e),
            name: "toml",
            id: 0,
        }
    }
}

/// Result typedef for the framework. Same as io::Result and friends.
pub type Result<T> = StdResult<T, Error>;

/// Attach an error logger to a future that return Result<()> and change the output to ()
pub trait LogErrorExt: Sized {
    fn log_error(self) -> LogError<Self> {
        LogError {
            f: self,
            kind: LogKind::Error,
        }
    }

    fn log_warn(self) -> LogError<Self> {
        LogError {
            f: self,
            kind: LogKind::Warn,
        }
    }
}

impl<T> LogErrorExt for T where T: Future<Output = Result<()>> {}

#[derive(Debug, Clone, Copy)]
enum LogKind {
    Error,
    Warn,
}

#[pin_project(project = LogErrorProj)]
pub struct LogError<F> {
    #[pin]
    f: F,
    kind: LogKind,
}

impl<F> Future for LogError<F>
where
    F: Future<Output = Result<()>>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<<Self as Future>::Output> {
        let LogErrorProj { f, kind } = self.project();
        let result = ready!(f.poll(ctx));
        if let Err(e) = result {
            match kind {
                LogKind::Error => error!("{}", e),
                LogKind::Warn => tracing::warn!("{}", e),
            }
        }
        Poll::Ready(())
    }
}
