use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use futures_util::FutureExt;
use themis_core::net::DisplayBytes;
use tokio::time::{sleep, Sleep};

#[derive(Debug, Eq, PartialEq)]
pub enum Info {
    Request(Bytes),
    Instance { sequence: u64, digest: Bytes },
    ViewChange { to: u64 },
    Spin,
}

impl Display for Info {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Info::Request(digest) => write!(f, "Request: {}", DisplayBytes(digest)),
            Info::Instance { sequence, digest } => {
                write!(f, "Instance {}: {}", sequence, DisplayBytes(digest))
            }
            Info::ViewChange { to } => write!(f, "View Change to view {}", to),
            Info::Spin => write!(f, "spin"),
            
        }
    }
}

#[derive(Debug)]
struct Inner {
    timeout: Pin<Box<Sleep>>,
    reason: Info,
}

impl Inner {
    fn new(timeout: Sleep, reason: Info) -> Self {
        Self {
            timeout: Box::pin(timeout),
            reason,
        }
    }
}

#[derive(Debug)]
pub struct Timeout {
    inner: Option<Inner>,
}

impl Timeout {
    pub fn empty() -> Self {
        Timeout { inner: None }
    }

    pub fn request(duration: Duration, digest: Bytes) -> Self {
        Self {
            inner: Some(Inner::new(sleep(duration), Info::Request(digest))),
        }
    }

    pub fn instance(duration: Duration, digest: Bytes, sequence: u64) -> Self {
        Self {
            inner: Some(Inner::new(
                sleep(duration),
                Info::Instance { sequence, digest },
            )),
        }
    }

    pub fn view_change(duration: Duration, view: u64) -> Self {
        Self {
            inner: Some(Inner::new(sleep(duration), Info::ViewChange { to: view })),
        }
    }

    pub fn clear(&mut self) {
        self.inner = None
    }

    pub fn is_running(&self) -> bool {
        self.inner.is_some()
    }
}

impl Future for Timeout {
    type Output = Info;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner.take() {
            None => Poll::Pending,
            Some(mut inner) => match inner.timeout.poll_unpin(cx) {
                Poll::Pending => {
                    self.inner = Some(inner);
                    Poll::Pending
                }
                Poll::Ready(_) => Poll::Ready(inner.reason),
            },
        }
    }
}

impl Default for Timeout {
    fn default() -> Self {
        Self::empty()
    }
}
