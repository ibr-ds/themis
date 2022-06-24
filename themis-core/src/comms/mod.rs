use futures_util::{
    sink::{Sink, SinkExt},
    stream::{Stream, StreamExt, TryStreamExt},
    task::Context,
};

use std::{fmt::Debug, pin::Pin, task::Poll};
use thiserror::Error;
use tokio::sync::mpsc::{
    Receiver as BoundedReceiver, Sender as BoundedSender, UnboundedReceiver, UnboundedSender,
};

use serde::de::StdError;

pub trait SinkWrapper<T>: Sink<T> + Debug + Send + Sync {
    fn clone_object(&self) -> Pin<Box<dyn SinkWrapper<T, Error = Self::Error>>>;
}

impl<T> SinkWrapper<T> for T
where
    T: Sink<T> + Debug + Send + Clone + Sync + 'static,
{
    fn clone_object(&self) -> Pin<Box<dyn SinkWrapper<T, Error = Self::Error>>> {
        Box::pin(self.clone())
    }
}

#[derive(Debug)]
pub enum Sender<T> {
    Bounded(BoundedSender<T>),
    Unbounded(UnboundedSender<T>),
    Custom(Pin<Box<dyn SinkWrapper<T, Error = SendError>>>),
    Drain(),
    Drop,
}

impl<T> Sender<T> {
    pub fn drain() -> Self {
        Self::Drain()
    }

    pub fn custom(sink: impl SinkWrapper<T, Error = SendError> + 'static) -> Self {
        Sender::Custom(Box::pin(sink))
    }

    pub async fn send(&mut self, message: T) -> Result<(), SendError> {
        match self {
            Sender::Bounded(s) => s.send(message).await?,
            Sender::Unbounded(s) => s.send(message)?,
            Sender::Custom(s) => s.send(message).await?,
            Sender::Drain() => {}
            Sender::Drop => {}
        };
        Ok(())
    }

    pub async fn send_all(
        &mut self,
        mut stream: impl Stream<Item = T> + Unpin,
    ) -> Result<(), SendError> {
        while let Some(message) = stream.next().await {
            self.send(message).await?;
        }
        Ok(())
    }

    pub async fn try_send_all<E: StdError + 'static>(
        &mut self,
        mut stream: impl Stream<Item = Result<T, E>> + Unpin,
    ) -> Result<(), TrySendError<E>> {
        while let Some(message) = stream
            .try_next()
            .await
            .map_err(|source| TrySendError::StreamErr { source })?
        {
            self.send(message).await?;
        }
        Ok(())
    }
}

impl<T> Sender<T> {
    pub fn unbounded_send(&mut self, message: T) -> Result<(), T> {
        match self {
            Sender::Bounded(_s) => Err(message),
            Sender::Custom(_s) => Err(message),
            Sender::Unbounded(s) => s.send(message).map_err(|e| e.0),
            Sender::Drain() => Ok(()),
            Sender::Drop => Ok(()),
        }
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        match self {
            Sender::Bounded(s) => Sender::Bounded(s.clone()),
            Sender::Unbounded(s) => Sender::Unbounded(s.clone()),
            Sender::Custom(s) => Sender::Custom(s.clone_object()),
            Sender::Drain() => Sender::Drain(),
            Sender::Drop => Sender::Drop,
        }
    }
}

#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum SendError {
    #[error("send failed because channel is full")]
    Full,
    #[error("send failed because receiver is gone")]
    Disconnected,
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for SendError {
    fn from(_s: tokio::sync::mpsc::error::SendError<T>) -> Self {
        SendError::Full
    }
}

#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum TrySendError<E: StdError + 'static> {
    #[error("{}", source)]
    SinkErr { source: SendError },
    #[error("{}", source)]
    StreamErr { source: E },
}

impl<E: StdError + 'static> From<SendError> for TrySendError<E> {
    fn from(e: SendError) -> Self {
        TrySendError::SinkErr { source: e }
    }
}

pub trait StreamWrapper: Stream + Send + Debug {}
impl<T> StreamWrapper for T where T: Stream + Send + Debug {}

#[derive(Debug)]
pub enum Receiver<T> {
    Bounded(BoundedReceiver<T>),
    Unbounded(UnboundedReceiver<T>),
    Complex(Pin<Box<dyn StreamWrapper<Item = T>>>),
}

impl<T> Stream for Receiver<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut() {
            Receiver::Bounded(r) => r.poll_recv(ctx),
            Receiver::Unbounded(r) => r.poll_recv(ctx),
            Receiver::Complex(r) => r.as_mut().poll_next(ctx),
        }
    }
}

pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::channel(size);
    (Sender::Bounded(tx), Receiver::Bounded(rx))
}

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    (Sender::Unbounded(tx), Receiver::Unbounded(rx))
}
