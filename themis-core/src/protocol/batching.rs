use std::{mem, pin::Pin, task::Poll, time::Duration};

use futures_util::{
    ready,
    stream::{FusedStream, Stream},
    task::Context,
    FutureExt, StreamExt,
};
use serde::{Deserialize, Serialize};
use tokio::time::{sleep, Sleep};

use crate::{
    app::{Request, RequestFlags},
    net::Message,
};

use super::Proposal;

// Batch config
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    pub min: usize,
    pub max: usize,
    pub timeout: Option<Duration>,
    pub rel_timeout: Option<Duration>,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            min: 1,
            max: 1,
            timeout: None,
            rel_timeout: None,
        }
    }
}

pub struct Batcher<S, T> {
    stream: S,
    abs_timeout: Option<Pin<Box<Sleep>>>,
    rel_timeout: Option<Pin<Box<Sleep>>>,
    config: BatchConfig,
    buffer: Vec<T>,
}

impl<S, T> Batcher<S, T> {
    pub fn new(stream: S, config: BatchConfig) -> Self {
        Self {
            buffer: Vec::with_capacity(config.max as usize),
            abs_timeout: None,
            rel_timeout: None,
            stream,
            config,
        }
    }

    fn cut(&mut self) -> Vec<T> {
        self.abs_timeout.take();
        self.rel_timeout.take();
        mem::replace(
            &mut self.buffer,
            Vec::with_capacity(self.config.max as usize),
        )
    }

    fn set_abs_timeout(&mut self) {
        if let Some(duration) = self.config.timeout {
            if let Some(timeout) = &mut self.abs_timeout {
                timeout.set(sleep(duration));
            } else {
                self.abs_timeout = Some(Box::pin(sleep(duration)))
            }
        }
    }

    fn set_rel_timeout(&mut self) {
        if let Some(duration) = self.config.rel_timeout {
            if let Some(timeout) = &mut self.rel_timeout {
                timeout.set(sleep(duration));
            } else {
                self.rel_timeout = Some(Box::pin(sleep(duration)))
            }
        }
    }

    fn new_item(&mut self, item: T) -> Option<Proposal<T>>
    where
        T: Batchable,
    {
        tracing::trace!("new item");
        if !item.should_batch() {
            return Some(Proposal::Single(item));
        }
        self.buffer.push(item);

        if self.buffer.len() >= self.config.max {
            return Some(Proposal::Batch(self.cut()));
        }

        None
    }

    fn poll_rel_timeout(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let t = self.rel_timeout.as_mut().map(|t| t.poll_unpin(cx));
        t.unwrap_or(Poll::Ready(()))
    }
}

impl<S, T> Stream for Batcher<S, T>
where
    S: Stream<Item = T> + Unpin + Send,
    T: Batchable,
{
    type Item = Proposal<T>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        macro_rules! cut {
            () => {
                return Poll::Ready(Some(Proposal::Batch(self.cut())))
            };
        }

        let mut items_added = false;
        loop {
            match self.stream.poll_next_unpin(cx) {
                Poll::Ready(Some(item)) => {
                    if let Some(batch) = self.new_item(item) {
                        tracing::trace!("new batch: max limit");
                        return Poll::Ready(Some(batch));
                    } else {
                        items_added = true;
                    }
                }
                Poll::Ready(None) => {
                    self.abs_timeout.take();
                    self.rel_timeout.take();
                    if self.buffer.is_empty() {
                        return Poll::Ready(None);
                    } else {
                        tracing::trace!("new batch: closed/last batch");
                        cut!()
                    }
                }
                Poll::Pending => break,
            }
        }

        if items_added {
            if self.abs_timeout.is_none() {
                self.set_abs_timeout();
            }
            if self.buffer.len() >= self.config.min {
                self.set_rel_timeout();
            }
        }

        if self.buffer.len() >= self.config.min
            && matches!(self.poll_rel_timeout(cx), Poll::Ready(()))
        {
            tracing::trace!("new batch: min limit/rel timeout");
            cut!()
        } else if let Some(abs_timeout) = &mut self.abs_timeout {
            ready!(abs_timeout.poll_unpin(cx));
            tracing::trace!("new batch: abs timeout");
            cut!()
        } else {
            Poll::Pending
        }
    }
}

pub trait Batchable {
    fn should_batch(&self) -> bool;
}

impl Batchable for Message<Request> {
    fn should_batch(&self) -> bool {
        !self.inner.flags.contains(RequestFlags::READ_ONLY)
    }
}

impl<S, T> FusedStream for Batcher<S, T>
where
    S: FusedStream<Item = T> + Send + Unpin,
    T: Batchable,
{
    fn is_terminated(&self) -> bool {
        self.buffer.is_empty() && self.stream.is_terminated()
    }
}

impl<S, T> Unpin for Batcher<S, T> where S: Unpin {}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use bytes::Bytes;

    use assert_matches::assert_matches;
    use futures_util::poll;

    use crate::{app::Request, net::Message};

    use super::*;

    #[derive(Debug)]
    struct Item {
        batch: bool,
    }

    impl Item {
        fn new() -> Self {
            Self { batch: true }
        }
    }

    impl Batchable for Item {
        fn should_batch(&self) -> bool {
            self.batch
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn no_batch() {
        let (mut sender, recv) = crate::comms::unbounded();
        let config = BatchConfig::default();
        let mut b = Batcher::new(recv, config);

        for i in 0..3 {
            sender
                .send(Message::new(
                    0,
                    0,
                    Request::new(i, Bytes::from_static(b"hello")),
                ))
                .await
                .unwrap();
        }

        for i in 0..3 {
            let next = b.next().await;
            assert_eq!(
                next,
                Some(Proposal::Batch(vec![Message::new(
                    0,
                    0,
                    Request::new(i, Bytes::from_static(b"hello")),
                )]))
            )
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_greater_batch() {
        let (mut sender, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 2,
            timeout: Duration::new(0, 0).into(),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);

        for i in 0..4 {
            sender
                .send(Message::new(
                    0,
                    0,
                    Request::new(i, Bytes::from_static(b"hello")),
                ))
                .await
                .unwrap();
        }

        for i in (0..4).step_by(2) {
            assert_matches!(b.next().await, Some(b) => {
                assert_eq!(2, b.len());
                assert_eq!(i, b[0].inner.sequence);
                assert_eq!(i+1, b[1].inner.sequence);
            });
        }
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_min() {
        let (mut sender, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 10,
            timeout: Duration::new(0, 0).into(),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);

        for i in 0..4 {
            sender
                .send(Message::new(
                    0,
                    0,
                    Request::new(i, Bytes::from_static(b"hello")),
                ))
                .await
                .unwrap();
        }

        assert_matches!(b.next().await, Some(b) => {
            assert_eq!(4, b.len());
            for (i, r) in b.iter().enumerate() {
                assert_eq!(i as u64, r.inner.sequence);
            }
        });
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_max() {
        let (mut sender, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 10,
            timeout: Duration::new(0, 0).into(),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);

        for i in 0..13 {
            sender
                .send(Message::new(
                    0,
                    0,
                    Request::new(i, Bytes::from_static(b"hello")),
                ))
                .await
                .unwrap();
        }

        assert_matches!(b.next().await, Some(b) => {
            assert_eq!(10, b.len());
            for (i, r) in b.iter().enumerate() {
                assert_eq!(i as u64, r.inner.sequence);
            }
        });

        assert_matches!(b.next().await, Some(b) => {
            assert_eq!(3, b.len());
            for (i, r) in b.iter().enumerate() {
                assert_eq!((i+10) as u64, r.inner.sequence);
            }
        });
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_abs_timeout() {
        let _ = tracing_subscriber::fmt::try_init();

        let (mut tx, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 10,
            timeout: Duration::new(1, 0).into(),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);
        tx.send(Item::new()).await.unwrap();

        assert_matches!(poll!(b.next()), Poll::Pending);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();
        b.next().await;
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_abs_timeout_above_min() {
        let _ = tracing_subscriber::fmt::try_init();

        let (mut tx, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 10,
            timeout: Duration::new(1, 0).into(),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);
        tx.send(Item::new()).await.unwrap();
        tx.send(Item::new()).await.unwrap();
        tx.send(Item::new()).await.unwrap();

        b.next().await;
    }

    #[tokio::test]
    #[cfg_attr(miri, ignore)]
    async fn test_rel_timeout() {
        let _ = tracing_subscriber::fmt::try_init();

        let (mut tx, recv) = crate::comms::unbounded();
        let config = BatchConfig {
            min: 2,
            max: 10,
            // timeout: Duration::new(1, 0).into(),
            rel_timeout: Some(Duration::from_secs(1)),
            ..Default::default()
        };
        let mut b = Batcher::new(recv, config);
        tx.send(Item::new()).await.unwrap();
        tx.send(Item::new()).await.unwrap();
        tx.send(Item::new()).await.unwrap();

        assert_matches!(poll!(b.next()), Poll::Pending);
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::resume();

        b.next().await;
    }
}
