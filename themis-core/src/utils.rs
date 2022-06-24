use bytes::BytesMut;
use futures_util::{
    stream::{FusedStream, Stream},
    task::Context,
};
use std::{pin::Pin, task::Poll};

use pin_project::pin_project;

/// Writer that writes to a BytesMut, allocating the necessary memory.
/// This wrapped is needed because BytesMut normally does not allocate,
/// but we don't know the size of a serialized Message beforehand.
pub struct AllocBytesWriter<'a>(pub &'a mut BytesMut);

impl std::io::Write for AllocBytesWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.extend_from_slice(buf); // Only BytesMut method that allocates
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Wraps a Stream and uses poll_fused to poll the Stream
/// This enable the use with select and select_next_some() for Stream that return None without
/// being terminated
#[pin_project]
#[derive(Debug, Default)]
pub struct Fuse<T>(#[pin] pub T);

impl<T> Stream for Fuse<T>
where
    T: FusedStream,
{
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.as_mut().project().0.poll_next(cx) {
            Poll::Ready(None) if !self.is_terminated() => Poll::Pending,
            poll => poll,
        }
    }
}

impl<T> FusedStream for Fuse<T>
where
    T: FusedStream,
{
    fn is_terminated(&self) -> bool {
        self.0.is_terminated()
    }
}

pub trait PollExt<T> {
    fn map_some<F, MapT>(self, f: F) -> Poll<Option<MapT>>
    where
        F: FnOnce(T) -> MapT,
        Self: Sized;
}

impl<T> PollExt<T> for Poll<Option<T>> {
    fn map_some<F, MapT>(self, f: F) -> Poll<Option<MapT>>
    where
        F: FnOnce(T) -> MapT,
        Self: Sized,
    {
        self.map(|o| o.map(f))
    }
}
