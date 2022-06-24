use std::io;

use futures_util::{sink::SinkExt, stream::StreamExt};

use crate::{
    comms::{Receiver, Sender, TrySendError},
    net::{
        codec::MessageCodec,
        messages::{Message, Raw, Tag},
    },
};
use futures_util::stream::{SplitSink, SplitStream};
use std::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub async fn send_task<T, TAG: Tag>(
    rx: Receiver<Message<Raw<TAG>>>,
    mut io: SplitSink<Framed<T, MessageCodec<TAG>>, Message<Raw<TAG>>>,
) -> io::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    io.send_all(&mut rx.map(Ok)).await?;
    Ok(())
}

pub async fn recv_task<T, TAG: Tag>(
    mut tx: Sender<Message<Raw<TAG>>>,
    io: SplitStream<Framed<T, MessageCodec<TAG>>>,
) -> io::Result<()>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    //    let tx = tx.sink_map_err(|_| Error::Io {
    //        source: io::Error::new(ErrorKind::BrokenPipe, "sender closed for receive task"),
    //    });
    //    io.forward(tx).await?;
    tx.try_send_all(io).await.map_err(|e| match e {
        TrySendError::SinkErr { source } => io::Error::new(ErrorKind::BrokenPipe, source),
        TrySendError::StreamErr { source } => source.into(),
    })
}
