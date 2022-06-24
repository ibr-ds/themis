use std::{fmt, net::SocketAddr, time::Duration};

use fmt::Debug;
use futures_util::FutureExt;
use tokio::{
    net::{TcpListener, TcpStream, ToSocketAddrs},
    time::sleep,
};
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use tracing::Instrument;

use crate::{
    comms::Sender,
    execute::spawn,
    net::{
        handshake::{HandshakeFactory, Role},
        NewConnection,
    },
    Error, Result,
};

use super::TcpConnection;

pub struct Endpoint<H> {
    myid: u64,
    listener: TcpListener,
    sink: Sender<NewConnection<TcpConnection>>,
    hs_factory: H,
}

impl<H> Endpoint<H>
where
    H: HandshakeFactory,
{
    /// Create a new endpoint for addr, will then perform handshake F for each new connecition.
    pub async fn bind<T>(
        myid: u64,
        addr: T,
        sink: Sender<NewConnection<TcpConnection>>,
        handshake: H,
    ) -> Self
    where
        T: tokio::net::ToSocketAddrs + Debug,
    {
        tracing::info!("binding {:?}", addr);
        let tcp = TcpListener::bind(addr).await.expect("bind");
        Self {
            myid,
            listener: tcp,
            sink,
            hs_factory: handshake,
        }
    }

    pub async fn listen(self) -> Result<()> {
        tracing::debug!("listening on {}", self.listener.local_addr().unwrap());
        loop {
            let (stream, addr) = self.listener.accept().await.map_err(|source| Error::Io {
                name: "listener",
                id: 0_u64,
                source,
            })?;
            tracing::debug!(addr = format_args!("{}", addr), "incoming connection");
            let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
            let mut sink = self.sink.clone();
            let hs = self.hs_factory.create_handshake(Role::Acceptor);
            spawn(
                async move {
                    tracing::trace!("start handshake");
                    let hsr = hs.execute(&mut stream).await?;

                    let new_conn = NewConnection {
                        io: stream,
                        role: Role::Acceptor,
                        handshake: hsr,
                    };

                    sink.send(new_conn).await?;
                    Ok::<_, crate::Error>(())
                }
                .map(move |r| {
                    if let Err(e) = r {
                        tracing::warn!("{}", e);
                    }
                })
                .instrument(tracing::info_span!(
                    "handshake",
                    addr = format_args!("{}", addr)
                )),
            );
        }
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }
}

#[tracing::instrument(skip(sink, handshake))]
pub async fn connect<'a, A, H>(
    addr: A,
    mut sink: Sender<NewConnection<TcpConnection>>,
    handshake: H,
) -> Result<()>
where
    A: ToSocketAddrs + Debug + 'a,
    H: HandshakeFactory,
{
    tracing::debug!("connecting");
    let stream = loop {
        match TcpStream::connect(&addr).await {
            Ok(stream) => break stream,
            Err(e) => {
                tracing::debug!("error: {}", e);
                sleep(Duration::from_millis(1000)).await;
            }
        }
    };

    let mut stream = Framed::new(stream, LengthDelimitedCodec::new());
    let result = handshake
        .create_handshake(Role::Connector)
        .execute(&mut stream)
        .await?;
    tracing::debug!("Info connected to {:?}", addr);

    let new_conn = NewConnection {
        io: stream,
        role: Role::Connector,
        handshake: result,
    };

    sink.send(new_conn).await?;
    Ok(())
}

#[cfg(test)]
mod test {
    use std::net::Ipv4Addr;

    use futures_util::StreamExt;
    use ring::hmac::HMAC_SHA256;
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
    };
    use tracing::Instrument;

    use crate::{authentication::hmac::HmacFactory, comms};

    use super::Endpoint;

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_endpoint() {
        let _ = tracing_subscriber::fmt::try_init();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let (conns_sender, mut conns_receiver) = comms::unbounded();
            let hs = HmacFactory::new(1, HMAC_SHA256);
            let mut addr = tokio::net::lookup_host("localhost:0").await.unwrap();
            let endpoint =
                Endpoint::bind(1, addr.next().unwrap(), conns_sender.clone(), hs.clone()).await;

            let addr = endpoint.local_addr().unwrap();

            tokio::spawn(
                async { endpoint.listen().await.unwrap() }
                    .instrument(tracing::info_span!("server")),
            );

            super::connect(addr, conns_sender, hs.clone())
                .instrument(tracing::info_span!("client"))
                .await
                .unwrap();

            let hs1 = conns_receiver.next().await.unwrap();
            tracing::info!("{:?}", hs1);
            let hs2 = conns_receiver.next().await.unwrap();
            tracing::info!("{:?}", hs2);
        })
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_tokio_listeners() {
        let _ = tracing_subscriber::fmt::try_init();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let listener = TcpListener::bind((Ipv4Addr::UNSPECIFIED, 9000))
                .await
                .unwrap();
            let addr = listener.local_addr().unwrap();
            tokio::spawn(async move {
                loop {
                    let (mut stream, _addr) = listener.accept().await.unwrap();
                    tokio::spawn(async move {
                        tracing::info!("pre-write");
                        stream.write_all(b"foobar").await.unwrap();
                        tracing::info!("written");
                    });
                }
            });

            let stream = TcpStream::connect(addr).await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut line = String::new();
            stream.read_line(&mut line).await.unwrap();
            tracing::info!(line = line.as_str(), "read");
        })
    }
}
