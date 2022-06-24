use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use futures_util::{
    future::{try_join_all, FutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use rand::random;
use ring::hmac::HMAC_SHA256;
use std::fmt::Debug;
use themis_core::{
    app::{Client, Request},
    authentication::{hmac::HmacFactory, AuthFactory, Cryptography},
    config::{load_from_paths, Peer, DEFAULT_CONFIG_PATH},
    net::{HandshakeFactory, Message, MessageCodec, Role, TaggedConnection},
    protocol::Match,
};
use tokio::net::{TcpStream, ToSocketAddrs};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, Parser)]
struct Cli {
    #[clap(long, default_value = DEFAULT_CONFIG_PATH)]
    config_path: Vec<String>,
    #[clap(long, min_values = 0)]
    ignore: Vec<u64>,
}

async fn connect(
    addr: impl ToSocketAddrs + Debug + Copy,
    auth: &AuthFactory<Client>,
) -> Result<TaggedConnection<Client>> {
    let tcp_stream = TcpStream::connect(addr)
        .await
        .with_context(|| format!("Connecting to {:?}.", addr))?;
    let mut handshaker = Framed::new(tcp_stream, LengthDelimitedCodec::new());
    let hs_result = auth
        .create_handshake(Role::Connector)
        .execute(&mut handshaker)
        .await
        .with_context(|| format!("Handshaking with {:?}.", addr))?;

    let signer = auth.create_signer(&hs_result.key);
    let codec = MessageCodec::new(signer);
    let connection = Framed::new(handshaker.into_inner(), codec);

    tracing::info!("Connected to {}", connection.get_ref().peer_addr()?);

    Ok(connection)
}

async fn connect_all(
    peers: &[Peer],
    auth: &AuthFactory<Client>,
) -> Result<Vec<TaggedConnection<Client>>> {
    let connectors = peers
        .iter()
        .map(|peer| connect((peer.host.as_str(), peer.client_port), auth));
    try_join_all(connectors).await
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let Cli {
        config_path,
        ignore,
    } = Cli::parse();
    let mut config = load_from_paths(&config_path).context("load config")?;
    config.set("id", random::<u32>()).expect("set own id");
    let peers: Vec<Peer> = config.get("peers")?;

    let crypto = HmacFactory::new(config.get("id").unwrap(), HMAC_SHA256);

    let mut connections = connect_all(&peers, &AuthFactory::Hmac(crypto))
        .await
        .context("Connecting to System failed.")?;
    tracing::info!("Connected to System.");

    let request = Message::broadcast(config.get("id")?, Request::new(0, Bytes::new()));
    let request = request.pack()?;
    try_join_all(
        connections
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| {
                let i = *i as u64;
                !ignore.contains(&i)
            })
            .map(|(_, stream)| stream)
            .map(|conn| conn.send(request.clone())),
    )
    .await?;

    let replies = try_join_all(
        connections
            .iter_mut()
            .map(|conn| conn.next().map(Option::unwrap)),
    )
    .await?;

    let first = &replies[0];
    if replies[1..].iter().all(|r| r.matches(first)) {
        tracing::info!("All replies equal!");
    } else {
        for reply in replies {
            tracing::info!("{:?}", reply);
        }
    }

    Ok(())
}
