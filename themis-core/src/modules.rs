use std::sync::Arc;

use futures_util::TryFutureExt;

use crate::{net::Bootstrap, Result};

use crate::{
    app::{self, Application, ApplicationWorker, Client, Response},
    authentication::{create_crypto_clients, AuthFactory},
    comms::{Receiver, Sender},
    config::{Config, Peer},
    execute::spawn,
    net::{self, Endpoint, Message, Raw, Tag},
    protocol::{Protocol2, ProtocolWorker},
};

/// Provides methods to create common modules such as client, peer and application modules.
/// For example, a client module consists of an endpoint to accept new connections and a group that
/// manages all connections and receives and sends messages.
/// A peer module additionally contains a handshake to identify peers.
/// TODO how to construct modules, parameters, return?

/// Spawns a client module
pub async fn clients(
    tx: Sender<Message<Raw<Client>>>,
    rx: Receiver<Message<Raw<Client>>>,
    config: Arc<Config>,
) -> Result<()> {
    let id: usize = config.get("id").expect("id");
    let peers: Vec<Peer> = config.get("peers").expect("peers");
    let me = &peers[id];
    let bind = me.bind.as_deref().unwrap_or_else(|| "::");

    let crypto = create_crypto_clients(&config);

    let group = net::Group::new(rx, tx, crypto.clone(), None);
    let client_ep = Endpoint::bind(
        id as u64,
        (bind, me.client_port),
        group.conn_sender(),
        crypto,
    )
    .await;

    // listener is fine do shut down
    let _listener = spawn(client_ep.listen());
    let group = spawn(group.execute());

    group
        .map_err(|e| crate::Error::Join {
            module: "clients".into(),
            source: e,
        })
        .await
}

/// Spawns a replica communications module
pub async fn peers<T: Tag>(
    tx: Sender<Message<Raw<T>>>,
    rx: Receiver<Message<Raw<T>>>,
    crypto: AuthFactory<T>,
    config: Arc<Config>,
    bootstrap: Option<Bootstrap>,
) -> Result<()> {
    let id: usize = config.get("id").expect("id");
    let peers: Vec<Peer> = config.get("peers").expect("peers");
    let me = &peers[id];

    let bind = me.bind.as_deref().unwrap_or_else(|| "::");
    let group = net::Group::new(rx, tx, crypto.clone(), bootstrap);
    let peer_ep = Endpoint::bind(
        id as u64,
        (bind, me.peer_port),
        group.conn_sender(),
        crypto.clone(),
    )
    .await;

    // peers[0] to peers[id-1]
    for peer in peers.into_iter().take(id) {
        let sink = group.conn_sender();
        let handshake = crypto.clone();
        spawn(async move {
            let host = peer.host;
            let port = peer.peer_port;
            let connector = net::connect((host.as_str(), port), sink, handshake);
            tracing::trace!("Attempting to connect to peer {}", peer.id);
            match connector.await {
                Err(e) => tracing::warn!("{}", e),
                Ok(()) => (),
            }
        });
    }

    // listener is fine do shut down
    let _listener = spawn(peer_ep.listen());
    let group = spawn(group.execute());

    group
        .map_err(|e| crate::Error::Join {
            module: "peers".into(),
            source: e,
        })
        .await
}

/// Spawns a protocol module
pub async fn protocol<T>(
    protocol: T,
    replicas: Receiver<Message<Raw<T::Messages>>>,
    clients: Receiver<Message<Raw<Client>>>,
    app: Receiver<Message<Response>>,
    config: &Config,
) -> Result<()>
where
    for<'some> T: Protocol2<'some> + 'static,
    T::Item: Send,
{
    let worker = ProtocolWorker::new(protocol, replicas, clients, app);

    let batchcfg = config.get("batch").expect("batchcfg");
    let batching = config.get("batching").unwrap_or(false);
    tracing::info!("batching={}", batching);
    tracing::info!("{:#?}", batchcfg);
    let prot = spawn(worker.execute(batching, batchcfg));
    prot.map_err(|e| crate::Error::Join {
        module: "protocol".into(),
        source: e,
    })
    .await??;
    Ok(())
}

/// Spawns a application module
pub async fn application<T>(
    uuid: u64,
    app: T,
    rx: Receiver<app::Command>,
    tx: Sender<Message<Response>>,
) -> Result<()>
where
    for<'app> T: Application<'app> + Sync + 'static,
{
    let app = ApplicationWorker::new(uuid, rx, tx, app);
    let app = spawn(app.execute());
    app.map_err(|e| crate::Error::Join {
        module: "application".into(),
        source: e,
    })
    .await?
}
