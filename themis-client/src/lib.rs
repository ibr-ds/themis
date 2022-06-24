#![feature(type_alias_impl_trait)]
#![recursion_limit = "256"]

use std::{future::Future, io, pin::Pin, task::Poll};

use futures_util::{task::Context, FutureExt, StreamExt, TryFutureExt};
use tokio::{runtime::Runtime, sync::oneshot};

use themis_core::{
    app,
    authentication::{create_crypto_clients, AuthFactory},
    comms::{unbounded, Sender},
    config::ClientPeer,
    execute::spawn,
    net::{self, Group},
    Error, Result,
};
pub use themis_core::{
    app::{Request, Response},
    net::Message,
};

use crate::bft::BftClient;
use std::sync::Arc;
use themis_core::config::Config;

mod bft;

#[derive(Debug, Copy, Clone)]
pub enum Destination {
    Primary,
    Specific(u64),
    Broadcast,
}

#[derive(Debug)]
struct RequestItem {
    req: Request,
    dest: Destination,
    response_to: oneshot::Sender<Message<Response>>,
}

impl RequestItem {
    pub fn new(
        req: Request,
        dest: Destination,
        response_to: oneshot::Sender<Message<Response>>,
    ) -> Self {
        Self {
            req,
            dest,
            response_to,
        }
    }
}

#[derive(Clone)]
pub struct Client {
    sender: Sender<RequestItem>,
    primary: u64,
}

impl Client {
    pub async fn connect(config: Arc<Config>) -> io::Result<Self> {
        // let me = config.get("id").expect("id");
        let peers: Vec<ClientPeer> = config.get("peers").expect("peers");

        let (group_to_bft_s, group_to_bft_r) = unbounded();
        let (client_to_bft_s, client_to_bft_r) = unbounded();
        let (bft_to_group_s, bft_to_group_r) = unbounded();
        let crypto: AuthFactory<app::Client> = create_crypto_clients(&config);

        let group = Group::new(bft_to_group_r, group_to_bft_s, crypto.clone(), None);

        let new_conns = group.conn_sender();

        let bft = BftClient::new(client_to_bft_r, group_to_bft_r, bft_to_group_s, &config);

        spawn(async {
            if let Err(e) = bft.execute().await {
                log::error!("Bft component error: {}", e);
            } else {
                log::error!("Bft component exited");
            }
        });

        spawn(async {
            group.execute().await;
            log::error!("netgroup exited");
        });

        let (notify_s, mut notify_r) = unbounded();
        let num_peers = peers.len();
        for (i, replica) in peers.into_iter().enumerate() {
            let (host, port) = (replica.host, replica.client_port);
            let sink = new_conns.clone();

            log::trace!("connecting to {}:{}", host, port);
            let mut n = notify_s.clone();
            let crypto = crypto.clone();
            spawn(async move {
                match net::connect((host.as_str(), port), sink, crypto).await {
                    Ok(_) => {
                        let _ = n.send(i as u64).await;
                    }
                    Err(e) => log::error!("Connect replica {}: {}", i, e),
                }
            });
        }

        // ensure f+1 connections.

        let f = num_peers / 3;
        let mut total = 0;
        let primary: u64 = 0;
        let wait_for_primary: bool = config.get("client.wait_for_primary").unwrap_or(true);
        let mut has_primary = !wait_for_primary;

        while let Some(id) = notify_r.next().await {
            total += 1;
            if id == primary {
                has_primary = true
            }
            if total >= f && has_primary {
                break;
            }
        }

        Ok(Self {
            sender: client_to_bft_s,
            primary: 0,
        })
    }

    pub fn get_primary(&self) -> u64 {
        self.primary
    }

    pub async fn request(
        &mut self,
        request: Request,
        destination: Destination,
    ) -> Result<ResponseFuture> {
        let (tx, rx) = oneshot::channel();

        self.sender
            .send(RequestItem::new(request, destination, tx))
            .await?;

        Ok(ResponseFuture(rx))
    }

    pub fn request_blocking(
        &mut self,
        request: Request,
        destination: Destination,
    ) -> Result<Message<Response>> {
        Runtime::new()
            .unwrap()
            .block_on(self.request(request, destination).and_then(|f| f))
    }
}

#[derive(Debug)]
pub struct ResponseFuture(oneshot::Receiver<Message<Response>>);

impl Future for ResponseFuture {
    type Output = Result<Message<Response>>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(ctx).map_err(|e| Error::application(e))
    }
}
