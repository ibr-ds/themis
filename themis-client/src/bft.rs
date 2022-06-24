use std::{
    future::Future,
    io::{self, ErrorKind},
    time::Duration,
};

use fnv::FnvHashMap;
use futures_util::{
    future::TryFutureExt as _,
    stream::{FuturesUnordered, StreamExt as _},
};
use snafu::{futures::TryFutureExt as _, ResultExt};
use tokio::{sync::oneshot, time::timeout};

use error::{Error, Result};
use themis_core::{
    app::{Client as ClientTag, Request, Response},
    comms::{unbounded, Receiver, Sender},
    config,
    net::{Message, RawMessage, Sequenced},
    protocol::quorum::{self, Quorum},
    utils::Fuse,
};

use crate::{Destination, RequestItem};
use themis_core::config::Config;

const CLIENT_TIMEOUT: u64 = 2000;

#[derive(Debug)]
struct ActiveRequest {
    responses_s: Sender<Message<Response>>,
    response_s: oneshot::Sender<Message<Response>>,
}

impl ActiveRequest {
    pub fn new(
        responses_s: Sender<Message<Response>>,
        response_s: oneshot::Sender<Message<Response>>,
    ) -> Self {
        Self {
            responses_s,
            response_s,
        }
    }
}

#[derive(Debug)]
struct ClientState {
    active_reqs: FnvHashMap<u64, ActiveRequest>,
    quorums: Fuse<FuturesUnordered<RequestFuture>>,
    requests_to_group_s: Sender<RawMessage<ClientTag>>,
    contact: u64,
    me: u64,
    peers: usize,
}

#[derive(Debug)]
pub(crate) struct BftClient {
    requests_r: Receiver<RequestItem>,
    replies_r: Receiver<RawMessage<ClientTag>>,
    state: ClientState,
}

impl BftClient {
    pub fn new(
        requests_r: Receiver<RequestItem>,
        replies_r: Receiver<RawMessage<ClientTag>>,
        requests_to_group_s: Sender<RawMessage<ClientTag>>,
        config: &Config,
    ) -> Self {
        let me = config.get("id").expect("id");
        let peers = config::well_known::num_peers(config).expect("num peers");

        Self {
            requests_r,
            replies_r,
            state: ClientState {
                active_reqs: Default::default(),
                quorums: Default::default(),
                requests_to_group_s,
                contact: 0,
                me,
                peers,
            },
        }
    }
}

impl BftClient {
    pub async fn execute(mut self) -> themis_core::Result<()> {
        let mut requests_r = self.requests_r.fuse();
        let mut replies_r = self.replies_r.fuse();

        loop {
            tokio::select! {
                Some(request) = requests_r.next() => {
                    self.state.new_request(request, self.state.requests_to_group_s.clone()).await?;
                }
                Some(response) = replies_r.next() => {
                    let response = match response.unpack() {
                        Ok(r) => r,
                        Err(_e) => continue,
                    };
                    self.state.new_response(response).await?;
                }
                Some(response) = self.state.quorums.next() => {
                    match response {
                        Err(e) =>  log::error!("Quorum failed: {:?}", e),
                        Ok(quorum) => self.state.handle_complete_quorum(quorum).await,
                    }
                },
                else => {
                    log::debug!("bft client layer died");
                    break Ok::<_, themis_core::Error>(());
                }
            }
        }?;
        log::error!("bft loop exited");
        Ok(())
    }
}

impl ClientState {
    async fn handle_complete_quorum(&mut self, msg: Message<Response>) {
        let sequence = msg.sequence();
        let item = self.active_reqs.remove(&sequence);
        if let Some(ActiveRequest { response_s, .. }) = item {
            if let Some(contact) = msg.inner.contact {
                self.contact = contact;
                log::debug!("Contact changed to {}.", self.contact);
            };
            if let Err(_e) = response_s.send(msg) {
                log::error!("Interest in {} lost", sequence);
            }
        } else {
            log::warn!("Cannot complete quorum, map entry is gone.")
        }
    }

    async fn new_request(
        &mut self,
        request: RequestItem,
        req_sender: Sender<RawMessage<ClientTag>>,
    ) -> themis_core::Result<()> {
        let RequestItem {
            req,
            dest,
            response_to,
        } = request;
        let request = match dest {
            Destination::Primary => Message::new(self.me, self.contact, req),
            Destination::Specific(dest) => Message::new(self.me, dest, req),
            Destination::Broadcast => Message::broadcast(self.me, req),
        };

        let chan = unbounded();

        let previous = self
            .active_reqs
            .insert(request.sequence(), ActiveRequest::new(chan.0, response_to));
        assert!(previous.is_none());

        self.quorums
            .0
            .push(make_request_future(request, req_sender, chan.1, self.peers));

        Ok(())
    }

    async fn new_response(&mut self, response: Message<Response>) -> themis_core::Result<()> {
        let seq = response.sequence();
        log::debug!("Response for {} from {}", seq, response.source);
        if let Some(ActiveRequest { responses_s, .. }) = self.active_reqs.get_mut(&seq) {
            responses_s.send(response).await?;
        }

        Ok(())
    }
}

#[inline(always)]
fn make_request_future(
    request: Message<Request>,
    sender: Sender<RawMessage<ClientTag>>,
    recv: Receiver<Message<Response>>,
    peers: usize,
) -> RequestFuture {
    let f = handle_request(request, sender, recv, peers);
    async move { f.await }
}

type RequestFuture = impl Future<Output = Result<Message<Response>>>;

impl From<io::Error> for Error {
    fn from(source: io::Error) -> Self {
        Self::Io { source }
    }
}

impl From<quorum::Error> for Error {
    fn from(_: quorum::Error) -> Self {
        Self::BadQuorum
    }
}

async fn try_complete_quorum(
    recv: &mut Receiver<Message<Response>>,
    quorum: &mut Quorum<Message<Response>>,
) -> Result<Message<Response>> {
    while let Some(msg) = recv.next().await {
        log::debug!(
            "response. sequence={} source={}",
            msg.sequence(),
            msg.source
        );
        let state = quorum.offer(msg)?;
        if let quorum::State::Finished(_) = state {
            return quorum.take_and_clear().ok_or(Error::BadQuorum);
        }
    }
    Err(Error::Io {
        source: io::Error::new(ErrorKind::BrokenPipe, "Receiver closed."),
    })
}

fn handle_request(
    mut request: Message<Request>,
    mut sender: Sender<RawMessage<ClientTag>>,
    mut recv: Receiver<Message<Response>>,
    peers: usize,
) -> impl Future<Output = Result<Message<Response>>> {
    let mut quorum = Quorum::new(peers / 3 + 1, peers);
    async move {
        sender
            .send(request.pack().unwrap())
            .await
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "Sender shut down"))
            .context(IoSnafu)?;
        match timeout(
            Duration::from_millis(CLIENT_TIMEOUT),
            try_complete_quorum(&mut recv, &mut quorum),
        )
        .await
        {
            Err(_elapsed) => (),
            Ok(result) => return result,
        };
        log::debug!("timeout. sequence={}", request.sequence());
        request.make_broadcast();
        sender
            .send(request.pack().unwrap())
            .map_err(|_| io::Error::new(ErrorKind::BrokenPipe, "Sender shut down"))
            .context(IoSnafu)
            .await?;
        try_complete_quorum(&mut recv, &mut quorum).await
    }
}

use error::IoSnafu;
mod error {
    use std::io;

    use snafu::Snafu;

    pub(super) type Result<T> = std::result::Result<T, Error>;

    #[derive(Debug, Snafu)]
    #[snafu(visibility(pub(super)))]
    pub enum Error {
        #[snafu(display("Request ended in IO error."))]
        Io { source: io::Error },
        #[snafu(display("Request did not complete a quorum."))]
        BadQuorum,
    }
}
