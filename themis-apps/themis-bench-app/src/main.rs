use std::{env, sync::Arc};

use bytes::Bytes;
use clap::Parser;
use log::info;

use futures_util::try_join;

use themis_core::{
    authentication::{create_crypto_replicas, Cryptography},
    comms as channel,
    config::{load_from_paths, setup_logging, Config, Execution, Peer},
    modules,
};
use themis_pbft::PBFT;
use tracing_subscriber::EnvFilter;

use crate::app::CounterApp;
use themis_core::config::DEFAULT_CONFIG_PATH;

#[cfg(feature = "metrics")]
use themis_core::execute::spawn;
#[cfg(feature = "metrics")]
use themis_core::metrics_server::start_server;

mod app;

async fn setup(config: Arc<Config>) {
    let me: usize = config.get("id").expect("id");
    let _keyfile: String = config
        .get(&format!("peers[{}].private_key", me))
        .expect("keyfile");
    let _peers: Vec<Peer> = config.get("peers").expect("peers");

    let crypto = create_crypto_replicas(&config);
    let verifier = crypto.create_signer(&Bytes::new());

    let peer_out = channel::unbounded();
    let peer_in = channel::unbounded();
    let peers = modules::peers(peer_in.0, peer_out.1, crypto, config.clone(), None);
    // let peers = themis_quic::peers(peer_in.0, peer_out.1, crypto, config.clone());

    let client_out = channel::unbounded();
    let client_in = channel::unbounded();
    let clients = modules::clients(client_in.0.clone(), client_out.1, config.clone());

    let app_in = channel::unbounded();
    let app_out = channel::unbounded();

    let protocol = PBFT::new(
        me as u64,
        peer_out.0,
        client_out.0,
        app_in.0,
        verifier,
        &config,
    );
    let protocol = modules::protocol(protocol, peer_in.1, client_in.1, app_out.1, &config);

    let app = CounterApp::new(me as u64, &config);
    let application = modules::application(me as u64, app, app_in.1, app_out.0);

    #[cfg(feature = "metrics")]
    let port: u16 = config
        .get(&format!("peers[{}].prometheus_port", me))
        .expect("prometheus port");
    #[cfg(feature = "metrics")]
    let _metrics = spawn(start_server(port));

    info!("setup modules");

    let state = try_join!(peers, clients, protocol, application);
    if let Err(e) = state {
        log::error!("Fatal: {}", e);
    }
}

#[derive(Debug, Parser)]
struct Opts {
    #[clap(long = "config", short, default_value = DEFAULT_CONFIG_PATH)]
    configs: Vec<String>,
    id: u64,
}

fn main() {
    setup_logging();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(atty::is(atty::Stream::Stdout))
        .init();

    let Opts { configs, id: me } = Opts::parse();

    let mut config = load_from_paths(&configs).expect("load config");
    config.set("id", me).expect("set id");
    let config = Arc::new(config);

    log::info!("STARTING REPLICA {}", me);
    log::info!(
        "Logging: {}",
        env::var("RUST_LOG").unwrap_or_else(|_| "not set".into())
    );

    let rt = config.get("execution").unwrap_or(Execution::Threadpool);
    log::info!("selected runtime {:?}", rt);
    let mut rt = match rt {
        Execution::Single => tokio::runtime::Builder::new_current_thread(),
        Execution::Threadpool => tokio::runtime::Builder::new_multi_thread(),
    };
    let rt = rt.enable_time().enable_io().build().unwrap();
    rt.block_on(async move { setup(config).await })
}
