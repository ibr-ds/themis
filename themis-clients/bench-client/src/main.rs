use std::{
    collections::vec_deque::VecDeque,
    env::set_var,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use bytes::Bytes;
use clap::Parser;
use crossbeam_utils::{atomic::AtomicCell, sync::WaitGroup};
use futures_util::{stream::FuturesOrdered, StreamExt};

use themis_client::{Client, Destination, Request};
use themis_core::{
    config::{load_from_paths, Config, DEFAULT_CONFIG_PATH},
    execute::Runtime,
};
use tracing::Instrument;

struct Context {
    config: Arc<Config>,
    counter: Arc<AtomicCell<u64>>,
    complete: Arc<AtomicCell<u64>>,
    latencies: Arc<AtomicCell<u64>>,
}

fn setup_logging() {
    if std::env::var("RUST_LOG").is_err() {
        set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();
}

async fn do_requests<'a>(
    client: &'a mut Client,
    total: u64,
    concurrent: u64,
    ctx: &'a Context,
    payload: Bytes,
) -> themis_core::Result<()> {
    let mut active_reqs = 0;
    let mut buffer = FuturesOrdered::new();
    let mut times = VecDeque::new();
    let mut i = 0;
    while i < total {
        while active_reqs < concurrent && i + active_reqs < total {
            let response = client
                .request(
                    Request::new(i + active_reqs, payload.clone()),
                    Destination::Primary,
                )
                .await?;
            buffer.push(response);
            times.push_back(Instant::now());
            active_reqs += 1;
        }
        let _response = buffer.next().await.unwrap();
        let millis = times.pop_front().unwrap().elapsed().as_millis() as u64;
        ctx.latencies.fetch_add(millis);
        ctx.counter.fetch_add(1);
        active_reqs -= 1;
        i += 1;
    }

    Ok(())
}

#[derive(Debug, Parser)]
struct Opts {
    #[clap(short, long, default_value = "1")]
    clients: u64,
    #[clap(long, default_value = DEFAULT_CONFIG_PATH)]
    config: Vec<String>,
    #[clap(long, default_value = "10")]
    payload: usize,
    #[clap(long = "concurrent", short = 'a', default_value = "10")]
    concurrent_requests: u64,

    #[clap(
        long,
        short,
        conflicts_with = "duration",
        required_unless_present = "duration"
    )]
    requests: Option<u64>,

    #[clap(long, short, required_unless_present = "requests")]
    duration: Option<u64>,

    #[clap(long)]
    quiet: bool,

    #[clap(long = "no-wait")]
    no_wait_for_primary: bool,
}

fn main() {
    setup_logging();

    let opts = Opts::parse();
    println!(
        "RUST_LOG={}",
        std::env::var("RUST_LOG").unwrap_or("".into())
    );
    println!("{:#?}", opts);
    let Opts {
        clients,
        config: config_path,
        payload,
        concurrent_requests: concurrent,
        requests,
        duration,
        quiet,
        no_wait_for_primary,
    } = opts;

    let counter = Arc::new(AtomicCell::new(0));
    let complete = Arc::new(AtomicCell::new(clients));
    let latencies = Arc::new(AtomicCell::new(0));

    let mut config = load_from_paths(&config_path).expect("load config");
    config
        .set("client.wait_for_primary", !no_wait_for_primary)
        .expect("set");

    let start = Instant::now();

    let req_per_client = requests.unwrap_or(u64::max_value()) / clients;

    let payload = Bytes::from(vec![0; payload]);

    let connected = WaitGroup::new();

    for i in 0..clients {
        let mut config = config.clone();
        let id = 100 + i;
        config.set("id", id).expect("set");
        let connected = connected.clone();
        let payload = payload.clone();
        let ctx = Context {
            config: config.into(),
            counter: counter.clone(),
            complete: complete.clone(),
            latencies: latencies.clone(),
        };

        thread::spawn(move || {
            let payload = payload.clone();

            let runtime = Runtime::new(&ctx.config);

            let cf = async move {
                let mut c = Client::connect(ctx.config.clone())
                    .await
                    .expect("could not create client");
                connected.wait();
                if let Err(e) = do_requests(&mut c, req_per_client, concurrent, &ctx, payload).await
                {
                    tracing::error!("Client {}: {}", i, e);
                }
                drop(c);
                ctx.complete.fetch_sub(1);
            }
            .instrument(tracing::info_span!("client_thread", id));
            let _start = Instant::now();
            runtime.block_on(cf);
            tracing::warn!("pool shutdown");
        });
    }

    let mut seconds = 0;

    let smooth: usize = 3;

    let mut rps_vec = Vec::new();
    let mut lag_vec = Vec::new();

    tracing::info!("waiting for {} connected clients", clients);
    connected.wait();
    tracing::info!("go");
    loop {
        thread::sleep(Duration::from_secs(1));
        seconds += 1;
        tracing::debug!("Elapsed time {}", start.elapsed().as_millis());
        let rps = counter.swap(0);
        let lag = latencies.swap(0);
        rps_vec.push(rps);
        lag_vec.push(lag);

        let smoother = rps_vec.iter().rev().take(smooth);
        let actual = smoother.len();
        let total: u64 = smoother.sum();
        let avg_lag: u64 = if total > 0 {
            lag_vec.iter().rev().take(smooth).sum::<u64>() / total as u64
        } else {
            0
        };
        let reqs: u64 = total / actual as u64;

        if !quiet {
            tracing::info!("RPS: {}", reqs);
            tracing::info!("LAG: {}", avg_lag);
        }

        if complete.load() == 0 || seconds == duration.unwrap_or(u64::max_value()) {
            break;
        }
    }

    let len = rps_vec.len() as u64;
    let mut avg_rps: u64 = rps_vec.iter().sum::<u64>() / len;
    if avg_rps == 0 {
        avg_rps = 1;
    }
    let avg_lag: u64 = lag_vec.iter().sum::<u64>() / len;
    // tracing::info!("{:?}", rps_vec);
    tracing::info!("Total rps: {}", avg_rps);
    tracing::info!("Total lag: {}", avg_lag / avg_rps);
}
