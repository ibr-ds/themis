use std::{
    env::set_var,
    io::{self, BufRead},
    sync::Arc,
    time::Instant,
};

use clap::Parser;
use futures_util::{future::try_join_all, stream::StreamExt};
use rand::{random, Rng};

use bytes::Bytes;
use themis_client::{Client, Destination, Request};
use themis_core::{
    app::RequestFlags,
    comms::unbounded,
    config::{load_from_paths, Config, DEFAULT_CONFIG_PATH},
    execute::Runtime,
};

fn create_message(seq: u64) -> Request {
    let mut payload = [0; 10];
    let mut rng = rand::thread_rng();
    rng.fill(&mut payload);
    Request {
        sequence: seq,
        flags: RequestFlags::empty(),
        payload: Bytes::copy_from_slice(payload.as_ref()),
    }
}

fn setup_logging() {
    if std::env::var("RUST_LOG").is_err() {
        set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();
}

#[derive(Debug, Parser)]
struct Opts {
    #[clap(long="config", default_value = DEFAULT_CONFIG_PATH)]
    config_path: Vec<String>,
    #[clap(short, long)]
    contact: u64,
    #[clap(short, long)]
    requests: u64,
    #[clap(short, long)]
    immediate: bool,
}

fn main() {
    setup_logging();

    let Opts {
        config_path,
        contact,
        requests,
        immediate,
    } = Opts::parse();
    let contact = Destination::Specific(contact);

    let mut sequence = 0;
    let mut config: Config = load_from_paths(&config_path).expect("cannot load config");
    config.set("id", random::<u32>()).expect("set id");
    let config = Arc::new(config);

    let (mut tx, mut rx) = unbounded();

    let thread = std::thread::spawn(move || {
        let r = Runtime::new_single();
        let f = async move {
            let mut bftclient = Client::connect(config.clone())
                .await
                .expect("could not create client");

            while rx.next().await.is_some() {
                log::info!("sending",);
                let start = Instant::now();
                let mut futures = Vec::with_capacity(requests as usize);
                for _ in 0..requests {
                    log::info!("sending request {}", sequence);
                    let msg = create_message(sequence);
                    let req = bftclient.request(msg, contact).await.expect("send request");
                    futures.push(async move {
                        let resp = req.await;
                        log::info!("request {} complete", sequence);
                        resp
                    });

                    sequence += 1;
                }
                try_join_all(futures).await.unwrap();

                log::info!("received reply, time: {}µs", start.elapsed().as_micros());
            }
            drop(bftclient);
            log::info!("done");
        };
        r.block_on(f);
    });

    if immediate {
        tx.unbounded_send(()).unwrap();
        drop(tx);
    } else {
        let mut line = String::new();

        let stdin = io::stdin();
        let mut stdin = stdin.lock();

        while stdin.read_line(&mut line).is_ok() {
            tx.unbounded_send(()).unwrap();
        }
    }
    thread.join().unwrap();
}
