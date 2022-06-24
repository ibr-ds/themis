use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures_util::stream::StreamExt;
use themis_core::{
    comms::unbounded,
    protocol::batching::{BatchConfig, Batchable, Batcher},
};
use tokio::{sync::Semaphore, time::sleep};

struct Item {
    _source: u64,
    _sequence: u64,
    created: Instant,
    response: Arc<Semaphore>,
}

impl Batchable for Item {
    fn should_batch(&self) -> bool {
        true
    }
}

#[tokio::main]
async fn main() {
    let config = BatchConfig {
        min: 1,
        max: 100,
        timeout: Duration::from_millis(500).into(),
        rel_timeout: Duration::from_millis(5).into(),
        ..Default::default()
    };

    let workers = 200;
    let max_reqs = 100;

    let (tx, rx) = unbounded();

    for source in 0..workers {
        let mut tx = tx.clone();
        tokio::spawn(async move {
            let sem = Arc::new(Semaphore::new(0));
            for sequence in 0..max_reqs {
                sleep(Duration::from_millis(5)).await;
                tx.send(Item {
                    _source: source,
                    _sequence: sequence,
                    response: sem.clone(),
                    created: Instant::now(),
                })
                .await
                .unwrap();
                let _ = sem.acquire().await.unwrap();
            }
        });
    }
    drop(tx);

    let mut batcher = Batcher::new(rx, config);
    let begin = Instant::now();
    let mut reqs = 0;
    while let Some(batch) = batcher.next().await {
        for p in &batch {
            p.response.add_permits(1);
        }
        let duration: Duration =
            batch.iter().map(|b| b.created.elapsed()).sum::<Duration>() / batch.len() as u32;
        println!("size {}, avg {}ms", batch.len(), duration.as_millis());
        reqs += batch.len();
    }
    let elapsed = begin.elapsed();
    let time = elapsed.as_millis();
    let rps = reqs as f64 / time as f64;
    println!("{}ms, {} rps", time, rps);
}
