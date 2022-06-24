use futures_util::future::Future;
use std::io;

use super::RuntimeTrait;

use tokio::runtime::{Builder, Runtime};

pub struct ThreadpoolRuntime {
    pool: Runtime,
}

impl ThreadpoolRuntime {
    pub fn new(pool_size: usize) -> io::Result<Self> {
        let mut builder = Builder::new_multi_thread();
        builder.enable_all();
        if pool_size > 0 {
            builder.worker_threads(pool_size);
        }
        let pool = builder.build()?;
        Ok(Self { pool })
    }
}

impl RuntimeTrait for ThreadpoolRuntime {
    fn block_on(self, f: impl Future<Output = ()> + Send + 'static) {
        self.pool.block_on(f);
    }

    fn spawn(&mut self, f: impl Future<Output = ()> + Send + 'static) {
        self.pool.spawn(f);
    }
}
