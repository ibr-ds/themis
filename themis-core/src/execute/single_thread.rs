use futures_util::future::Future;

use std::io;
use tokio::{runtime, runtime::Runtime};

use super::RuntimeTrait;

pub struct SingleThreadRuntime {
    current: Runtime,
}

impl SingleThreadRuntime {
    pub fn new() -> io::Result<Self> {
        let current = runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        Ok(Self { current })
    }
}

impl RuntimeTrait for SingleThreadRuntime {
    fn block_on(self, f: impl Future<Output = ()> + Send + 'static) {
        self.current.block_on(f);
    }

    fn spawn(&mut self, f: impl Future<Output = ()> + Send + 'static) {
        self.current.spawn(f);
    }
}
