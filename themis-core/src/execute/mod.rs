use crate::config::{Config, Execution};

use futures_util::future::{Future, FutureExt};

mod single_thread;
mod threadpool;

use single_thread::SingleThreadRuntime;
use threadpool::ThreadpoolRuntime;

enum RuntimeKind {
    Single(SingleThreadRuntime),
    Pool(ThreadpoolRuntime),
}

pub struct Runtime {
    executor: RuntimeKind,
}

impl Runtime {
    pub fn new(config: &Config) -> Self {
        let execution: Execution = config.get("execution").expect("execution");
        match execution {
            Execution::Single => Self::new_single(),
            Execution::Threadpool => Self::new_pool(),
        }
    }

    pub fn new_single() -> Self {
        Self {
            executor: RuntimeKind::Single(
                SingleThreadRuntime::new().expect("could not create single thread runtime"),
            ),
        }
    }

    pub fn new_pool() -> Self {
        Self {
            executor: RuntimeKind::Pool(
                ThreadpoolRuntime::new(0).expect("could not create threadpool runtime"),
            ),
        }
    }

    pub fn block_on(self, f: impl Future<Output = ()> + Send + 'static) {
        match self.executor {
            RuntimeKind::Single(thread) => thread.block_on(f),
            RuntimeKind::Pool(pool) => pool.block_on(f),
        }
    }

    pub fn spawn<T>(&mut self, future: T)
    where
        T: Future<Output = ()> + Send + 'static,
    {
        match self.executor {
            RuntimeKind::Single(ref mut e) => {
                e.spawn(future.boxed());
            }
            RuntimeKind::Pool(ref mut e) => {
                e.spawn(future.boxed());
            }
        };
    }
}

trait RuntimeTrait {
    // fn run(self);

    fn block_on(self, f: impl Future<Output = ()> + Send + 'static);

    fn spawn(&mut self, f: impl Future<Output = ()> + Send + 'static);
}

pub fn spawn<Fut>(f: Fut) -> tokio::task::JoinHandle<Fut::Output>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send,
{
    tokio::spawn(f)
}
