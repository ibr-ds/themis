#![feature(type_alias_impl_trait)]
#![recursion_limit = "256"]
#![allow(dead_code)]
#![warn(rust_2018_idioms)]
//#![warn(missing_docs)]
//#![deny(warnings)]

pub use self::error::*;

pub mod app;
pub mod authentication;
pub mod comms;
pub mod config;
mod error;
pub mod execute;
pub mod modules;
pub mod net;
pub mod protocol;
pub mod utils;

#[cfg(feature = "metrics")]
pub mod metrics_server;
