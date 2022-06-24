mod connection;
mod endpoint;
mod group;

pub use endpoint::*;
pub use group::*;
use tokio::net::TcpStream;

use super::Connection;

type TcpConnection = Connection<TcpStream>;
