use std::{
    fs::read_to_string,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use error::Error;
pub use themis_config::Config;

use crate::protocol::batching::BatchConfig;

pub const DEFAULT_CONFIG_PATH: &str = "config/default/";

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClientPeer {
    pub id: usize,
    pub host: String,
    pub client_port: u16,
    pub public_key: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Peer {
    pub id: usize,
    pub host: String,
    pub bind: Option<String>,
    pub client_port: u16,
    pub peer_port: u16,
    pub public_key: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Defaults {
    pub id: usize,
    pub execution: Execution,
    pub batch: BatchConfig,
    pub peers: Vec<Peer>,
}

impl Default for Defaults {
    fn default() -> Self {
        Self {
            id: 0,
            execution: Execution::Single,
            batch: BatchConfig::default(),
            peers: vec![
                Peer {
                    id: 0,
                    host: "::1".to_string(),
                    bind: Some("::1".to_string()),
                    client_port: 10000,
                    peer_port: 10001,
                    public_key: PathBuf::default(),
                },
                Peer {
                    id: 1,
                    host: "::1".to_string(),
                    bind: Some("::1".to_string()),
                    client_port: 10100,
                    peer_port: 10101,
                    public_key: PathBuf::default(),
                },
                Peer {
                    id: 2,
                    host: "::1".to_string(),
                    bind: Some("::1".to_string()),
                    client_port: 10200,
                    peer_port: 10201,
                    public_key: PathBuf::default(),
                },
                Peer {
                    id: 3,
                    host: "::1".to_string(),
                    bind: Some("::1".to_string()),
                    client_port: 10300,
                    peer_port: 10301,
                    public_key: PathBuf::default(),
                },
            ],
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum Execution {
    Threadpool,
    Single,
}

impl Default for Execution {
    fn default() -> Self {
        Execution::Single
    }
}

pub const RUST_LOG: &str = "RUST_LOG";
pub fn setup_logging() {
    if std::env::var(RUST_LOG).is_err() {
        std::env::set_var(RUST_LOG, "info");
    }
}

pub fn load_from_value<S: Serialize>(value: S) -> Result<Config, Box<dyn std::error::Error>> {
    let config = Config {
        root: value.serialize(themis_config::Serializer)?,
    };

    Ok(config)
}

fn load_from_dir(path: &Path, config: &mut Config) -> Result<(), Error> {
    assert!(path.is_dir());

    let mut dir = std::fs::read_dir(path).map_err(|source| Error::Io {
        source,
        path: path.to_owned(),
    })?;
    while let Some(Ok(entry)) = dir.next() {
        let file_type = entry.file_type().map_err(|source| Error::Io {
            source,
            path: path.to_owned(),
        })?;
        if file_type.is_file() {
            let bytes = read_to_string(entry.path()).map_err(|source| Error::Io {
                source,
                path: path.to_owned(),
            })?;
            let mut de = toml::Deserializer::new(&bytes);
            config
                .merge_from_deserializer(&mut de)
                .map_err(|source| Error::Toml {
                    source,
                    path: path.to_owned(),
                })?;
        }
    }

    Ok(())
}

pub fn load_from_paths<S: AsRef<Path>>(files: &[S]) -> Result<Config, Error> {
    let mut config = themis_config::Config::default();

    for path in files.iter().map(AsRef::as_ref) {
        if path.is_dir() {
            tracing::trace!(?path, "loading directory");
            load_from_dir(path, &mut config)?;
        } else {
            tracing::trace!(?path, "loading file");
            let bytes = read_to_string(path).map_err(|source| Error::Io {
                source,
                path: path.to_owned(),
            })?;
            let mut de = toml::Deserializer::new(&bytes);
            config
                .merge_from_deserializer(&mut de)
                .map_err(|e| Error::Toml {
                    path: path.to_owned(),
                    source: e,
                })?;
        }
    }

    Ok(config)
}

pub fn default() -> Config {
    load_from_value(Defaults::default()).expect("load defaults")
}

pub mod well_known {
    use themis_config::Config;

    pub fn num_peers(config: &Config) -> Option<usize> {
        config.get::<Vec<()>>("peers").ok().map(|v| v.len())
    }
}

mod error {
    use std::{io, path::PathBuf};

    use thiserror::Error;

    #[derive(Debug, Error)]
    pub enum Error {
        #[error("Cannot deserialize {}: {}.", path.display(), source)]
        Toml {
            source: themis_config::MergeError<toml::de::Error>,
            path: PathBuf,
        },
        #[error("Cannot open {}: {}", path.display(), source)]
        Io { source: io::Error, path: PathBuf },
    }
}

#[cfg(test)]
mod test {
    use crate::config::{self, load_from_paths, DEFAULT_CONFIG_PATH};

    #[test]
    #[cfg_attr(miri, ignore)]
    fn load_paths() {
        let mut dir = std::env::current_dir().unwrap();

        if dir.ends_with("themis-core") {
            dir.pop();
            std::env::set_current_dir(dir).unwrap();
        }

        let _config = dbg!(load_from_paths(&[DEFAULT_CONFIG_PATH]));
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn num_peers() {
        let _ = tracing_subscriber::fmt::try_init();

        let mut dir = std::env::current_dir().unwrap();

        if dir.ends_with("themis-core") {
            dir.pop();
            std::env::set_current_dir(dir).unwrap();
        }

        let config = dbg!(load_from_paths(&[DEFAULT_CONFIG_PATH])).unwrap();
        dbg!(config::well_known::num_peers(&config)).unwrap();
    }
}
