use bytes::Bytes;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Get { key: String },
    Set { key: String, value: Bytes },
    Delete { key: String },
    // ExportState,
    // ImportState { state: Bytes },
}

#[derive(Error, Debug, Serialize, Deserialize)]
pub enum StoreError {
    #[error("failed (de-)serialization")]
    SerdeError,
}

pub type Return = Result<Option<Bytes>, StoreError>;
