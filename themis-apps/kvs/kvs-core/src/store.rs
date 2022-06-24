use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::messages::*;

pub type KVMap = BTreeMap<String, Bytes>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Store {
    storage: KVMap,
}

impl Store {
    pub fn new() -> Store {
        let storage = BTreeMap::new();
        Store { storage }
    }

    pub fn set(&mut self, key: String, value: Bytes) -> Return {
        Ok(self.storage.insert(key, value))
    }

    // returns a copy of the value
    pub fn get(&mut self, key: &str) -> Return {
        match self.storage.get(key) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: &str) -> Return {
        Ok(self.storage.remove(key))
    }

    pub fn export_state(&mut self) -> Bytes {
        let serialized_state: Vec<u8> =
            rmp_serde::to_vec_named(&self.storage).expect("Serde error");
        Bytes::from(serialized_state)
    }

    pub fn import_state(&mut self, serialized_state: Bytes) -> Return {
        let new_map: KVMap = match rmp_serde::from_slice(&serialized_state[..]) {
            Ok(map) => map,
            Err(_) => return Err(StoreError::SerdeError),
        };
        self.storage = new_map;
        Ok(None)
    }

    pub fn execute(&mut self, op: Bytes) -> Bytes {
        let operation: Operation = match rmp_serde::from_slice(&op) {
            Ok(operation) => operation,
            Err(_) => {
                return Bytes::from(
                    rmp_serde::to_vec(&StoreError::SerdeError).expect("serde failed repeatedly"),
                )
            }
        };
        match operation {
            Operation::Get { key } => {
                let result = self.get(&key);
                let serialized_result = match rmp_serde::to_vec(&result) {
                    Ok(r) => r,
                    Err(_) => {
                        rmp_serde::to_vec(&StoreError::SerdeError).expect("serde failed repeatedly")
                    }
                };
                Bytes::from(serialized_result)
            }
            Operation::Set { key, value } => {
                let result = self.set(key, value);
                let serialized_result = match rmp_serde::to_vec(&result) {
                    Ok(r) => r,
                    Err(_) => {
                        rmp_serde::to_vec(&StoreError::SerdeError).expect("serde failed repeatedly")
                    }
                };
                Bytes::from(serialized_result)
            }
            Operation::Delete { key } => {
                let result = self.delete(&key);
                let serialized_result = match rmp_serde::to_vec(&result) {
                    Ok(r) => r,
                    Err(_) => {
                        rmp_serde::to_vec(&StoreError::SerdeError).expect("serde failed repeatedly")
                    }
                };
                Bytes::from(serialized_result)
            }
        }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}
