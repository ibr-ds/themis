#![feature(vec_into_raw_parts)]
use std::{cmp::min, convert::TryFrom, ffi::CStr, os::raw::c_char, sync::Arc};

use bytes::Bytes;
use themis_client::{Client, Destination};
use themis_core::{
    app::{Request, RequestFlags, Response},
    config::{load_from_paths, Config, DEFAULT_CONFIG_PATH},
    net::Message,
    Result,
};

use futures::future;
use tokio::runtime::Runtime;

use kvs_core::messages::{Operation, Return};

pub struct KVSClient {
    bft_client: Client,
    id: i32,
    seq: u64,
}

impl KVSClient {
    pub fn new(bft_client: Client, id: i32) -> Self {
        Self {
            bft_client,
            id,
            seq: 0,
        }
    }
}

#[no_mangle]
pub extern "C" fn init(id: i32) -> *mut KVSClient {
    //config path is set so that starting ycsb from kvs-client/YCSB-C/ works
    let config_path: Vec<String> = vec![DEFAULT_CONFIG_PATH.into()];
    let mut config: Config = load_from_paths(&config_path).expect("cannot load config");
    config.set("id", id).expect("set id");
    let config = Arc::new(config);

    let rt = Runtime::new().unwrap();
    let handle = rt.handle();

    let f = async move {
        Client::connect(config.clone())
            .await
            .expect("could not create client")
    };

    let bftclient = handle.block_on(f);
    println!("created bftclient {}", id); // this isn't printed somehow
                                          //still the creation works and we can return

    //create a thread to run Client::connect in background
    let _thread = std::thread::spawn(move || {
        // block_on pending future to keep runtime alive and prevent shutdown of spawned background threads
        let () = rt.block_on(future::pending());
    });

    let kvs_client = KVSClient::new(bftclient, id);

    let kvs_box = Box::new(kvs_client);
    Box::into_raw(kvs_box) as *mut KVSClient
}

/// # Safety
/// valid pointer
#[no_mangle]
pub unsafe extern "C" fn destroy(kvs_client: *mut KVSClient) {
    Box::from_raw(kvs_client);
    // bft_client goes out of scope
}
/// # Safety
/// `client`is a valid pointer
/// key is a 0-terminated string
/// data is an array of length
#[no_mangle]
pub unsafe extern "C" fn kvs_insert(
    client: *mut KVSClient,
    key: *const c_char,
    data: *mut u8,
    length: u32,
) -> i32 {
    let kvs_client = &mut *client;

    let key_string = match get_key_str(key) {
        Some(k) => k,
        None => return 1,
    };

    let value_slice = match get_data_slice(data, length) {
        Some(d) => d,
        None => return 1,
    };

    let value = value_slice.to_vec();
    println!(
        "Client {} SeqNo {} Insert {} {:?}",
        kvs_client.id, kvs_client.seq, key_string, value
    );

    let operation = Operation::Set {
        key: key_string,
        value: Bytes::from(value),
    };

    let themis_result = send(kvs_client, operation);

    match themis_result {
        Err(_) => -1, // maybe further differentiate errors
        Ok(message) => {
            let response: Response = message.inner;
            let payload: Bytes = response.payload;
            let kvs_result: Return = match rmp_serde::from_slice(&payload) {
                Ok(r) => r,
                Err(_) => return -2,
            };
            match kvs_result {
                Err(_) => -3, // maybe further differentiate errors
                Ok(_) => 0,
            }
        }
    }
}

/// # Safety
/// `client`is a valid pointer
/// key is a 0-terminated string
/// buffer is an array of length
#[no_mangle]
pub unsafe extern "C" fn kvs_get(
    client: *mut KVSClient,
    key: *const c_char,
    buffer: *mut u8,
    length: u32,
) -> i32 {
    let kvs_client = &mut *client;

    let key_string = match get_key_str(key) {
        Some(k) => k,
        None => return -1,
    };

    let buffer_slice = match get_data_slice(buffer, length) {
        Some(d) => d,
        None => return -1,
    };

    println!(
        "Client {} SeqNo {} Get {}",
        kvs_client.id, kvs_client.seq, key_string
    );

    let operation = Operation::Get { key: key_string };

    let themis_result = send(kvs_client, operation);

    let data = match themis_result {
        Err(_) => return -1,
        Ok(message) => {
            let response: Response = message.inner;
            let payload: Bytes = response.payload;
            let kvs_result: Return = match rmp_serde::from_slice(&payload) {
                Ok(r) => r,
                Err(_) => return -2,
            };
            match kvs_result {
                Err(_) => return -3,
                Ok(s) => match s {
                    None => return 0,
                    Some(v) => v,
                },
            }
        }
    };

    let min_length: usize = min(length as usize, data.len());

    buffer_slice[..min_length].copy_from_slice(&data[..min_length]);
    min_length as i32
}

/// # Safety
/// `client`is a valid pointer
/// key is a 0-terminated string
#[no_mangle]
pub unsafe extern "C" fn kvs_delete(client: *mut KVSClient, key: *const c_char) -> i32 {
    let kvs_client = &mut *client;

    let key_string = match get_key_str(key) {
        Some(k) => k,
        None => return -1,
    };

    println!(
        "Client {} SeqNo {} Delete {}",
        kvs_client.id, kvs_client.seq, key_string
    );

    let operation = Operation::Delete { key: key_string };

    let themis_result = send(kvs_client, operation);

    // unpack the result
    match themis_result {
        Err(_) => -1,
        Ok(message) => {
            let response: Response = message.inner;
            let payload: Bytes = response.payload;
            let kvs_result: Return = match rmp_serde::from_slice(&payload) {
                Ok(r) => r,
                Err(_) => return -2,
            };
            match kvs_result {
                Err(_) => -3,
                Ok(_) => 0,
            }
        }
    }
}

// generates a rust String from the C char array
unsafe fn get_key_str(key: *const c_char) -> Option<String> {
    if key.is_null() {
        return None;
    }
    let key_str;
    let raw = CStr::from_ptr(key);
    key_str = match raw.to_str() {
        Ok(k) => k,
        Err(_) => return None,
    };
    Some(String::from(key_str))
}

unsafe fn get_data_slice<'d>(data: *mut u8, length: u32) -> Option<&'d mut [u8]> {
    if data.is_null() {
        return None;
    }
    let length_usize = match usize::try_from(length) {
        Ok(v) => v,
        Err(_) => return None,
    };
    let value_slice: &mut [u8];

    value_slice = std::slice::from_raw_parts_mut(data, length_usize);

    Some(value_slice)
}

unsafe fn send(kvs_client: &mut KVSClient, operation: Operation) -> Result<Message<Response>> {
    let payload = Bytes::from(rmp_serde::to_vec(&operation).expect("Serde Error"));

    let request = Request {
        sequence: kvs_client.seq,
        flags: RequestFlags::empty(),
        payload,
    };
    kvs_client.seq += 1;

    kvs_client
        .bft_client
        .request_blocking(request, Destination::Primary)
}
