# BFT Framework written in Rust + PBFT implementation

## Structure

### ./themis-core
The core modules for running the BFT framework
The most important components are
- execute:  a single threaded executor and threadpool bindings.
- net: TCP connection handling, handshakes and message encoding/decoding
- crypto: Traits and Implementations for message signing and verification
- app and protocol: Traits for implementing BFT protocols and applications
- modules: Default setup code to spawn the different modules.

### ./themis-client
Client library for interacting with replicas,
Provides a synchronous and asynchronous interface

### ./themis-tools
  - control starts 4 replicas locally
  - keygen genrates private/public keys used for consensus. Only ECDSA and Ed25519 keys can be generated. Rsa keys have to be generated with e.g. openssl
  - msgdump is an analysis tool for client requests in binary form.

### ./themis-pbft
Contains the implementation of the PBFT protocol

### ./themis-applications/themis-bench-app
An example application that implements the Application trait.

This crate is the binary that starts a replica

### ./themis-clients
Different client implementations used for benchmarking or debugging/testing.


## Building the Project
The BFT Framework is build with nightly Rust, a version is pinned in the `rust-toolchain` file. Tested on Linux x86_64 and armv7, but it should in theory run on every Rust target with std support.

- Download Rustup and install Cargo. Cargo should use rustup to automatically install the correct nightly toolchain.
- Run ```cargo build --bins``` to build all binaries in Debug mode
- Run ```cargo build --release --bins``` to build all binaries in Release mode

All binaries support the `--help` option to see available commands.

## Running the example Application
Generate 4 key pairs:
- `cargo run --bin keygen -- Ed25519 0 4 --out-dir keys`

Run four replicas with REPLICA_IDs 0 to 3. Logging can be set with the RUST_LOG environment variable. For example,`export RUST_LOG="debug"`.
- with cargo: `cargo run --release --bin themis-bench-app REPLICA_ID --config 'config/default/'`
- or without cargo: `./target/release/themis-bench-app REPLICA_ID --config 'config/default/'`

CONFIG_FILE is an optional parameter and defaults to config/default/.

Use the cli client to send some requests:
- `cargo run --bin cli-client `

You can use a program called `tmuxp` to load a convenient tmux session with four replica and the bench client on localhost.

`tmuxp load tmux.yaml`

This sets up a tmux session with the five panes. `tmux.yaml` can be changed to fit your needs when testing changes.

## Deploy on remote Servers
There is an example ansible configuration in `/ansible`. You can call it from the project root:

`ansible-playbook -i ansible/inventory/beagles.ini ansible/deploy.yaml`

This uploads `/keys`, `/config` and the bench client and app the the servers defined in the ini file.
If you require different servers, create another inventory file similar to the example.
You can also change the `base_dir` variable in the inventory.
For example, you can change it to something that includes your username, if you do not have permission for the default directory.

## Prometheus metrics
In the feature *metrics* the themis-pbft crate collects metrics from the protocol using prometheus.\
To activate this feature run ```cargo build --bins --features metrics```.\
The metrics include the time until a request is processed, the time from a view change request to the change and the
last committed sequence number, the next sequence number, low mark, high mark, and view number.
They are defined in the module [themis_pbft::metrics](./themis-pbft/src/metrics.rs).\
The module [themis_core::metrics_server](./themis-core/src/metrics_server.rs) publishes these metrics using a simple warp server.\
The server must be started from the application.
See [themis-bench-app](./themis-apps/themis-bench-app/src/main.rs) for an example.

## Themis Key-Value-Store
The Key-Value-Store consists of 3 crates:
- [kvs-core](./themis-apps/kvs/kvs-core) a very simple Key-Value-Store with very few dependencies.\
  It does not implement the themis-application trait to be usable with SGX enclaves.
- [kvs-app](./themis-apps/kvs/kvs-app) uses kvs-core and the implements the application trait to test the Key-Value-Store locally without SGX.
- [kvs-client](./themis-clients/kvs-client) implements the client to send requests to the Key-Value-Store via themis.\
  It is compiled to a static library, that C or C++ applications can be linked against.\
  The [themis-kvs-YCSB](https://gitlab.ibr.cs.tu-bs.de/drescher/themis-kvs-YCSB) repository contains a modified implementation of YCSB that links against this static library.\
  It can be used to benchmark the themis Key-Value-Store using YCSB.

# Testing
This repository has different unit / integration and system tests. These can be performed with the standard `cargo test`.

In addition, a subset of the tests can be run with [Miri](https://github.com/rust-lang/miri) with `cargo miri test`.
Miri can detect among other things use-after-free, out-of-bounds memory accesses, invalid use of uninitialized data or not sufficiently aligned memory accesses and references.
