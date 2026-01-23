# Plan: Implement `recceiver` (RecSync Server) in Rust

## Overview
The goal is to implement the RecSync Server (`recceiver`) in Rust, mirroring the functionality of the existing Python/Twisted implementation (`recsync/server`). This server receives EPICS channel information from IOCs via the `reccaster` protocol (TCP) and syncs it to ChannelFinder. It also broadcasts its presence via UDP.

## Architecture
The new crate `recsync-rs/recceiver` will be a binary crate within the `recsync-rs` workspace.

### Modules
1.  **`main`**: Entry point, configuration loading, and runtime setup.
2.  **`announcer`**: Handles UDP broadcasting to announce server presence to IOCs.
3.  **`server`**: Handles TCP connections from IOCs (`reccaster` clients). Implements the state machine (Greeting -> Ping/Pong -> Data -> Done).
4.  **`store`**: In-memory aggregation of IOC data (records, infos).
5.  **`channelfinder`**: Client for interacting with the ChannelFinder REST API.
6.  **`config`**: Configuration parsing (INI format, compatible with existing `demo.conf`).

## Dependencies
- `tokio`: Async runtime (TCP/UDP).
- `tokio-util`: Codec support.
- `wire`: Existing internal crate for protocol definitions (needs updates).
- `reqwest`: HTTP client for ChannelFinder.
- `serde`, `serde_json`: JSON handling.
- `config` or `rust-ini`: Configuration parsing.
- `log` / `env_logger`: Logging.

## Proposed Changes & Commit Order

1.  **`feat(wire): implement server-side codec`**
    - Update `recsync-rs/wire/src/codec.rs` to implement `Encoder` for Server messages (`ServerGreet`, `Ping`) and `Decoder` for Client messages (`ClientGreet`, `Pong`, `AddRecord`, `DelRecord`, `UploadDone`, `AddInfo`).
    - Fix any missing message types or struct alignments.
    - **Test**: Add round-trip unit tests in `wire` to verify binary compatibility with the protocol spec.

2.  **`feat(recceiver): initial crate setup`**
    - Create `recsync-rs/recceiver/Cargo.toml`.
    - Create basic `main.rs`.
    - Update workspace in `recsync-rs/Cargo.toml`.

3.  **`feat(recceiver): implement UDP announcer`**
    - Implement `Announcer` struct/task in `announcer.rs`.
    - Broadcast the specific struct (`>HH4sHHI`) periodically.
    - **Test**: Add a unit test that listens on the UDP port and verifies the announced packet structure.

4.  **`feat(recceiver): implement TCP server and session`**
    - Implement `server.rs` to listen on TCP.
    - Implement `session.rs` to handle the protocol state machine using `wire::MessageCodec`.
    - Handle `ClientGreet`, `Ping/Pong`, and accumulate data messages.
    - **Test**: Unit tests for the state machine logic (mocking the stream).

5.  **`feat(recceiver): implement ChannelFinder client`**
    - Implement `channelfinder.rs` using `reqwest`.
    - Implement `find`, `update`, `set` methods.
    - Implement `store.rs` to manage the logic of syncing received data to ChannelFinder (handling active/inactive states).
    - **Test**: Mock ChannelFinder API responses to test `store` logic without a real server.

6.  **`feat(recceiver): add configuration and wiring`**
    - Implement `config.rs` to parse `demo.conf`.
    - Wire everything together in `main.rs`.

7.  **`test(integration): rust client <-> rust server`**
    - Create an integration test suite in `recsync-rs/tests`.
    - Spin up `recceiver` and `reccaster` (from the existing crate) in-process or as subprocesses.
    - Verify that `reccaster` can successfully register records with `recceiver`.

8.  **`test(interop): legacy python interop (Docker)`**
    - Create a `docker-compose.test.yml` that includes:
        - `channelfinder` (Service + ES/Postgres)
        - `recceiver-rs` (The new Rust server)
        - `reccaster-py` (The legacy Python client)
        - `recceiver-py` (The legacy Python server - for validating Rust client)
    - **Scenario A**: Python Client -> Rust Server. Verify data appears in ChannelFinder.
    - **Scenario B**: Rust Client -> Python Server. Verify data appears in ChannelFinder.
    - Write a test script (Rust or Shell) to orchestrate these scenarios and query ChannelFinder to assert success.