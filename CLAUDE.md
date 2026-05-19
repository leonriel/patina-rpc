# Patina RPC - AI Assistant Guidelines

## Project Context
You are helping build **Patina**, a high-performance, asynchronous Remote Procedure Call (RPC) protocol written in Rust. Patina acts as the foundational network layer for a larger distributed infrastructure stack (including CopperDB, an LSM storage engine). Performance, data density, and memory safety are the top priorities.

## Core Technology Stack
* **Async Runtime:** `tokio` (multi-threaded feature).
* **Network Transport:** `tokio::net::TcpStream`.
* **Framing:** `tokio-util::codec::LengthDelimitedCodec` (Strictly 4-byte/u32 length prefixes).
* **Serialization:** `serde` + `bincode` (Little-endian, dense binary representation. **Do not use JSON**).

## Architectural Rules
1. **The Wire Layer:** All messages must be wrapped in an `Envelope` enum (`Request`, `Response`, `Error`, `Heartbeat`). The payload inside the envelope is always a raw `Vec<u8>`.
2. **The Multiplexer:** The client and server must decouple the Read path from the Write path using background Tokio tasks. 
3. **Client State:** The client tracks pending requests using an `Arc<Mutex<HashMap<u64, tokio::sync::oneshot::Sender<Envelope>>>>`.
4. **ID Generation:** Request IDs must be generated using an `AtomicU64` starting at 0 with `Ordering::Relaxed`. Do not use UUIDs.
5. **Channel Usage:** Use `tokio::sync::mpsc` for pushing outbound messages to the background writer task.

## Coding Standards
* **Zero Unsafe:** Do not write `unsafe` Rust blocks unless explicitly instructed by the user. Rely on safe Rust abstractions.
* **No Panics / No Unwrap:** Never use `.unwrap()`, `.expect()`, or `panic!` in production library code. Always propagate errors using `Result` and the `?` operator. The use of `.unwrap()` is strictly forbidden everywhere *except* inside test modules (`#[cfg(test)]`) and integration tests.
* **Test-Driven Development (TDD):** When asked to implement a new feature, write failing unit/integration tests first. Use `tokio::test` for async tests.
* **Error Handling:** Use `thiserror` for defining custom error types. 
* **Logging:** Use the `tracing` crate for instrumentation (`info!`, `debug!`, `error!`), not `println!`.
* **Naming — `Envelope` bindings:** Do not use `env` as a variable name for `Envelope` values; it reads as "environment". Default to the full word `envelope`. For tight scopes where a one-letter binding is idiomatic (closures, single-line match arms), `e` is acceptable.