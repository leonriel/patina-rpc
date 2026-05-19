# **Patina RPC — Design Document**

Patina is a high-performance, asynchronous Remote Procedure Call (RPC) protocol written in Rust. It is designed to act as the central nervous system for the CopperDB infrastructure stack. This document is cumulative: each phase builds on the contract established by earlier phases.

---

## **Phase 1: Wire Layer**

### **1\. Overview**

This phase outlines the architecture and implementation details for the Wire Layer of **Patina**. The Wire Layer is responsible for translating Rust data structures into bytes, structuring those bytes into discrete network frames, and safely transmitting them over a TCP stream.

### **2\. Serialization Strategy**

Given that Patina is initially designed to power a Rust-native infrastructure stack, we prioritize zero-cost abstractions, speed, and maximum data density over human readability.

* **Format:** Bincode (via the bincode crate).
* **Why Bincode?** It is heavily optimized for Rust's serde framework. Unlike JSON or MessagePack, Bincode strips away all field names and metadata, relying entirely on the pre-compiled struct definition for deserialization. This results in the smallest possible payload size on the wire.
* **Endianness:** Little-endian (the default for Bincode and optimal for modern CPU architectures).

### **3\. Protocol Envelope Design**

Every message transmitted over the network is wrapped in a standard Envelope. This allows the multiplexer to inspect the message metadata before attempting to deserialize the core payload.

#### **3.1. Envelope Structure**

The top-level message is defined as a Rust enum to guarantee strict typing on the wire.

Rust
\#\[derive(Serialize, Deserialize, Debug, Clone)\]
pub enum Envelope {
    Request(RequestData),
    Response(ResponseData),
    Error(ErrorData),
    Heartbeat,
}

#### **3.2. Core Data Types**

The internal structs carry the multiplexing state and the raw serialized arguments.

| Type | Fields | Purpose |
| :---- | :---- | :---- |
| **RequestData** | id: u64  method: String  payload: Vec\<u8\> | Initiates an RPC call. The id uniquely identifies the request for multiplexing. The method maps to the server-side function, and the payload contains the pre-serialized arguments. |
| **ResponseData** | id: u64  payload: Vec\<u8\> | Returns the successful result of an RPC call. The id must match the originating Request. |
| **ErrorData** | id: u64  code: u16  message: String | Signals a failure. Includes standard HTTP-like status codes (e.g., 404 Not Found, 500 Internal Server Error) and a human-readable message. |

**Note on payload:** The payload field is a raw byte vector. The wire layer does not care what is inside it. The higher-level generated RPC stubs handle serializing the specific function arguments into this Vec\<u8\> before it gets placed into the Envelope.

### **4\. Network Framing**

TCP is a streaming protocol; it has no concept of message boundaries. To prevent the server from reading partial envelopes or conflating multiple envelopes together, Patina implements **Length-Prefixed Framing**.

* **Prefix Size:** 4 bytes (u32). This allows a maximum payload size of \~4.29 GB per message, which is more than sufficient for standard microservice workloads and chunked storage transfers.
* **Implementation:** We will leverage Tokio's tokio\_util::codec::LengthDelimitedCodec. This handles the asynchronous chunking, buffering, and frame delimiting automatically, drastically reducing the risk of buffer overflow or edge-case network bugs.

#### **4.1. On-the-Wire Byte Layout**

When a Patina message is transmitted, the exact byte layout on the TCP stream looks like this:

Plaintext
`+-------------------+---------------------------------------------------+`
`| Length Prefix     | Serialized Bincode Envelope                       |`
`| (4 bytes, u32 BE) | (N bytes, variable length)                        |`
`+-------------------+---------------------------------------------------+`
`| 0x00 0x00 0x00 0x2A| Enum Tag (1 byte) | ID (8 bytes) | Method Len...   |`
`+-------------------+---------------------------------------------------+`

*The Length Prefix strictly denotes the size of the Serialized Bincode Envelope. It does not include its own 4 bytes.*

### **5\. Edge Cases & Resilience**

To ensure the wire layer is robust in a distributed environment, the following constraints are enforced at the codec level:

1. **Maximum Frame Length Constraints:** To prevent Denial of Service (DoS) attacks or out-of-memory panics via memory exhaustion, the LengthDelimitedCodec must be configured with a strict max\_frame\_length (e.g., 64 MB). If a client attempts to send a frame larger than this, the connection will be immediately terminated.
2. **Heartbeat Pings:** The Envelope::Heartbeat variant carries no payload and is extremely lightweight. It is periodically sent over idle TCP connections to ensure the underlying socket has not been silently dropped by intermediate NATs, load balancers, or firewalls.
3. **Connection Teardown:** If the framing layer encounters a corrupted length prefix or a Bincode deserialization error, the TCP stream is considered tainted. The protocol will drop the connection rather than attempt to recover the byte stream, relying on the client to re-establish a fresh connection.

---

## **Phase 2: Connection Multiplexer**

### **1\. The Core Challenge: Why We Need a Multiplexer**

In a naive synchronous client, Thread A locks the TCP stream, writes a request, and waits for the server's response. If the server takes 5 seconds to process Thread A's request, Thread B is completely blocked from sending its request.

To fix this, we decouple the **Write Path** from the **Read Path**.

### **2\. Client-Side Multiplexer Architecture**

The client is responsible for keeping track of which responses belong to which callers. We achieve this using a background read loop and a shared state map.

#### **The Shared State**

You will need a thread-safe map that stores a one-time-use channel for every request currently in flight.

Rust
type PendingMap \= Arc\<Mutex\<HashMap\<u64, tokio::sync::oneshot::Sender\<Envelope\>\>\>\>;

#### **The Primitives & Tasks**

When a client connects to a server, Patina splits the Framed TCP stream (from tokio\_util) into a SplitSink (for writing) and a SplitStream (for reading).

1. **The Writer Channel (mpsc::Sender):** You cannot have multiple threads writing to the TCP socket at the same time. Instead, the client struct holds a tokio::sync::mpsc::Sender. When a user makes an RPC call, the request envelope is sent into this channel.
2. **The Background Writer Task:**
   A dedicated tokio::spawn loop constantly drains the mpsc channel and writes the envelopes to the SplitSink (the actual TCP stream).
3. **The Background Reader Task:**
   A dedicated tokio::spawn loop constantly awaits new frames from the SplitStream.

#### **Life of a Client Request**

1. **Initiation:** The caller invokes client.call("GetUser", payload).
2. **Registration:** The client generates a unique u64 ID via std::sync::atomic::AtomicU64. It creates a oneshot::channel. It stores the Sender half in the PendingMap under that ID, and holds onto the Receiverhalf.
3. **Dispatch:** The client sends the RequestData envelope into the mpsc channel.
4. **Waiting:** The caller .awaits the oneshot::Receiver.
5. **Resolution:** The Server replies. The Background Reader Task reads the frame, extracts the ID, locks the PendingMap, removes the oneshot::Sender, and pushes the response through it. The waiting caller is instantly woken up with their data.

### **3\. Server-Side Multiplexer Architecture**

The server is much simpler because it doesn't need to match IDs to waiting channels; it just needs to read requests, do the work concurrently, and write the answers back with the same ID.

#### **The Primitives & Tasks**

1. **The Write Channel (mpsc::Sender):**
   Just like the client, the server needs a dedicated task to handle writing to the socket to prevent interleaved bytes.
2. **The Dispatcher Loop:**
   The server's main task loops over the SplitStream, reading incoming envelopes.
3. **The Worker Spawn:**
   For *every single* incoming request, the server calls tokio::spawn(handle\_request(...)).

#### **Life of a Server Response**

1. **Ingestion:** The Dispatcher Loop reads a RequestData envelope (ID: 42).
2. **Delegation:** It clones the mpsc::Sender and passes it, along with the envelope, into a newly spawned Tokio task. The Dispatcher immediately goes back to listening for the next frame.
3. **Execution:** The spawned task routes the request to the user's business logic (e.g., querying CopperDB).
4. **Reply:** Once the business logic yields a result, the task wraps it in a ResponseData envelope (with ID: 42\) and pushes it into the mpsc channel.
5. **Egress:** The server's Background Writer Task pulls the envelope from the mpsc channel and flushes it down the TCP socket.

### **4\. Edge Cases & Cleanup**

If you don't aggressively clean up state, your multiplexer will leak memory.

* **Client Timeouts:** If a user wraps their RPC call in tokio::time::timeout and it expires, the caller drops their oneshot::Receiver. However, the ID and the oneshot::Sender are still sitting in the PendingMap\! You must implement a mechanism (often a Drop guard) to remove the ID from the map if the caller gives up.
* **Connection Drops:** If the TCP connection unexpectedly closes, the Background Reader Task will exit. Before it shuts down, it must iterate through the entire PendingMap and send an Error to every single waiting oneshot channel. Otherwise, all pending caller threads will hang forever waiting for a response that will never arrive.
