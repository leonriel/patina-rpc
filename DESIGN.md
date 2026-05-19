# **Patina RPC: Wire Layer Design Document**

## **1\. Overview**

This document outlines the architecture and implementation details for the Wire Layer of **Patina**, a high-performance, asynchronous Remote Procedure Call (RPC) protocol written in Rust. The Wire Layer is responsible for translating Rust data structures into bytes, structuring those bytes into discrete network frames, and safely transmitting them over a TCP stream. Patina is designed to act as the central nervous system for the CopperDB infrastructure stack.

## **2\. Serialization Strategy**

Given that Patina is initially designed to power a Rust-native infrastructure stack, we prioritize zero-cost abstractions, speed, and maximum data density over human readability.

* **Format:** Bincode (via the bincode crate).  
* **Why Bincode?** It is heavily optimized for Rust's serde framework. Unlike JSON or MessagePack, Bincode strips away all field names and metadata, relying entirely on the pre-compiled struct definition for deserialization. This results in the smallest possible payload size on the wire.  
* **Endianness:** Little-endian (the default for Bincode and optimal for modern CPU architectures).

## **3\. Protocol Envelope Design**

Every message transmitted over the network is wrapped in a standard Envelope. This allows the multiplexer to inspect the message metadata before attempting to deserialize the core payload.

### **3.1. Envelope Structure**

The top-level message is defined as a Rust enum to guarantee strict typing on the wire.

Rust  
\#\[derive(Serialize, Deserialize, Debug, Clone)\]  
pub enum Envelope {  
    Request(RequestData),  
    Response(ResponseData),  
    Error(ErrorData),  
    Heartbeat,  
}

### **3.2. Core Data Types**

The internal structs carry the multiplexing state and the raw serialized arguments.

| Type | Fields | Purpose |
| :---- | :---- | :---- |
| **RequestData** | id: u64  method: String  payload: Vec\<u8\> | Initiates an RPC call. The id uniquely identifies the request for multiplexing. The method maps to the server-side function, and the payload contains the pre-serialized arguments. |
| **ResponseData** | id: u64  payload: Vec\<u8\> | Returns the successful result of an RPC call. The id must match the originating Request. |
| **ErrorData** | id: u64  code: u16  message: String | Signals a failure. Includes standard HTTP-like status codes (e.g., 404 Not Found, 500 Internal Server Error) and a human-readable message. |

**Note on payload:** The payload field is a raw byte vector. The wire layer does not care what is inside it. The higher-level generated RPC stubs handle serializing the specific function arguments into this Vec\<u8\> before it gets placed into the Envelope.

## **4\. Network Framing**

TCP is a streaming protocol; it has no concept of message boundaries. To prevent the server from reading partial envelopes or conflating multiple envelopes together, Patina implements **Length-Prefixed Framing**.

* **Prefix Size:** 4 bytes (u32). This allows a maximum payload size of \~4.29 GB per message, which is more than sufficient for standard microservice workloads and chunked storage transfers.  
* **Implementation:** We will leverage Tokio's tokio\_util::codec::LengthDelimitedCodec. This handles the asynchronous chunking, buffering, and frame delimiting automatically, drastically reducing the risk of buffer overflow or edge-case network bugs.

### **4.1. On-the-Wire Byte Layout**

When a Patina message is transmitted, the exact byte layout on the TCP stream looks like this:

Plaintext  
`+-------------------+---------------------------------------------------+`  
`| Length Prefix     | Serialized Bincode Envelope                       |`  
`| (4 bytes, u32 BE) | (N bytes, variable length)                        |`  
`+-------------------+---------------------------------------------------+`  
`| 0x00 0x00 0x00 0x2A| Enum Tag (1 byte) | ID (8 bytes) | Method Len...   |`  
`+-------------------+---------------------------------------------------+`

*The Length Prefix strictly denotes the size of the Serialized Bincode Envelope. It does not include its own 4 bytes.*

## **5\. Edge Cases & Resilience**

To ensure the wire layer is robust in a distributed environment, the following constraints are enforced at the codec level:

1. **Maximum Frame Length Constraints:** To prevent Denial of Service (DoS) attacks or out-of-memory panics via memory exhaustion, the LengthDelimitedCodec must be configured with a strict max\_frame\_length (e.g., 64 MB). If a client attempts to send a frame larger than this, the connection will be immediately terminated.  
2. **Heartbeat Pings:** The Envelope::Heartbeat variant carries no payload and is extremely lightweight. It is periodically sent over idle TCP connections to ensure the underlying socket has not been silently dropped by intermediate NATs, load balancers, or firewalls.  
3. **Connection Teardown:** If the framing layer encounters a corrupted length prefix or a Bincode deserialization error, the TCP stream is considered tainted. The protocol will drop the connection rather than attempt to recover the byte stream, relying on the client to re-establish a fresh connection.

