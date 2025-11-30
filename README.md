# Sharded Key Value Store with Stateless Router - Architecture and Design Document

## Table of Contents
- [Sharded Key Value Store with Stateless Router - Architecture and Design Document](#sharded-key-value-store-with-stateless-router---architecture-and-design-document)
  - [Table of Contents](#table-of-contents)
  - [0. High Level System Overview](#0-high-level-system-overview)
  - [1. Routing Logic](#1-routing-logic)
    - [1.1 Partitioning Strategy](#11-partitioning-strategy)
    - [1.2 Stateless Router Design](#12-stateless-router-design)
  - [2. Wire Protocol](#2-wire-protocol)
    - [2.1 Message Framing](#21-message-framing)
    - [2.2 Request Message Format](#22-request-message-format)
      - [Field Rules](#field-rules)
    - [Request Encoding (Pseudocode)](#request-encoding-pseudocode)
    - [2.3 Response Message Format](#23-response-message-format)
      - [Response Opcodes](#response-opcodes)
      - [Response Decoding (Pseudocode)](#response-decoding-pseudocode)
  - [3. Key–Value Storage Engine](#3-keyvalue-storage-engine)
    - [3.1 Overview](#31-overview)
    - [3.2 Data Model](#32-data-model)
      - [3.2.1 Keys](#321-keys)
      - [3.2.2 Values](#322-values)
    - [3.3 Storage Substrate](#33-storage-substrate)
      - [3.3.1 Node Layout](#331-node-layout)
      - [3.3.2 Node Memory Allocation](#332-node-memory-allocation)
    - [3.4 Hash Table Behavior](#34-hash-table-behavior)
      - [3.4.1 Load Factor and Resize Threshold](#341-load-factor-and-resize-threshold)
      - [3.4.2 Resize Operation](#342-resize-operation)
      - [3.4.3 Hash Function](#343-hash-function)
    - [3.5 Execution Model](#35-execution-model)
      - [3.5.1 Single-Threaded Guarantee](#351-single-threaded-guarantee)
      - [3.5.2 Per-Shard Event Loop](#352-per-shard-event-loop)
      - [3.5.3 Pseudocode: Shard Event Loop](#353-pseudocode-shard-event-loop)
    - [3.6 Operations](#36-operations)
      - [3.6.1 PUT(key, value)](#361-putkey-value)
      - [3.6.2 GET(key)](#362-getkey)
      - [3.6.3 DELETE(key)](#363-deletekey)
      - [3.6.4 EXISTS(key)](#364-existskey)
    - [3.7 Error Semantics](#37-error-semantics)
    - [3.8 Memory Ownership Model](#38-memory-ownership-model)
    - [3.9 Invariants](#39-invariants)
  - [4. Networking Flow, connection handling, timeouts](#4-networking-flow-connection-handling-timeouts)
    - [4.1 Networking Flow \& Connection Handling](#41-networking-flow--connection-handling)
    - [4.2 Timeouts](#42-timeouts)
  - [5. Handling Shard and Router Failures](#5-handling-shard-and-router-failures)
    - [5.1 Heartbeat Endpoints](#51-heartbeat-endpoints)
    - [5.2 Shard Failures](#52-shard-failures)
    - [5.3 Router Failures](#53-router-failures)
    - [5.4 Per-Core Router Loop Failures](#54-per-core-router-loop-failures)
- [Sharded Key Value Store with Stateless Router - Architecture and Design Document](#sharded-key-value-store-with-stateless-router---architecture-and-design-document-1)
  - [Table of Contents](#table-of-contents-1)
  - [0. High Level System Overview](#0-high-level-system-overview-1)
  - [1. Routing Logic](#1-routing-logic-1)
    - [1.1 Partitioning Strategy](#11-partitioning-strategy-1)
    - [1.2 Stateless Router Design](#12-stateless-router-design-1)
  - [2. Wire Protocol](#2-wire-protocol-1)
    - [2.1 Message Framing](#21-message-framing-1)
    - [2.2 Request Message Format](#22-request-message-format-1)
      - [Field Rules](#field-rules-1)
    - [Request Encoding (Pseudocode)](#request-encoding-pseudocode-1)
    - [2.3 Response Message Format](#23-response-message-format-1)
      - [Response Opcodes](#response-opcodes-1)
      - [Response Decoding (Pseudocode)](#response-decoding-pseudocode-1)
  - [3. Key–Value Storage Engine](#3-keyvalue-storage-engine-1)
    - [3.1 Overview](#31-overview-1)
    - [3.2 Data Model](#32-data-model-1)
      - [3.2.1 Keys](#321-keys-1)
      - [3.2.2 Values](#322-values-1)
    - [3.3 Storage Substrate](#33-storage-substrate-1)
      - [3.3.1 Node Layout](#331-node-layout-1)
      - [3.3.2 Node Memory Allocation](#332-node-memory-allocation-1)
    - [3.4 Hash Table Behavior](#34-hash-table-behavior-1)
      - [3.4.1 Load Factor and Resize Threshold](#341-load-factor-and-resize-threshold-1)
      - [3.4.2 Resize Operation](#342-resize-operation-1)
      - [3.4.3 Hash Function](#343-hash-function-1)
    - [3.5 Execution Model](#35-execution-model-1)
      - [3.5.1 Single-Threaded Guarantee](#351-single-threaded-guarantee-1)
      - [3.5.2 Per-Shard Event Loop](#352-per-shard-event-loop-1)
      - [3.5.3 Pseudocode: Shard Event Loop](#353-pseudocode-shard-event-loop-1)
    - [3.6 Operations](#36-operations-1)
      - [3.6.1 PUT(key, value)](#361-putkey-value-1)
      - [3.6.2 GET(key)](#362-getkey-1)
      - [3.6.3 DELETE(key)](#363-deletekey-1)
      - [3.6.4 EXISTS(key)](#364-existskey-1)
    - [3.7 Error Semantics](#37-error-semantics-1)
    - [3.8 Memory Ownership Model](#38-memory-ownership-model-1)
    - [3.9 Invariants](#39-invariants-1)
  - [4. Networking Flow, connection handling, timeouts](#4-networking-flow-connection-handling-timeouts-1)
    - [4.1 Networking Flow \& Connection Handling](#41-networking-flow--connection-handling-1)
    - [4.2 Timeouts](#42-timeouts-1)
  - [5. Handling Shard and Router Failures](#5-handling-shard-and-router-failures-1)
    - [5.1 Heartbeat Endpoints](#51-heartbeat-endpoints-1)
    - [5.2 Shard Failures](#52-shard-failures-1)
    - [5.3 Router Failures](#53-router-failures-1)
    - [5.4 Per-Core Router Loop Failures](#54-per-core-router-loop-failures-1)
    - [5.5 Client Connection Failures](#55-client-connection-failures)

## 0. High Level System Overview

                                                        +-----------------------------+
                                                        |        Client Devices       |
                                                        +---------------+-------------+
                                                                        |
                                                                        |  TCP (KV Wire Protocol)
                                                                        |
                                                                        v
                                                        +----------------------------+
                                                        |        Stateless Router     |
                                                        |  - parse length prefix      |
                                                        |  - extract key bytes        |
                                                        |  - shard_id = hash(key) % 2 |
                                                        |  - forward request          |
                                                        +--------------+--------------+
                                                                       |
                                                +----------------------+--------------------------+
                                                |                                                 |
                                                v                                                 v
                                +----------------------------+                    +----------------------------+
                                |          Shard 0           |                    |          Shard 1           |
                                |  Single-Threaded KV Engine |                    |  Single-Threaded KV Engine |
                                |  - bucket array            |                    |  - bucket array            |
                                |  - chained hash nodes      |                    |  - chained hash nodes      |
                                |  - GET / PUT / DELETE      |                    |  - GET / PUT / DELETE      |
                                +--------------+-------------+                    +--------------+-------------+
                                            |                                                    |
                                            |                  response frames                   |
                                            |                                                    |
                                            +-------------------------+--------------------------+
                                                                      |
                                                                      v
                                                        +-----------------------------+
                                                        |     Router (Response Path)  |
                                                        |  - match request_id         |
                                                        |  - forward response         |
                                                        +--------------+--------------+
                                                                        |
                                                                        |  TCP (KV Wire Protocol)
                                                                        |
                                                                        v
                                                        +-----------------------------+
                                                        |        Client Devices       |
                                                        +-----------------------------+


                                                        +-----------------------------+
                                                        |        Health Checks        |
                                                        |     /health → “OK”          |
                                                        |  (router and shards only)   |
                                                        +-----------------------------+


---
## 1. Routing Logic

### 1.1 Partitioning Strategy

The router employs a simple, deterministic, and stateless partitioning strategy to distribute the key-space across the two required shard nodes (Shard A and Shard B).

The routing decision is made by extracting the key bytes from the request, computing a fast hash, and applying a modulo operation:

```
ShardID = Murmur3(KeyBytes) % 2
```

The choice of Murmur3 ensures a statistically uniform distribution of keys, guaranteeing a near-perfect 50/50 balance of both data and query load across the two shards.

### 1.2 Stateless Router Design

This partitioning strategy is foundational to the stateless nature of the Router.

- Zero State: The Router requires no internal routing tables or caching.

- Deterministic: Any router instance running this logic will always route the same key to the same shard, which is critical for correctness.

- Core Flow: The router's function is reduced to a proxy: it parses the wire protocol only to extract the key, computes the hash, and forwards the entire frame to the destination shard. It then transparently relays the shard's response back to the client.


## 2. Wire Protocol

### 2.1 Message Framing

Every message begins with a 2-byte unsigned length prefix:

```
+------------------------------+------------------------------+
| length (uint16)  | message_body[length bytes]   |
+------------------------------+------------------------------+
```

Receiver logic:

```
L = read_uint16(socket)
body = read_exact(socket, L)
decode(body)
```

This framing layer is the only mechanism that defines message boundaries on a TCP stream.

---

### 2.2 Request Message Format

The request body (bytes after the 2-byte length prefix) has the following fixed layout:

```
                                 Request Message Body
                         (all fields shown in network byte order)

  0                   8                   16                  24                 32
  +-------------------+-------------------+-------------------+------------------+
  |   opcode (8)      |                request_id (64)                           |
  +-------------------+-------------------+-------------------+------------------+
  |            key_len (16)               |              value_len (16)          |
  +----------------------------+-----------------------------+-------------------+
  |                     key bytes (variable, key_len bytes)                      |
  +------------------------------------------------------------------------------+
  |                    value bytes (variable, value_len bytes)                   |
  +------------------------------------------------------------------------------+
  |                               checksum (32)                                  |
  +------------------------------------------------------------------------------+

Field definitions:
  opcode:       1 byte
  request_id:   8 bytes
  key_len:      2 bytes
  value_len:    2 bytes
  key:          key_len bytes
  value:        value_len bytes   (0 bytes for GET)
  checksum:     4 bytes, CRC32 over all fields from opcode through
                the end of the value bytes.

```

#### Field Rules

* `opcode`

  * `0x00` = GET
  * `0x01` = PUT
  * `0x02` = DELETE
  * `0x03` = EXISTS
* `request_id` — 64-bit opaque identifier returned verbatim in response
* `key_len` — uint16
* `value_len` — uint16 (must be `0` for GET)
* `checksum` — CRC32 over all bytes from `opcode` through end of value

### Request Encoding (Pseudocode)

``` c
/*
 * Encode a KV request into a contiguous byte buffer and send it on a TCP socket.
 * All multi-byte fields are written in big-endian.
 */

int encode_and_send_request(int sock,
                            uint8_t  opcode,
                            uint64_t request_id,
                            uint8_t *key,
                            uint16_t key_len,
                            uint8_t *val,
                            uint16_t val_len)
{
    /* ------------------------------------------------------------
     * 1. Compute payload size (everything after the 2-byte length)
     * ------------------------------------------------------------
     *
     * [opcode:1] [request_id:8] [key_len:2] [value_len:2]
     * [key bytes:key_len] [value bytes:val_len] [checksum:4]
     */
    uint16_t payload_size =
        1 + 8 + 2 + 2 + key_len + val_len + 4;

    /* Total frame size includes the 2-byte prefix */
    size_t frame_size = 2 + payload_size;

    uint8_t *buf = malloc(frame_size);
    if (!buf) return -1;

    size_t off = 0;

    /* ------------------------------------------------------------
     * 2. Write length prefix (uint16)
     * ------------------------------------------------------------ */
    buf[off++] = (payload_size >> 8) & 0xFF;
    buf[off++] = (payload_size     ) & 0xFF;

    /* ------------------------------------------------------------
     * 3. Write opcode
     * ------------------------------------------------------------ */
    buf[off++] = opcode;

    /* ------------------------------------------------------------
     * 4. Write request_id (uint64)
     * ------------------------------------------------------------ */
    buf[off++] = (request_id >> 56) & 0xFF;
    buf[off++] = (request_id >> 48) & 0xFF;
    buf[off++] = (request_id >> 40) & 0xFF;
    buf[off++] = (request_id >> 32) & 0xFF;
    buf[off++] = (request_id >> 24) & 0xFF;
    buf[off++] = (request_id >> 16) & 0xFF;
    buf[off++] = (request_id >>  8) & 0xFF;
    buf[off++] = (request_id      ) & 0xFF;

    /* ------------------------------------------------------------
     * 5. key_len (uint16)
     * ------------------------------------------------------------ */
    buf[off++] = (key_len >> 8) & 0xFF;
    buf[off++] = (key_len     ) & 0xFF;

    /* ------------------------------------------------------------
     * 6. value_len (uint16)
     * ------------------------------------------------------------ */
    buf[off++] = (val_len >> 8) & 0xFF;
    buf[off++] = (val_len     ) & 0xFF;

    /* ------------------------------------------------------------
     * 7. Copy key bytes
     * ------------------------------------------------------------ */
    memcpy(&buf[off], key, key_len);
    off += key_len;

    /* ------------------------------------------------------------
     * 8. Copy value bytes
     * ------------------------------------------------------------ */
    memcpy(&buf[off], val, val_len);
    off += val_len;

    /* ------------------------------------------------------------
     * 9. Compute checksum over the *payload only*
     *    (i.e., from buf[2] through buf[2 + payload_size - 4])
     * ------------------------------------------------------------ */
    uint32_t cksum = crc32(&buf[2], payload_size - 4);

    buf[off++] = (cksum >> 24) & 0xFF;
    buf[off++] = (cksum >> 16) & 0xFF;
    buf[off++] = (cksum >>  8) & 0xFF;
    buf[off++] = (cksum      ) & 0xFF;

    /* Sanity */
    assert(off == frame_size);

    /* ------------------------------------------------------------
     * 10. Write the entire frame to the socket
     * ------------------------------------------------------------ */
    ssize_t n = write(sock, buf, frame_size);
    free(buf);

    if (n != (ssize_t)frame_size)
        return -1;   /* short write or error */

    return 0;
}


- The caller provides key and value buffers; the encoder does no allocation for them.
- All integers must be written big-endian (network order).
- The checksum does NOT include the 2-byte length prefix.
- write(sock, buf, frame_size) must be wrapped in a write_all() helper in real code.
- Offsets and lengths are exact and fixed — the parser must mirror this structure.


```

---

### 2.3 Response Message Format

The response body uses the same framing and nearly the same header:

```
                                Response Message Body
                        (all fields shown in network byte order)

  0                   8                   16                  24                 32
  +-------------------+-------------------+-------------------+-------------------+
  |   opcode (8)      |                request_id (64)                            |
  +-------------------+-------------------+-------------------+-------------------+
  |               key_len (16)            |              value_len (16)           |
  +---------------------------+-----------------------------+---------------------+
  |                    value bytes (variable, value_len bytes)                    |
  +-------------------------------------------------------------------------------+
  |                               checksum (32)                                   |
  +-------------------------------------------------------------------------------+

Field definitions:
  opcode:       1 byte (100 OK_GET, 101 OK_PUT, 200 ERR_NOT_FOUND, 201 ERR_INVALID)
  request_id:   8 bytes (must match request)
  key_len:      2 bytes (usually 0)
  value_len:    2 bytes
  value:        value_len bytes (present only for OK_GET)
  checksum:     4 bytes, CRC32 over all fields from opcode through
                the end of value bytes.

```

#### Response Opcodes

* `100` = OK_GET
* `101` = OK_PUT
* `200` = ERR_NOT_FOUND
* `201` = ERR_INVALID
* `202` = ERR_SHARD_UNAVAILABLE
* `203` = ERR_ROUTER_INTERNAL
* `204` = ERR_TIMEOUT

The shard always echoes the `request_id`.

#### Response Decoding (Pseudocode)

``` c
/*
 * Decode a KV response frame received from a shard or router.
 * The first 2 bytes (uint16) specify the message length.
 * The message body begins immediately after the prefix.
 */

uint16_t L = read_uint16(sock);          /* read 2-byte length prefix */
uint8_t *body = read_exact(sock, L);     /* read full message body   */

/* ------------------------------------------------------------
 * Fixed header fields (byte offsets from start of `body`)
 *
 *  body[0]      = opcode (uint8)
 *  body[1..8]   = request_id (uint64)
 *  body[9..10]  = key_len (uint16)
 *  body[11..12] = value_len (uint16)
 *  body[13..]   = value bytes (value_len bytes)
 *  final 4 bytes = checksum (uint32)
 * ------------------------------------------------------------ */

uint8_t opcode = body[0];

/* parse request_id */
uint64_t request_id =
      ((uint64_t)body[1] << 56)
    | ((uint64_t)body[2] << 48)
    | ((uint64_t)body[3] << 40)
    | ((uint64_t)body[4] << 32)
    | ((uint64_t)body[5] << 24)
    | ((uint64_t)body[6] << 16)
    | ((uint64_t)body[7] <<  8)
    | ((uint64_t)body[8]      );

/* parse lengths */
uint16_t key_len =
      ((uint16_t)body[9] << 8)
    |  (uint16_t)body[10];

uint16_t value_len =
      ((uint16_t)body[11] << 8)
    |  (uint16_t)body[12];

/* extract value bytes (responses do not return key bytes) */
uint8_t *value = &body[13];   /* value starts at offset 13 */

/* compute where checksum begins */
size_t checksum_offset = 13 + value_len;

/* read checksum */
uint32_t recv_cksum =
      ((uint32_t)body[checksum_offset]     << 24)
    | ((uint32_t)body[checksum_offset + 1] << 16)
    | ((uint32_t)body[checksum_offset + 2] <<  8)
    | ((uint32_t)body[checksum_offset + 3]      );

/* verify checksum across entire body except the checksum field */
uint32_t calc_cksum = crc32(body, checksum_offset);

if (recv_cksum != calc_cksum) {
    /* checksum mismatch: treat as protocol error */
    handle_protocol_error();
}

/* response successfully decoded */


- The server must echo the client’s request_id; clients must use it to match responses to requests.
- opcode must be one of the defined response codes (OK_GET, OK_PUT, ERR_NOT_FOUND, ERR_INVALID).
- Responses never include key bytes; key_len is normally zero.
- All multi-byte integers must be transmitted in network byte order.
- value_len specifies the length of the returned value; a value of zero means no payload is present.
- The checksum is a 32-bit CRC over all body bytes except the checksum field itself; clients must verify it.
- Frames with invalid lengths, invalid checksums, or malformed fields must be rejected as protocol errors.
- Field offsets and sizes are fixed; implementations must follow this layout exactly.

```

---


## 3. Key–Value Storage Engine 

### 3.1 Overview

This section defines the storage engine for a two-shard in-memory key–value (KV) database. Routing is handled upstream via **Murmur3(key) % 2**, guaranteeing that every request arrives at the correct shard. Each shard is a **single-threaded**, self-contained storage engine implementing GET, PUT, DELETE, and EXISTS over opaque binary keys and values.

The design emphasizes correctness, simplicity, and deterministic performance. No persistence, replication, or distributed coordination is within scope.

---

### 3.2 Data Model

#### 3.2.1 Keys
A key is defined as an opaque binary blob:
```
key := [uint16 length][length bytes]
```

The shard does not interpret the bytes. All keys are binary-safe.

#### 3.2.2 Values
A value is defined identically:
```
value := [uint16 length][length bytes]
```
Values are also treated as opaque binary data.

---

### 3.3 Storage Substrate

Each shard maintains a **hash table** implemented as:

- A **contiguous bucket array** of size `capacity`.
- Each bucket holds a pointer to a **linked-list chain** of nodes (possibly empty).
- Linked lists resolve collisions.
- All operations are performed single-threadedly; no locking is required inside the shard.

#### 3.3.1 Node Layout

Each chain node has the following structure:
```
struct Node 
{
    uint16 key_len
    uint8 key_bytes[key_len]
    uint16 value_len
    uint8  value_bytes[value_len]

    Node* next
}
```

**Rationale:**  
Even after hashing, bucket collisions are possible. The shard must explicitly store the full key bytes to differentiate colliding keys and guarantee correctness.

#### 3.3.2 Node Memory Allocation

Nodes are allocated from a **fixed-size pool allocator**, configured with:

- A known maximum node size determined by key+value limits.
- O(1) allocation and free.
- Automatic pool expansion when its internal load factor exceeds 0.80.

**Rationale:**  
This avoids malloc fragmentation and provides predictable allocation latency.

---

### 3.4 Hash Table Behavior

#### 3.4.1 Load Factor and Resize Threshold
The hash table maintains a **load factor ≤ 0.80**:
```
load_factor = (number_of_stored_keys) / (number_of_buckets)
```
When the load factor exceeds `0.80` after an insert, the table **resizes**.

#### 3.4.2 Resize Operation

On resize:

1. Double the bucket array capacity.  
2. Allocate a new contiguous bucket array.  
3. Visit every node in every old bucket.  
4. For each node:
   - Recompute `new_index = hash(node.key) % new_capacity`
   - Move the node into the linked list at the new bucket index  
5. Free the old bucket array.

**Important:**  
Nodes are redistributed **individually**, not bucket-by-bucket.  
Different nodes from the same old bucket may land in *different* new buckets after rehashing.

#### 3.4.3 Hash Function

The shard trusts the router’s placement and does **not** compute shard identity.  
It **does** compute per-table bucket placement using:
```
bucket_index = Murmur3(key_bytes) % bucket_capacity
```

Bucket placement and shard placement use identical key bytes for hashing.

---

### 3.5 Execution Model

#### 3.5.1 Single-Threaded Guarantee
- Each shard executes incoming requests sequentially:
  - No concurrent mutations
  - No locks or atomics
  - Deterministic ordering of operations
  - Zero race possibility
  
#### 3.5.2 Per-Shard Event Loop

- Each shard runs its own dedicated event loop, multiplexing all client connections assigned to that shard.
This ensures a single-threaded but non-blocking execution path. 

#### 3.5.3 Pseudocode: Shard Event Loop

```c
while (true):

    // wait until either a new connection arrives or a client socket has data
    events = epoll_wait(shard_epoll_fd)

    for ev in events:

        // a new client is connecting to this shard
        if (ev.fd == listener_fd):
            client_fd = accept(listener_fd)          // take the connection off the queue
            set_nonblocking(client_fd)               // never block this single-threaded loop
            epoll_add(shard_epoll_fd, client_fd, READ) // start watching it for incoming data
            continue

        // an existing client has sent some bytes
        else if (ev.type == READ):

            // try to read a full framed request (length prefix + body)
            if (!read_frame(ev.fd, frame)):
                close(ev.fd)                         // client hung up or malformed frame
                continue

            request  = decode(frame)                 // parse opcode, key, value
            result   = kv_execute(request)           // run the actual hash-table operation (GET/PUT/DELETE/EXISTS)
            response = encode(result)                // build a proper response frame

            write(ev.fd, response)                   // send it back on the same socket

```

---

### 3.6 Operations

#### 3.6.1 PUT(key, value)
- Hash the key to determine the bucket  
- Traverse the bucket chain  
  - If the key exists → replace value  
  - Else → allocate new node and insert at head  
- Increment key count  
- Trigger a resize if load factor > 0.80  

#### 3.6.2 GET(key)
- Hash key  
- Traverse chain comparing stored key bytes  
- If found: return value  
- Else: NOT_FOUND  

#### 3.6.3 DELETE(key)
- Hash key  
- Traverse chain with a `prev` pointer  
- If found: unlink node, return to pool, decrement size  
- Else: NOT_FOUND  

#### 3.6.4 EXISTS(key)
- Same traversal as GET, without returning value  
- Returns boolean  

---

### 3.7 Error Semantics

The storage engine returns deterministic error codes:

- **OK**  
- **NOT_FOUND**  
- **BAD_REQUEST** (malformed key/value length or invalid args)  
- **OVERSIZED_PAYLOAD**  
- **INTERNAL_ERROR** (allocator failure or invariant violation)  
- **SHARD_UNAVAILABLE** (propagated from router, not produced by shard)
- **TIMEOUT** (shard took too long, router injects this)

```
| Shard Return Code     | Network Opcode Returned | Meaning on the Wire               |
|-----------------------|-------------------------|-----------------------------------|
| OK                    | 100 or 101              | GET OK or PUT OK                  |
| NOT_FOUND             | 200                     | ERR_NOT_FOUND                     |
| BAD_REQUEST           | 201                     | ERR_INVALID                       |
| OVERSIZED_PAYLOAD     | 201                     | ERR_INVALID                       |
| INTERNAL_ERROR        | 203                     | ERR_ROUTER_INTERNAL               |
| SHARD_UNAVAILABLE     | 202                     | ERR_SHARD_UNAVAILABLE (router)    |
| TIMEOUT               | 204                     | ERR_TIMEOUT (router)              |

```
The shard never emits 202 or 204; those come from the router.

---

### 3.8 Memory Ownership Model

- The shard **owns** all Node objects and all key/value bytes  
- Memory is freed on DELETE 
- Memory is moved not copied during rehash
- Client and router do not own or retain any shard-side memory  

---

### 3.9 Invariants

1. Each key appears at most once.  
2. Each key resides in exactly one bucket determined by:  
   `hash(key) % bucket_capacity`  
3. All nodes contain correct, self-owned copies of key and value bytes.  
4. Load factor never exceeds 0.80 after a completed resize.  
5. No concurrent access or mutation occurs inside the shard.  

---

## 4. Networking Flow, connection handling, timeouts

### 4.1 Networking Flow & Connection Handling
                                            NETWORK + CONNECTION LIFECYCLE FLOW
                                    ==================================================
                                            ┌─────────────────────────────────┐
                                            │         Client Devices          │
                                            │   (phones, tablets, services)   │
                                            └─────────────────────────────────┘
                                                            │ TCP frames
                                                            ▼
                                            ┌─────────────────────────────────┐
                                            │    Router: Acceptor Thread      │
                                            │  - accept() new connections     │
                                            │  - set_nonblocking(client_fd)   │
                                            │  - pick loop N (round robin)    │
                                            │  - push fd → loop[N].queue      │
                                            │  - eventfd_notify(loop[N])      │
                                            └─────────────────────────────────┘
                                                            │ fd handoff + wake
                                                            ▼
                ┌───────────────────────────────────────────────────────────────────────────────────────────────┐
                │                                  Router Event Loops (4× identical)                            │
                │ Each loop owns:                                                                               │
                │   - epoll_fd                               - eventfd_notify                                   │
                │   - new_conn_queue                         - persistent shard_fd[0], shard_fd[1]              │
                │   - its own set of client_fds                                                                 │
                │                                                                                               │
                │   ┌─────────────────────────────────────────────────────────────────────────────────────────┐ │
                │   │                             Event Loop i   (i = 0..3)                                   │ │
                │   │                                                                                         │ │
                │   │   On wake (eventfd):                                                                    │ │
                │   │       - drain new_conn_queue                                                            │ │
                │   │       - epoll_ctl(ADD client_fd)                                                        │ │
                │   │                                                                                         │ │
                │   │   Per request:                                                                          │ │
                │   │       1. epoll → client_fd readable                                                     │ │
                │   │       2. read full frame (enforce T_client_read, T_idle)                                │ │
                │   │       3. hash key → shard_id                                                            │ │
                │   │       4. forward frame → shard_fd[shard_id]                                             │ │
                │   │       5. record outstanding[request_id] = {client_fd, deadline = now + T_max_request}   │ │
                │   │                                                                                         │ │
                │   │   Response path:                                                                        │ │
                │   │       a. epoll → shard_fd readable                                                      │ │
                │   │       b. read shard response frame                                                      │ │
                │   │       c. extract request_id                                                             │ │
                │   │       d. find client_fd = outstanding[request_id].fd                                    │ │
                │   │       e. write response back to client_fd (enforce T_client_write)                      │ │
                │   │       f. erase outstanding[request_id]                                                  │ │
                │   │                                                                                         │ │
                │   │   Timeout and error handling:                                                           │ │
                │   │       - Periodic sweep: for each outstanding with deadline < now →                      │ │
                │   │             send synthetic error frame opcode 204 (ERR_TIMEOUT), then close client_fd   │ │
                │   │       - If shard_fd write or read fails or exceeds T_shard_response:                    │ │
                │   │             mark shard down, close shard_fd, start nonblocking reconnect with backoff   │ │
                │   │             fail in flight with opcode 204 (ERR_TIMEOUT)                                │ │
                │   │       - While shard is down:                                                            │ │
                │   │             new requests for that shard get immediate error opcode 202                  │ │
                │   │             (ERR_SHARD_UNAVAILABLE)                                                     │ │
                │   │       - On router internal bug or invariant violation:                                  │ │
                │   │             send opcode 203 (ERR_ROUTER_INTERNAL), then close fd                        │ │
                │   └─────────────────────────────────────────────────────────────────────────────────────────┘ │
                └───────────────────────────────────────────────────────────────────────────────────────────────┘
                                                            │
                                                            ▼
                                ┌─────────────────────────────────────────────────────────────┐
                                │                         Shard 0 / Shard 1                   │
                                │                (single threaded KV engines)                 │
                                │   - decode request                                          │
                                │   - run GET/PUT/DELETE                                      │
                                │   - build response frame                                    │
                                │   - on internal failure: drop router connection             │
                                │       router maps this to ERR_SHARD_UNAVAILABLE (202)      │
                                └─────────────────────────────────────────────────────────────┘
                                                            │
                                                            ▼
                                ┌─────────────────────────────────────────────────────────────┐
                                │                      Flow per response                      │
                                │   1. epoll → shard_fd readable                              │
                                │   2. read shard response frame                              │
                                │   3. extract request_id                                     │
                                │   4. find originating client_fd                             │
                                │   5. forward frame back to client                           │
                                │   6. clear outstanding entry                                │
                                └─────────────────────────────────────────────────────────────┘
                                                            │
                                                            ▼
                                ┌─────────────────────────────────────────────────────────────┐
                                │                           Client                            │
                                │    - read response                                          │
                                │    - verify checksum                                        │
                                │    - match request_id                                       │
                                │    - deliver result or surface ERR_* to caller              │
                                └─────────────────────────────────────────────────────────────┘

---

```c
for (;;) {

    // pull any new connections handed over by the acceptor
    while (new_conn_queue.pop(&fd)) {
        set_nonblocking(fd);                 // make sure it never blocks the loop
        epoll_ctl(epfd, ADD, fd, READ);      // tell kernel we care about reads on this fd
    }

    // wait until some fd becomes readable
    n = epoll_wait(epfd, events);

    for (i = 0; i < n; i++) {
        ev = events[i];

        // ------------ CLIENT → ROUTER → SHARD ------------
        if (ev.fd is client_fd && ev.readable) {

            frame = read_full_frame(client_fd);   // assemble full TCP-framed request
            if (!frame) {
                close(client_fd);                 // dead client or bad frame
                continue;
            }

            req_id = extract_request_id(frame);   // pull request_id from header
            key    = extract_key_slice(frame);    // pull key bytes for hashing

            shard_id = Murmur3(key) % 2;          // pick shard deterministically

            write_nonblocking(shard_fd[shard_id], frame); // forward to shard

            outstanding[req_id] = {               // remember who asked for it
                .client_fd = client_fd,
                .deadline  = now() + T_max_request
            };

            continue;
        }

        // ------------ SHARD → ROUTER → CLIENT ------------
        if (ev.fd is shard_fd[0] or shard_fd[1]) {

            resp = read_full_frame(ev.fd);        // get full shard response
            if (!resp) {
                mark_shard_down(ev.fd);           // the shard connection died
                fail_all_outstanding_for_that_shard();
                continue;
            }

            req_id    = extract_request_id(resp); // response always echoes this
            client_fd = outstanding[req_id].client_fd;

            write_nonblocking(client_fd, resp);   // deliver to the right client
            outstanding.erase(req_id);            // cleanup bookkeeping

            continue;
        }
    }

    // ------------ TIMEOUT HANDLING ------------
    for each entry in outstanding:
        if entry.deadline < now():
            send_err_timeout(entry.client_fd);    // synthetic timeout response
            close(entry.client_fd);               // kill stuck client
            outstanding.erase(entry.req_id);      // drop it from the table
}

```
### 4.2 Timeouts

- **T_client_read**  
  Fired when a client_fd took too long to deliver a complete request frame. Router dropped the partial frame and closed the connection.

- **T_client_write**  
  Fired when the router could not flush a response back to the client in time. Router sent ERR_TIMEOUT (204) and closed the client_fd.

- **T_idle**  
  Fired when a client stayed silent for too long. Router closed the idle client_fd to reclaim resources.

- **T_max_request**  
  Fired when a request sat in `outstanding` too long waiting for a shard response. Router emitted ERR_TIMEOUT (204) and removed the entry.

- **T_shard_response**  
  Fired when a shard_fd failed to return a response in the expected service window. Router marked the shard down and failed all in-flight requests with ERR_TIMEOUT (204).

- **T_shard_reconnect_backoff**  
  Fired during repeated attempts to reconnect to a down shard. Router waited according to its backoff policy and retried a nonblocking reconnect.


## 5. Handling Shard and Router Failures

Failure handling is minimalist. This system is small and correctness matters more than anything else.

### 5.1 Heartbeat Endpoints

Each router and each shard exposes a trivial heartbeat:

```
/health → “OK”
```

This is a nonblocking liveness probe used by external tooling. It exercises no KV logic and never touches shard data.
Each process exposes this on a separate port from the KV protocol, servicing probes without entering the KV path.

### 5.2 Shard Failures

If a shard process crashes or its TCP link drops:

* The router immediately marks the shard as unavailable.
* New requests targeting that shard get `ERR_SHARD_UNAVAILABLE (202)` with zero retries.
* In-flight requests to that shard time out and return `ERR_TIMEOUT (204)`.
* Router attempts nonblocking reconnect with exponential backoff.

### 5.3 Router Failures

Routers are stateless. If a router dies:

* Clients reconnect to a fresh (backup) router and resend the request.
* No state recovery is needed because routing uses only `hash(key) % 2`.
* Optional Backup Router: Optionally run a second router instance. Since routers hold no state, failover is trivial: clients transparently reconnect to whichever router is alive.

### 5.4 Per-Core Router Loop Failures

Each router event loop (core) is isolated. If one loop crashes:

* Only its own connections die.
* Clients reconnect and retry through any other loop because routing is stateless.
* No cross-loop coordination is required.

