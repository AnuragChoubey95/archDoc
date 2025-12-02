
# RFC: Sharded Key-Value Store with Stateless Routing

## 1. Abstract

This document specifies the architecture and implementation of a high-performance distributed Key-Value (KV) store built using modern C++23. The system demonstrates a shared-nothing architecture in which data is partitioned across multiple storage nodes called Shards, accessed via a layer of stateless Router processes. Deterministic behavior, predictable latency, and code simplicity are achieved by using a single-threaded, event-driven (Reactor) concurrency model instead of traditional multi-threaded locking.

## 2. System Goals

The design targets a scalable storage substrate capable of sustaining high throughput with minimal latency spikes. Horizontal scalability is achieved through partitioning the overall key-space across Shard nodes so that adding more Shards increases both total storage capacity and aggregate throughput. Routers remain fully stateless, making them trivially scalable, fault tolerant, and simple to replace or restart without any data migration. Predictable performance comes from eliminating locks and thread contention within the Shards, which process all requests on a single Reactor thread. 

## 3. Wire Protocol Specification

All communication occurs over raw TCP using a compact binary protocol designed for zero-copy parsing and efficient dispatch.

### 3.1 Message Framing and Layout

Each frame begins with a two byte length prefix, followed by a fixed header and variable length key and value slices.

```

+----------------+------------------------------------------------------------+
| Length (2B)    | OpCode(1) | ReqID(8) | KeyLen(2) | ValLen(2) | ...payload |
+----------------+------------------------------------------------------------+

````

**Reference:** `include/kv/core.hpp`

```cpp
enum class OpCode : uint8_t {
    Get    = 0x00,
    Put    = 0x01,
    Delete = 0x02,
    Exists = 0x03,
};

struct MessageFrame {
    OpCode opcode;
    RequestId req_id; 
    KeyView   key;
    ValueView value;
};
````

### 3.2 Serialization

Frames are serialized in network byte order using dedicated helpers to write integers in big endian format. A CRC32 checksum is appended to validate integrity before dispatch to Shard nodes.

**Reference:** `src/core.cpp`

```cpp
std::vector<Byte> encode_frame(const MessageFrame& frame) {
    std::vector<Byte> buffer;

    uint16_t payload_size =
        1 + 8 + 2 + 2 +
        frame.key.size() +
        frame.value.size() +
        4; // checksum

    write_be(buffer, payload_size);
    buffer.push_back(static_cast<Byte>(frame.opcode));
    write_be(buffer, frame.req_id);
    write_be(buffer, static_cast<uint16_t>(frame.key.size()));
    write_be(buffer, static_cast<uint16_t>(frame.value.size()));

    buffer.insert(buffer.end(), frame.key.begin(), frame.key.end());
    buffer.insert(buffer.end(), frame.value.begin(), frame.value.end());

    uint32_t checksum = crc32(payload_view);
    write_be(buffer, checksum);

    return buffer;
}
```

## 4. Networking Subsystem (Reactor)

The Reactor abstracts the OS event multiplexer (epoll on Linux, kqueue on macOS) and drives both Routers and Shards using a unified event loop. Instead of maintaining threads per connection, the Reactor uses a single worker thread to orchestrate all IO through non-blocking sockets.

### 4.1 Event Loop

The event loop computes the nearest timer deadline, blocks in the OS multiplexer, and dispatches IO callbacks registered against each file descriptor.

**Reference:** `src/net/reactor.cpp`

```cpp
void EventLoop::run() {
    while (running_) {
        int timeout_ms = compute_timer_deadline();
        int n = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;

            if (is_read && handlers_[fd]->read_cb)
                handlers_[fd]->read_cb(fd);

            if (is_write && handlers_[fd]->write_cb)
                handlers_[fd]->write_cb(fd);
        }

        process_timers();
    }
}
```

## 5. Router Subsystem

Routers are stateless proxy processes that handle request routing, connection multiplexing, and asynchronous request tracking.

### 5.1 Process Model: Preforked Workers

To exploit multi-core CPUs without complex threading logic, the Router spawns multiple worker processes. All workers bind to the same port using `SO_REUSEPORT`, allowing the OS kernel to distribute incoming connections across workers.

**Reference:** `src/bin/router_main.cpp`

```cpp
for (int i = 0; i < num_workers; ++i) {
    pid_t pid = fork();
    if (pid == 0) {
        run_worker(i, config);
        return 0;
    }
}
```

### 5.2 Routing Logic and Request Tracking

Routers compute `ShardIndex = Murmur3(Key) % ShardCount` to ensure deterministic routing. Because requests are asynchronous, a global request ID is created and mapped to the client's original request metadata so Shard responses can be forwarded back to the originating client.

**Reference:** `src/router/service.cpp`

```cpp
void RouterService::process_client_frame(int client_fd, std::span<const Byte> body) {
    auto frame = decode(body);

    RequestId global = next_global_req_id_++;
    in_flight_[global] = { client_fd, frame.req_id, std::chrono::steady_clock::now() };

    uint32_t hash = Hasher::hash(frame.key);
    size_t shard_idx = hash % shards_.size();

    forward_to_shard(shard_idx, raw_fwd, frame.req_id, client_fd);
}
```

## 6. Shard Subsystem

Each Shard is a fully single-threaded process running the Reactor. Its job is to execute storage operations sequentially against its in-memory engine.

### 6.1 Execution Logic

Since only one thread touches storage data structures, no locks or atomic primitives are needed in the critical path. This produces predictable, deterministic performance.

**Reference:** `src/shard/service.cpp`

```cpp
void ShardService::process_frame(int client_fd, std::span<const Byte> body) {
    auto frame = decode(body);

    switch (frame.opcode) {
        case OpCode::Put:
            if (!store_.put(frame.key, frame.value))
                send_error(client_fd, frame.req_id, OpCode::Error);
            else
                send_response(client_fd, frame.req_id, OpCode::OkPut);
            break;

        case OpCode::Get:
            if (auto res = store_.get(frame.key))
                send_response(client_fd, frame.req_id, OpCode::OkGet, res.value());
            else
                send_error(client_fd, frame.req_id, OpCode::NotFound);
            break;

        // ...
    }
}
```

## 7. Storage Engine

The storage engine is an in-memory hash map backed by a pool allocator implemented with a single large mmap region.

### 7.1 Pool Allocator

The allocator reserves a contiguous memory range and divides it into fixed blocks managed by a free list. Allocations run in constant time and incur no syscalls after initialization.

**Reference:** `src/store/engine.cpp`

```cpp
PoolAllocator::PoolAllocator(size_t capacity_blocks) {
    memory_start_ = ::mmap(nullptr, memory_size_,
                           PROT_READ | PROT_WRITE,
                           MAP_PRIVATE | MAP_ANONYMOUS,
                           -1, 0);
    // Build free list...
}

void* PoolAllocator::allocate() {
    if (!free_list_) return nullptr;
    void* blk = free_list_;
    free_list_ = free_list_->next;
    used_++;
    return blk;
}
```

### 7.2 Hash Map

The map uses separate chaining with nodes allocated directly from the pool. Resizing doubles bucket count and rehashes all nodes while still running on the single thread.

**Reference:** `src/store/engine.cpp`

```cpp
std::optional<core::ProtocolError> Map::put(core::KeyView key, core::ValueView value) {
    size_t idx = bucket_index(key);
    Node* curr = buckets_[idx];

    while (curr) {
        if (std::ranges::equal(curr->key_view(), key)) {
            // update
            return std::nullopt;
        }
        curr = curr->next;
    }

    void* block = pool_.allocate();
    Node* node = new (block) Node();
    // copy fields...

    node->next = buckets_[idx];
    buckets_[idx] = node;

    rehash_if_needed();
    return std::nullopt;
}
```

## 8. Future Work

- Add simple logging and metrics so we can see what the Router and Shards are doing without guessing.
- Add observability hooks like latency counters and per-shard stats to make debugging less of a pain.
- Add replication so shards aren’t single points of failure and the system can survive a node going down.