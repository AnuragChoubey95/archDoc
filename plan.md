This is a high-level breakdown of the project structure. Each section below represents a specific logical component, which maps to corresponding header (`.hpp`/`.ixx`) and implementation (`.cpp`) files.

[cite_start]This structure enforces the "Clean abstraction layers" requirement [cite: 63] by strictly separating Networking, Protocol, Storage, and Service logic.

### 1. Core & Protocol Unit (`kv::core`)
**Files:** `include/kv/core.hpp`, `src/core.cpp`

This is the foundational vocabulary of the system. It has **zero dependencies** on networking or storage. It is linked by both the Router and the Shards.

* **Responsibilities:**
    * **Types:** Defines strong types like `KeyView` (`std::span`), `ValueView`, `RequestId`.
    * **Hashing:** Implements `Murmur3` (or similar) as a `constexpr` function to ensure the Router and Shard use the exact same logic for partition verification.
    * **Wire Format:** Defines the `MessageFrame` struct.
    * **Serialization:** Provides `encode_frame()` and `decode_header()` functions. These must use `std::span` to avoid unnecessary memory copying.
* **Dependencies:** None.

### 2. Networking Unit (`kv::net`)
**Files:** `include/kv/net.hpp`, `src/net/socket.cpp`, `src/net/reactor.cpp`

This unit abstracts the operating system's IO (Linux `epoll` or macOS `kqueue`) into a modern C++ Reactor pattern. It handles bytes, not "messages."

* **Responsibilities:**
    * **RAII Socket Wrapper:** A `Socket` class that manages file descriptors, ensuring `close()` is called on destruction. It handles non-blocking flags.
    * **The Reactor (Event Loop):** An `EventLoop` class that wraps `epoll_wait`. It accepts specific callbacks (`on_read`, `on_write`, `on_error`) using `std::function` or templates.
    * [cite_start]**Timer Wheel:** Manages the timeouts required by the mandate[cite: 57], handling `T_client_read` and `T_idle`.
* **Dependencies:** System headers (`<sys/socket.h>`, `<sys/epoll.h>`).

### 3. Storage Unit (`kv::store`)
**Files:** `include/kv/store.hpp`, `src/store/engine.cpp`

This unit contains the actual Hash Table logic. It is linked **only** by the Shard executables.

* **Responsibilities:**
    * **Memory Management:** A `PoolAllocator` class to manage `Node` memory efficiently (avoiding `new`/`delete` fragmentation).
    * **Hash Table Logic:** The `Map` class implementing `get`, `put`, `del`, `exists`. It handles collision resolution (chaining) and resizing.
    * [cite_start]**Thread Safety:** Since the mandate allows single-threaded execution per shard[cite: 68], this unit can remain lock-free *if* the Shard Service guarantees single-threaded access. Otherwise, it uses `std::mutex` internally.
* **Dependencies:** `kv::core`.

### 4. Router Service Unit (`kv::router`)
**Files:** `include/kv/router.hpp`, `src/router/service.cpp`, `src/bin/router_main.cpp`

This is the specific business logic for the Router process. It acts as the "glue" between the Networking layer and the Core protocol.

* **Responsibilities:**
    * **Connection Manager:** Maintains the persistent connections to Shard 0 and Shard 1.
    * [cite_start]**Routing Logic:** Implements the `hash(key) % 2` strategy[cite: 55].
    * **Request Lifecycle:** Maps incoming client `client_fd` to outgoing `shard_fd` and tracks in-flight requests in an `std::unordered_map` for response forwarding.
    * [cite_start]**Failure Handling:** Detects if a Shard disconnects and replies with `ERR_SHARD_UNAVAILABLE`[cite: 58].
* **Dependencies:** `kv::core`, `kv::net`.

### 5. Shard Service Unit (`kv::shard`)
**Files:** `include/kv/shard.hpp`, `src/shard/service.cpp`, `src/bin/shard_main.cpp`

This is the specific business logic for the Shard process.

* **Responsibilities:**
    * **Service Loop:** Initializes the `kv::net::EventLoop`.
    * **Command Dispatch:** Reads a `MessageFrame`, determines the `OpCode` (PUT/GET), and invokes the `kv::store::Map`.
    * **Response Generation:** Takes the result from Storage and serializes it back into a `MessageFrame` to send to the Router.
* **Dependencies:** `kv::core`, `kv::net`, `kv::store`.

### 6. Client Library Unit (`kv::client`)
**Files:** `include/kv/client.hpp`, `src/client.cpp`

[cite_start]A user-friendly C++ library for interacting with the system, required for the demo[cite: 74].

* **Responsibilities:**
    * **Sync API:** Provides blocking `get(key)`, `put(key, val)` methods.
    * **Connection:** Manages the TCP connection to the Router.
    * **Retry Logic:** If the Router disconnects, it handles reconnection.
* **Dependencies:** `kv::core`, `kv::net`.

### Summary of Compilation Dependencies

1.  **Router Executable** links: `core`, `net`, `router`
2.  **Shard Executable** links: `core`, `net`, `store`, `shard`
3.  **Client Demo** links: `core`, `net`, `client`

This modular approach allows you to implement and test the **Storage Engine** in isolation before ever writing a line of networking code, and vice versa.