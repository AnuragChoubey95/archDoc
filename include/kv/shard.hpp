#pragma once

/**
 * @file shard.hpp
 * @brief Defines the Shard Service (Data Node).
 */

#include "kv/core.hpp"
#include "kv/net.hpp"
#include "kv/store.hpp"
#include <unordered_map>
#include <vector>
#include <memory>

namespace kv::shard {

    /**
     * @brief Context for a connected client (usually the Router).
     */
    struct ConnectionContext {
        net::Socket socket;
        std::vector<core::Byte> buffer; // Accumulation buffer for TCP stream

        explicit ConnectionContext(net::Socket&& s) : socket(std::move(s)) {}
    };

    /**
     * @brief The Shard Service.
     * * Responsibilities:
     * - Listen on a TCP port.
     * - Accept connections (from Router).
     * - Deserialize MessageFrames.
     * - Execute commands against the Storage Engine (kv::store::Map).
     * - Serialize and send responses.
     */
    class ShardService {
    public:
        struct Config {
            uint16_t port;
            size_t initial_bucket_count = 1024;
        };

        explicit ShardService(Config config);
        ~ShardService() = default;

        /**
         * @brief Starts the Shard Event Loop.
         * * Blocks the calling thread.
         */
        void run();

        /**
         * @brief Stops the Shard.
         */
        void stop();

    private:
        Config config_;
        net::EventLoop loop_;
        net::Socket server_socket_;
        
        // The Storage Engine (Owns the data)
        store::Map store_;

        // Active Connections (fd -> Context)
        std::unordered_map<int, std::unique_ptr<ConnectionContext>> clients_;

        // --- Event Handlers ---
        void on_accept(int listener_fd);
        void on_data(int client_fd);

        // --- Logic ---
        void process_frame(int client_fd, std::span<const core::Byte> body);
        void send_response(int client_fd, core::RequestId req_id, core::OpCode op, std::span<const core::Byte> value = {});
        void send_error(int client_fd, core::RequestId req_id, core::OpCode op);
    };

} // namespace kv::shard