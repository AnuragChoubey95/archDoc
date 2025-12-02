#pragma once

/**
 * @file router.hpp
 * @brief Defines the Stateless Router Service.
 */

#include "kv/core.hpp"
#include "kv/net.hpp"
#include <unordered_map>
#include <vector>
#include <string>
#include <memory>
#include <deque>

namespace kv::router {

    /**
     * @brief Context for a connected client or shard.
     * * buffers incoming data until a full frame is ready.
     */
    struct ConnectionContext {
        net::Socket socket;
        std::vector<core::Byte> buffer;      // Input buffer
        std::vector<core::Byte> out_buffer;  // Output buffer for handling EAGAIN

        explicit ConnectionContext(net::Socket&& s) : socket(std::move(s)) {}
    };

    /**
     * @brief The Stateless Router Service.
     * * - Accepts client connections.
     * - Maintains persistent connections to Shards.
     * - Routes requests based on Hash(Key) % ShardCount.
     * - Forwards responses back to clients.
     */
    class RouterService {
    public:
        /**
         * @brief Configuration for the Router.
         */
        struct Config {
            uint16_t port;
            std::vector<std::pair<std::string, uint16_t>> shard_addresses;
        };

        explicit RouterService(Config config);
        ~RouterService() = default;

        /**
         * @brief Starts the Router Event Loop.
         * * Blocks the calling thread.
         */
        void run();

        /**
         * @brief Stops the Router.
         */
        void stop();

    private:
        Config config_;
        net::EventLoop loop_;
        net::Socket server_socket_;

        // Shard Management
        struct ShardConnection {
            std::unique_ptr<ConnectionContext> conn;
            std::string ip;
            uint16_t port;
            bool active = false;
        };
        std::vector<ShardConnection> shards_;

        // Client Management
        std::unordered_map<int, std::unique_ptr<ConnectionContext>> clients_;

        // Request Tracking
        struct InFlightReq {
            int client_fd;
            core::RequestId original_req_id; // The ID sent by the client
            std::chrono::steady_clock::time_point timestamp;
        };
        std::unordered_map<core::RequestId, InFlightReq> in_flight_;
        core::RequestId next_global_req_id_ = 1;

        // --- Event Handlers ---

        void on_accept(int listener_fd);
        void on_client_data(int client_fd);
        void on_shard_data(int shard_fd, size_t shard_idx);

        // --- Logic ---

        void connect_to_shards();
        void process_client_frame(int client_fd, std::span<const core::Byte> frame_data);
        
        // Updated to take original_req_id for error reporting
        void forward_to_shard(size_t shard_idx, std::span<const core::Byte> raw_frame, core::RequestId original_req_id, int client_fd);
        
        void send_error_to_client(int client_fd, core::RequestId req_id, core::OpCode error_code);
        void flush_output(ConnectionContext& ctx, int fd);
    };

} // namespace kv::router