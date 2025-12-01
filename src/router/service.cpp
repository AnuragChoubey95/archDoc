/**
 * @file service.cpp
 * @brief Implementation of the RouterService logic.
 */

#include "kv/router.hpp"
#include <iostream>
#include <algorithm>
#include <thread>
#include <sys/socket.h> // Required for AF_INET, SOCK_STREAM, ::socket
#include <netinet/in.h> // Required for IPPROTO_TCP, sockaddr_in
#include <arpa/inet.h>  // Required for inet_addr

namespace kv::router {

    using namespace kv::core;
    using namespace kv::net;

    RouterService::RouterService(Config config) : config_(std::move(config)) {
        // Initialize Shard structures
        for (const auto& addr : config_.shard_addresses) {
            ShardConnection sc;
            sc.ip = addr.first;
            sc.port = addr.second;
            shards_.push_back(std::move(sc));
        }
    }

    void RouterService::run() {
        // 1. Setup Server Socket
        // Explicitly include <sys/socket.h> to use AF_INET and SOCK_STREAM
        auto sock_res = net::Socket{::socket(AF_INET, SOCK_STREAM, 0)};
        if (!sock_res.valid()) throw std::runtime_error("Failed to create socket");
        server_socket_ = std::move(sock_res);

        server_socket_.set_reuse_addr();
        server_socket_.set_reuse_port(); // Required for Prefork/Multi-process binding
        server_socket_.set_non_blocking();
        
        if (!server_socket_.bind_inaddr_any(config_.port)) {
            // Note: If SO_REUSEPORT failed, only worker 0 succeeds and others crash here.
            throw std::runtime_error("Failed to bind router port");
        }
        
        if (!server_socket_.listen()) {
            throw std::runtime_error("Failed to listen on router port");
        }

        std::cout << "[Router] Listening on port " << config_.port << "\n" << std::flush; // <-- FLUSH ADDED

        // 2. Register Accept Handler
        loop_.on_read(server_socket_.native_handle(), [this](int fd) {
            this->on_accept(fd);
        });

        // 3. Connect to Shards
        connect_to_shards();

        // 4. Run Loop
        loop_.run();
    }

    void RouterService::stop() {
        loop_.stop();
    }

    void RouterService::connect_to_shards() {
        for (size_t i = 0; i < shards_.size(); ++i) {
            auto& shard = shards_[i];
            
            // Create socket
            auto sock = net::Socket{::socket(AF_INET, SOCK_STREAM, 0)};
            sock.set_non_blocking();

            // Attempt connect
            auto res = sock.connect(shard.ip, shard.port);
            
            int fd = sock.native_handle();
            shard.conn = std::make_unique<ConnectionContext>(std::move(sock));
            shard.active = true;

            // Register Shard Handlers
            loop_.on_read(fd, [this, i, fd](int) {
                this->on_shard_data(fd, i);
            });

            std::cout << "[Router] Connected to Shard " << i << " at " << shard.ip << ":" << shard.port << "\n" << std::flush; // <-- FLUSH ADDED
        }
    }

    void RouterService::on_accept(int listener_fd) {
        auto res = server_socket_.accept();
        if (!res) return;

        Socket client_sock = std::move(res.value());
        client_sock.set_non_blocking();
        int fd = client_sock.native_handle();

        // Create Context
        clients_[fd] = std::make_unique<ConnectionContext>(std::move(client_sock));

        // Register Handler
        loop_.on_read(fd, [this](int fd) {
            this->on_client_data(fd);
        });
    }

    // Helper: Tries to extract full frames from a buffer
    // Returns: Number of bytes consumed
    template<typename Handler>
    size_t process_buffer(std::vector<Byte>& buffer, Handler&& handler) {
        size_t offset = 0;
        
        while (offset + LENGTH_PREFIX_SIZE <= buffer.size()) {
            // Peek length
            std::span<const Byte> prefix_span(buffer.data() + offset, LENGTH_PREFIX_SIZE);
            auto len_res = decode_length_prefix(prefix_span);
            if (!len_res) break; // Should not happen if size check passed

            size_t payload_len = len_res.value();
            size_t total_frame_len = LENGTH_PREFIX_SIZE + payload_len;

            if (offset + total_frame_len > buffer.size()) {
                // Incomplete frame, wait for more data
                break; 
            }

            // We have a full frame
            std::span<const Byte> frame_span(buffer.data() + offset, total_frame_len);
            
            // Invoke handler (skip prefix for body decoding)
            handler(frame_span);

            offset += total_frame_len;
        }
        return offset;
    }

    void RouterService::on_client_data(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;

        auto& ctx = it->second;
        
        // 1. Read into temp buffer
        Byte temp[4096];
        auto read_res = ctx->socket.read(temp);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        if (!read_res || read_res.value() == 0) {
            // EOF or Error
            clients_.erase(fd);
            loop_.remove(fd);
            return;
        }

        // 2. Append to Context Buffer
        size_t n = read_res.value();
        ctx->buffer.insert(ctx->buffer.end(), temp, temp + n);

        // 3. Process Frames
        size_t consumed = process_buffer(ctx->buffer, [this, fd](std::span<const Byte> frame) {
            // Frame includes prefix here
            this->process_client_frame(fd, frame.subspan(LENGTH_PREFIX_SIZE));
        });

        // 4. Cleanup buffer
        if (consumed > 0) {
            ctx->buffer.erase(ctx->buffer.begin(), ctx->buffer.begin() + consumed);
        }
    }

    void RouterService::process_client_frame(int client_fd, std::span<const Byte> body) {
        // 1. Decode Header (Zero Copy)
        auto frame_res = decode_frame_body(body);
        if (!frame_res) {
            std::cerr << "[Router] Bad frame from client " << client_fd << "\n";
            clients_.erase(client_fd);
            loop_.remove(client_fd);
            return;
        }

        const auto& frame = frame_res.value();

        // 2. Routing Logic
        uint32_t hash = Hasher::hash(frame.key);
        size_t shard_idx = hash % shards_.size();

        // 3. Re-construct raw frame to forward
        std::span<const Byte> raw_frame(body.data() - LENGTH_PREFIX_SIZE, body.size() + LENGTH_PREFIX_SIZE);

        forward_to_shard(shard_idx, raw_frame, frame.req_id, client_fd);
    }

    void RouterService::forward_to_shard(size_t shard_idx, std::span<const Byte> raw_frame, RequestId req_id, int client_fd) {
        if (shard_idx >= shards_.size() || !shards_[shard_idx].active) {
            send_error_to_client(client_fd, req_id, OpCode::Error); // 202 Unavailable
            return;
        }

        auto& shard = shards_[shard_idx];
        auto write_res = shard.conn->socket.write(raw_frame);

        if (!write_res) {
            // Write failed (shard died?)
            shard.active = false;
            send_error_to_client(client_fd, req_id, OpCode::Error); // 202
            return;
        }

        // Track In-Flight
        in_flight_[req_id] = { client_fd, std::chrono::steady_clock::now() };
    }

    void RouterService::on_shard_data(int shard_fd, size_t shard_idx) {
        auto& shard = shards_[shard_idx];
        
        Byte temp[4096];
        auto read_res = shard.conn->socket.read(temp);

        if (!read_res || read_res.value() == 0) {
            std::cerr << "[Router] Shard " << shard_idx << " disconnected.\n" << std::flush; // <-- FLUSH ADDED
            shard.active = false;
            loop_.remove(shard_fd);
            return;
        }

        size_t n = read_res.value();
        shard.conn->buffer.insert(shard.conn->buffer.end(), temp, temp + n);

        size_t consumed = process_buffer(shard.conn->buffer, [this](std::span<const Byte> frame_with_prefix) {
            // 1. Decode Body
            auto body = frame_with_prefix.subspan(LENGTH_PREFIX_SIZE);
            auto frame_res = decode_frame_body(body);
            if (!frame_res) return;

            auto& frame = frame_res.value();
            
            // 2. Find Client
            auto it = in_flight_.find(frame.req_id);
            if (it != in_flight_.end()) {
                int client_fd = it->second.client_fd;
                
                // 3. Forward to Client
                if (clients_.count(client_fd)) {
                    clients_[client_fd]->socket.write(frame_with_prefix);
                }
                
                // 4. Cleanup
                in_flight_.erase(it);
            }
        });

        if (consumed > 0) {
            shard.conn->buffer.erase(shard.conn->buffer.begin(), shard.conn->buffer.begin() + consumed);
        }
    }

    void RouterService::send_error_to_client(int client_fd, RequestId req_id, OpCode error_code) {
        if (clients_.find(client_fd) == clients_.end()) return;

        MessageFrame err_frame {
            .opcode = error_code,
            .req_id = req_id,
            .key = {},
            .value = {}
        };

        auto raw = encode_frame(err_frame);
        clients_[client_fd]->socket.write(raw);
    }

} // namespace kv::router