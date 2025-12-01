/**
 * @file service.cpp
 * @brief Implementation of the ShardService logic.
 */

#include "kv/shard.hpp"
#include <iostream>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace kv::shard {

    using namespace kv::core;
    using namespace kv::net;

    ShardService::ShardService(Config config) 
        : config_(std::move(config))
        , store_(config_.initial_bucket_count) 
    {
    }

    void ShardService::run() {
        // 1. Setup Server Socket
        auto sock_res = net::Socket{::socket(AF_INET, SOCK_STREAM, 0)};
        if (!sock_res.valid()) throw std::runtime_error("Failed to create socket");
        server_socket_ = std::move(sock_res);

        server_socket_.set_reuse_addr();
        server_socket_.set_reuse_port(); 
        server_socket_.set_non_blocking();
        
        if (!server_socket_.bind_inaddr_any(config_.port)) {
            throw std::runtime_error("Failed to bind shard port");
        }
        
        if (!server_socket_.listen()) {
            throw std::runtime_error("Failed to listen on shard port");
        }

        std::cout << "[Shard] Listening on port " << config_.port << "\n" << std::flush;

        // 2. Register Accept Handler
        loop_.on_read(server_socket_.native_handle(), [this](int fd) {
            this->on_accept(fd);
        });

        // 3. Run Loop
        loop_.run();
    }

    void ShardService::stop() {
        loop_.stop();
    }

    void ShardService::on_accept(int listener_fd) {
        auto res = server_socket_.accept();
        if (!res) return;

        Socket client_sock = std::move(res.value());
        client_sock.set_non_blocking();
        int fd = client_sock.native_handle();

        clients_[fd] = std::make_unique<ConnectionContext>(std::move(client_sock));

        loop_.on_read(fd, [this](int fd) {
            this->on_data(fd);
        });
        
        std::cout << "[Shard] Accepted connection: " << fd << "\n" << std::flush;
    }

    // Helper: Tries to extract full frames from a buffer
    template<typename Handler>
    size_t process_buffer(std::vector<Byte>& buffer, Handler&& handler) {
        size_t offset = 0;
        
        while (offset + LENGTH_PREFIX_SIZE <= buffer.size()) {
            std::span<const Byte> prefix_span(buffer.data() + offset, LENGTH_PREFIX_SIZE);
            auto len_res = decode_length_prefix(prefix_span);
            if (!len_res) break; 

            size_t payload_len = len_res.value();
            size_t total_frame_len = LENGTH_PREFIX_SIZE + payload_len;

            if (offset + total_frame_len > buffer.size()) {
                break; // Incomplete
            }

            // Extract Body (excluding prefix)
            std::span<const Byte> body_span(buffer.data() + offset + LENGTH_PREFIX_SIZE, payload_len);
            
            handler(body_span);

            offset += total_frame_len;
        }
        return offset;
    }

    void ShardService::on_data(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;

        auto& ctx = it->second;
        
        Byte temp[4096];
        auto read_res = ctx->socket.read(temp);

        if (!read_res || read_res.value() == 0) {
            std::cout << "[Shard] Client disconnected: " << fd << "\n" << std::flush;
            clients_.erase(fd);
            loop_.remove(fd);
            return;
        }

        size_t n = read_res.value();
        ctx->buffer.insert(ctx->buffer.end(), temp, temp + n);

        size_t consumed = process_buffer(ctx->buffer, [this, fd](std::span<const Byte> body) {
            this->process_frame(fd, body);
        });

        if (consumed > 0) {
            ctx->buffer.erase(ctx->buffer.begin(), ctx->buffer.begin() + consumed);
        }
    }

    void ShardService::process_frame(int client_fd, std::span<const Byte> body) {
        auto frame_res = decode_frame_body(body);
        if (!frame_res) {
            // Protocol error
            std::cerr << "[Shard] Bad frame from client " << client_fd << "\n" << std::flush;
            // In a real system we might send an error frame here if we could extract ReqID
            return;
        }

        const auto& frame = frame_res.value();
        
        // Debug Log
        std::cout << "[Shard] Processing OpCode " << static_cast<int>(frame.opcode) 
                  << " for ReqID " << frame.req_id << "\n" << std::flush;

        // --- COMMAND DISPATCH ---
        switch (frame.opcode) {
            case OpCode::Get: {
                auto res = store_.get(frame.key);
                if (res.has_value()) {
                    // Success: Return Value
                    // Convert vector<Byte> to span
                    std::span<const Byte> val_span(res.value().data(), res.value().size());
                    send_response(client_fd, frame.req_id, OpCode::OkGet, val_span);
                } else {
                    // Not Found (or other error mapped to Not Found for GET)
                    send_error(client_fd, frame.req_id, OpCode::NotFound);
                }
                break;
            }

            case OpCode::Put: {
                auto err = store_.put(frame.key, frame.value);
                if (!err) {
                    // Success: Return Empty OK
                    send_response(client_fd, frame.req_id, OpCode::OkPut);
                } else {
                    // Error (e.g. Too Large)
                    send_error(client_fd, frame.req_id, OpCode::Error);
                }
                break;
            }

            case OpCode::Delete: {
                auto err = store_.del(frame.key);
                if (!err) {
                    send_response(client_fd, frame.req_id, OpCode::OkPut);
                } else {
                    // The spec says: If not found -> NOT_FOUND
                    send_error(client_fd, frame.req_id, OpCode::NotFound);
                }
                break;
            }
            
            case OpCode::Exists: {
                // EXISTS behaves like GET but returns boolean (Success/NotFound)
                // We reuse GET logic but discard value
                auto res = store_.get(frame.key);
                if (res.has_value()) {
                    send_response(client_fd, frame.req_id, OpCode::OkPut);
                } else {
                    send_error(client_fd, frame.req_id, OpCode::NotFound);
                }
                break;
            }

            default:
                send_error(client_fd, frame.req_id, OpCode::Error);
                break;
        }
    }

    void ShardService::send_response(int client_fd, RequestId req_id, OpCode op, std::span<const Byte> value) {
        if (clients_.find(client_fd) == clients_.end()) return;

        MessageFrame resp {
            .opcode = op,
            .req_id = req_id,
            .key = {}, // Responses don't echo key
            .value = value
        };

        auto raw = encode_frame(resp);
        clients_[client_fd]->socket.write(raw);
    }

    void ShardService::send_error(int client_fd, RequestId req_id, OpCode op) {
        send_response(client_fd, req_id, op, {});
    }

} // namespace kv::shard