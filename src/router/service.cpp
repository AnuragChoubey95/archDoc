/**
 * @file service.cpp
 * @brief Implementation of the RouterService logic.
 */

#include "kv/router.hpp"
#include <iostream>
#include <algorithm>
#include <thread>
#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>  

namespace kv::router {

    using namespace kv::core;
    using namespace kv::net;

    RouterService::RouterService(Config config) : config_(std::move(config)) {
        for (const auto& addr : config_.shard_addresses) {
            ShardConnection sc;
            sc.ip = addr.first;
            sc.port = addr.second;
            shards_.push_back(std::move(sc));
        }
    }

    void RouterService::run() {
        auto sock_res = net::Socket{::socket(AF_INET, SOCK_STREAM, 0)};
        if (!sock_res.valid()) throw std::runtime_error("Failed to create socket");
        server_socket_ = std::move(sock_res);

        server_socket_.set_reuse_addr();
        server_socket_.set_reuse_port(); 
        server_socket_.set_non_blocking();
        
        if (!server_socket_.bind_inaddr_any(config_.port)) {
            throw std::runtime_error("Failed to bind router port");
        }
        
        if (!server_socket_.listen()) {
            throw std::runtime_error("Failed to listen on router port");
        }

        std::cout << "[Router] Listening on port " << config_.port << "\n" << std::flush;

        loop_.on_read(server_socket_.native_handle(), [this](int fd) {
            this->on_accept(fd); 
        });

        connect_to_shards();
        loop_.run();
    }

    void RouterService::stop() {
        loop_.stop();
    }

    void RouterService::connect_to_shards() {
        for (size_t i = 0; i < shards_.size(); ++i) {
            auto& shard = shards_[i];
            
            auto sock = net::Socket{::socket(AF_INET, SOCK_STREAM, 0)};
            sock.set_non_blocking();

            auto res = sock.connect(shard.ip, shard.port);
            
            int fd = sock.native_handle();
            shard.conn = std::make_unique<ConnectionContext>(std::move(sock));
            shard.active = true;

            loop_.on_read(fd, [this, i, fd](int) {   
                this->on_shard_data(fd, i);
            });

            std::cout << "[Router] Connected to Shard " << i << " at " << shard.ip << ":" << shard.port << "\n" << std::flush;
        }
    }

    void RouterService::on_accept(int listener_fd) {
        auto res = server_socket_.accept();
        if (!res) return;

        Socket client_sock = std::move(res.value());
        client_sock.set_non_blocking();
        int fd = client_sock.native_handle();

        clients_[fd] = std::make_unique<ConnectionContext>(std::move(client_sock));

        loop_.on_read(fd, [this](int fd) {
            this->on_client_data(fd);
        });
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
                break; 
            }

            std::span<const Byte> frame_span(buffer.data() + offset, total_frame_len);
            handler(frame_span);

            offset += total_frame_len;
        }
        return offset;
    }

    void RouterService::flush_output(ConnectionContext& ctx, int fd) {
        if (ctx.out_buffer.empty()) return;

        auto res = ctx.socket.write(ctx.out_buffer);
        
        if (!res) {
            if (res.error() != EWOULDBLOCK && res.error() != EAGAIN) {
                 // Fatal error, cleanup
                 if (clients_.count(fd)) {
                     loop_.remove(fd);
                     clients_.erase(fd);
                     return;
                 }
                 for (auto& shard : shards_) {
                     if (shard.conn && shard.conn->socket.native_handle() == fd) {
                         loop_.remove(fd);
                         shard.active = false;
                         shard.conn.reset();
                         return;
                     }
                 }
            }
            return;
        }
        size_t written = res.value();
        if (written >= ctx.out_buffer.size()) {
            ctx.out_buffer.clear();
            loop_.on_write(fd, nullptr); 
        } else {
            ctx.out_buffer.erase(ctx.out_buffer.begin(), ctx.out_buffer.begin() + written);
        }
    }

    void RouterService::on_client_data(int fd) {
        auto it = clients_.find(fd);
        if (it == clients_.end()) return;

        auto& ctx = it->second;
        
        Byte temp[4096];
        auto read_res = ctx->socket.read(temp);

        if (!read_res || read_res.value() == 0) {
            clients_.erase(fd);
            loop_.remove(fd);
            return;
        }

        size_t n = read_res.value();
        ctx->buffer.insert(ctx->buffer.end(), temp, temp + n);

        size_t consumed = process_buffer(ctx->buffer, [this, fd](std::span<const Byte> frame) {
            this->process_client_frame(fd, frame.subspan(LENGTH_PREFIX_SIZE));
        });

        if (consumed > 0) {
            ctx->buffer.erase(ctx->buffer.begin(), ctx->buffer.begin() + consumed);
        }
    }

    void RouterService::process_client_frame(int client_fd, std::span<const Byte> body) {
        auto frame_res = decode_frame_body(body);
        if (!frame_res) {
            clients_.erase(client_fd);
            loop_.remove(client_fd);
            return;
        }

        const auto& frame = frame_res.value();

        // 1. Generate Global Unique ID
        RequestId global_id = next_global_req_id_++;

        // 2. Store Mapping (Fixes ID Collision)
        // Note: frame.req_id is the ORIGINAL ID from the client
        in_flight_[global_id] = { client_fd, frame.req_id, std::chrono::steady_clock::now() };

        // 3. Patch Frame with Global ID
        MessageFrame fwd_frame = frame; 
        fwd_frame.req_id = global_id;

        auto raw_fwd = encode_frame(fwd_frame);

        // 4. Routing Logic
        uint32_t hash = Hasher::hash(frame.key);
        size_t shard_idx = hash % shards_.size();

        // Pass global_id implicitly in raw_fwd, pass original_req_id for error handling
        forward_to_shard(shard_idx, raw_fwd, frame.req_id, client_fd);
    }

    void RouterService::forward_to_shard(size_t shard_idx, std::span<const Byte> raw_frame, RequestId original_req_id, int client_fd) {
        if (shard_idx >= shards_.size() || !shards_[shard_idx].active) {
            send_error_to_client(client_fd, original_req_id, OpCode::Error); 
            return;
        }

        auto& shard = shards_[shard_idx];
        auto& ctx = *shard.conn;
        int fd = ctx.socket.native_handle();

        // Output Buffering Logic
        if (!ctx.out_buffer.empty()) {
            ctx.out_buffer.insert(ctx.out_buffer.end(), raw_frame.begin(), raw_frame.end());
        } else {
            auto write_res = ctx.socket.write(raw_frame);
            if (!write_res) {
                if (write_res.error() == EAGAIN || write_res.error() == EWOULDBLOCK) {
                    ctx.out_buffer.insert(ctx.out_buffer.end(), raw_frame.begin(), raw_frame.end());
                    loop_.on_write(fd, [this, &ctx, fd](int) { this->flush_output(ctx, fd); });
                } else {
                    shard.active = false;
                    send_error_to_client(client_fd, original_req_id, OpCode::Error);
                }
            } else {
                size_t written = write_res.value();
                if (written < raw_frame.size()) {
                    ctx.out_buffer.insert(ctx.out_buffer.end(), raw_frame.begin() + written, raw_frame.end());
                    loop_.on_write(fd, [this, &ctx, fd](int) { this->flush_output(ctx, fd); });
                }
            }
        }
    }

    void RouterService::on_shard_data(int shard_fd, size_t shard_idx) {
        auto& shard = shards_[shard_idx];
        
        Byte temp[4096];
        auto read_res = shard.conn->socket.read(temp);

        // Handle Shard Disconnect
        if (!read_res || read_res.value() == 0) {
            std::cerr << "[Router] Shard " << shard_idx << " disconnected.\n" << std::flush;
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
            
            // 2. Find Client via Global ID
            auto it = in_flight_.find(frame.req_id);
            if (it != in_flight_.end()) {
                int client_fd = it->second.client_fd;
                
                // --- Restore Original Request ID ---
                RequestId orig_id = it->second.original_req_id;
                
                MessageFrame client_resp = frame;
                client_resp.req_id = orig_id; 
                
                auto raw_resp = encode_frame(client_resp);

                // 3. Forward to Client
                auto client_it = clients_.find(client_fd);
                if (client_it != clients_.end()) {
                    auto& ctx = *client_it->second;
                    
                    if (!ctx.out_buffer.empty()) {
                        ctx.out_buffer.insert(ctx.out_buffer.end(), raw_resp.begin(), raw_resp.end());
                    } else {
                        auto res = ctx.socket.write(raw_resp);
                        
                        if (!res) {
                            if (res.error() == EAGAIN || res.error() == EWOULDBLOCK) {
                                ctx.out_buffer.insert(ctx.out_buffer.end(), raw_resp.begin(), raw_resp.end());
                                loop_.on_write(client_fd, [this, &ctx, client_fd](int) { 
                                    this->flush_output(ctx, client_fd); 
                                });
                            } else {
                                loop_.remove(client_fd);
                                clients_.erase(client_fd);
                            }
                        } else {
                            size_t written = res.value();
                            if (written < raw_resp.size()) {
                                ctx.out_buffer.insert(ctx.out_buffer.end(), raw_resp.begin() + written, raw_resp.end());
                                loop_.on_write(client_fd, [this, &ctx, client_fd](int) { 
                                    this->flush_output(ctx, client_fd); 
                                });
                            }
                        }
                    }
                }                
                in_flight_.erase(it);
            }
        });

        if (consumed > 0) {
            shard.conn->buffer.erase(shard.conn->buffer.begin(), shard.conn->buffer.begin() + consumed);
        }
    }

    void RouterService::send_error_to_client(int client_fd, RequestId req_id, OpCode error_code) {
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) return;

        MessageFrame err_frame {
            .opcode = error_code,
            .req_id = req_id,
            .key = {},
            .value = {}
        };

        auto raw = encode_frame(err_frame);
        auto& ctx = *it->second;

        // Reuse buffering logic
        if (!ctx.out_buffer.empty()) {
            ctx.out_buffer.insert(ctx.out_buffer.end(), raw.begin(), raw.end());
        } else {
             auto res = ctx.socket.write(raw);
             if (!res && (res.error() == EAGAIN || res.error() == EWOULDBLOCK)) {
                 ctx.out_buffer.insert(ctx.out_buffer.end(), raw.begin(), raw.end());
                 loop_.on_write(client_fd, [this, &ctx, client_fd](int) { 
                    this->flush_output(ctx, client_fd); 
                 });
             }
        }
    }

} // namespace kv::router