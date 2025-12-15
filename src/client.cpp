/**
 * @file client.cpp
 * @brief Implementation of the Client Library.
 */

#include "kv/client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

namespace kv::client {

    using namespace kv::core;
    using namespace kv::net;

    Client::Client(Config config) : config_(std::move(config)) {}

    Client::~Client() {
        disconnect();
    }

    void Client::disconnect() {
        if (sock_) {
            sock_->close();
            sock_.reset();
        }
    }

    std::expected<void, ClientError> Client::connect() {
        disconnect(); // Close existing if any

        // Create socket
        auto raw_sock = ::socket(AF_INET, SOCK_STREAM, 0);
        if (raw_sock < 0) return std::unexpected(ClientError::InternalError);

        sock_ = std::make_unique<Socket>(raw_sock);
        
        // Note: For the Client, we keep the socket BLOCKING for simplicity of the sync API.
        // If we reused the net::Socket set_non_blocking(), we'd need a select() loop here.

        // Connect
        // Since net::Socket::connect assumes non-blocking logic in our implementation,
        // we might need to be careful. Let's use standard blocking connect here for the client
        // or update Socket to handle blocking connects.
        // For this assignment, let's implement a simple blocking connect logic.
        
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(config_.router_port);
        if (inet_pton(AF_INET, config_.router_host.c_str(), &addr.sin_addr) <= 0) {
            return std::unexpected(ClientError::InternalError);
        }

        if (::connect(raw_sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            return std::unexpected(ClientError::NetworkError);
        }

        return {};
    }

    bool Client::ensure_connection() {
        if (sock_ && sock_->valid()) return true;
        
        // Retry loop
        for (int i = 0; i < config_.max_retries; ++i) {
            if (connect()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * (i + 1))); // Backoff
        }
        return false;
    }

    // --- Public API Implementations ---

    // Refactored helper that handles the full transaction and returns the raw body buffer
    // so we can extract data safely.
    std::expected<std::vector<Byte>, ClientError> Client::get(std::string_view key) {
        if (!ensure_connection()) return std::unexpected(ClientError::NetworkError);

        RequestId rid = next_req_id_++;
        MessageFrame req { .opcode = OpCode::Get, .req_id = rid, .key = std::as_bytes(std::span{key}), .value = {} };
        
        auto raw = encode_frame(req);
        if (!sock_->write(raw)) { disconnect(); return std::unexpected(ClientError::NetworkError); }

        // Read Response
        Byte len_buf[2];
        if (sock_->read(len_buf).value_or(0) != 2) { disconnect(); return std::unexpected(ClientError::NetworkError); }
        
        auto len = decode_length_prefix(len_buf);
        if (!len) return std::unexpected(ClientError::ProtocolError);
        
        std::vector<Byte> body(len.value());
        size_t read_so_far = 0;
        while (read_so_far < body.size()) {
            auto n = sock_->read(std::span<Byte>(body.data() + read_so_far, body.size() - read_so_far));
            if (!n || n.value() == 0) { disconnect(); return std::unexpected(ClientError::NetworkError); }
            read_so_far += n.value();
        }

        auto frame_res = decode_frame_body(body);
        if (!frame_res) return std::unexpected(ClientError::ProtocolError);
        
        auto frame = frame_res.value();
        if (frame.req_id != rid) return std::unexpected(ClientError::ProtocolError);

        if (frame.opcode == OpCode::OkGet) {
            std::vector<Byte> val(frame.value.begin(), frame.value.end());
            return val;
        } else if (frame.opcode == OpCode::NotFound) {
            return std::unexpected(ClientError::KeyNotFound);
        } else {
            return std::unexpected(ClientError::ServerBusy);
        }
    }

    std::expected<void, ClientError> Client::put(std::string_view key, std::string_view value) {
        if (!ensure_connection()) return std::unexpected(ClientError::NetworkError);

        RequestId rid = next_req_id_++;
        MessageFrame req { 
            .opcode = OpCode::Put, 
            .req_id = rid, 
            .key = std::as_bytes(std::span{key}), 
            .value = std::as_bytes(std::span{value}) 
        };
        
        auto raw = encode_frame(req);
        if (!sock_->write(raw)) { disconnect(); return std::unexpected(ClientError::NetworkError); }

        // Read Response
        Byte len_buf[2];
        if (sock_->read(len_buf).value_or(0) != 2) { disconnect(); return std::unexpected(ClientError::NetworkError); }
        auto len = decode_length_prefix(len_buf);
        
        std::vector<Byte> body(len.value());
        size_t read_so_far = 0;
        while (read_so_far < body.size()) {
            auto n = sock_->read(std::span<Byte>(body.data() + read_so_far, body.size() - read_so_far));
            if (!n || n.value() == 0) { disconnect(); return std::unexpected(ClientError::NetworkError); }
            read_so_far += n.value();
        }

        auto frame_res = decode_frame_body(body);
        auto frame = frame_res.value();

        if (frame.opcode == OpCode::OkPut) return {};
        return std::unexpected(ClientError::InternalError);
    }

    std::expected<void, ClientError> Client::del(std::string_view key) {
        if (!ensure_connection()) return std::unexpected(ClientError::NetworkError);

        RequestId rid = next_req_id_++;
        MessageFrame req { .opcode = OpCode::Delete, .req_id = rid, .key = std::as_bytes(std::span{key}), .value = {} };
        
        auto raw = encode_frame(req);
        if (!sock_->write(raw)) { disconnect(); return std::unexpected(ClientError::NetworkError); }

        Byte len_buf[2];
        if (sock_->read(len_buf).value_or(0) != 2) { disconnect(); return std::unexpected(ClientError::NetworkError); }
        auto len = decode_length_prefix(len_buf);
        
        std::vector<Byte> body(len.value());
        size_t read_so_far = 0;
        while (read_so_far < body.size()) {
            auto n = sock_->read(std::span<Byte>(body.data() + read_so_far, body.size() - read_so_far));
            if (!n || n.value() == 0) { disconnect(); return std::unexpected(ClientError::NetworkError); }
            read_so_far += n.value();
        }

        auto frame = decode_frame_body(body).value();
        if (frame.opcode == OpCode::OkPut) return {}; // Delete uses OkPut equivalent
        if (frame.opcode == OpCode::NotFound) return std::unexpected(ClientError::KeyNotFound);
        return std::unexpected(ClientError::InternalError);
    }

    std::expected<bool, ClientError> Client::exists(std::string_view key) {
        if (!ensure_connection()) return std::unexpected(ClientError::NetworkError);

        RequestId rid = next_req_id_++;
        MessageFrame req { .opcode = OpCode::Exists, .req_id = rid, .key = std::as_bytes(std::span{key}), .value = {} };
        
        auto raw = encode_frame(req);
        if (!sock_->write(raw)) { disconnect(); return std::unexpected(ClientError::NetworkError); }

        Byte len_buf[2];
        if (sock_->read(len_buf).value_or(0) != 2) { disconnect(); return std::unexpected(ClientError::NetworkError); }
        auto len = decode_length_prefix(len_buf);
        
        std::vector<Byte> body(len.value());
        size_t read_so_far = 0;
        while (read_so_far < body.size()) {
            auto n = sock_->read(std::span<Byte>(body.data() + read_so_far, body.size() - read_so_far));
            if (!n || n.value() == 0) { disconnect(); return std::unexpected(ClientError::NetworkError); }
            read_so_far += n.value();
        }

        auto frame = decode_frame_body(body).value();
        if (frame.opcode == OpCode::OkPut) return true;
        if (frame.opcode == OpCode::NotFound) return false;
        return std::unexpected(ClientError::InternalError);
    }

} // namespace kv::client