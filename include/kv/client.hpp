#pragma once

/**
 * @file client.hpp
 * @brief Defines the Client Library for interacting with the Distributed KV Store.
 */

#include "kv/core.hpp"
#include "kv/net.hpp"
#include <string>
#include <vector>
#include <optional>
#include <expected>

namespace kv::client {

    /**
     * @brief Error codes specific to the client library.
     */
    enum class ClientError {
        NetworkError,
        ProtocolError,
        ServerBusy,
        KeyNotFound,
        InternalError
    };

    /**
     * @brief A synchronous client for the KV Store.
     * * This class is not thread-safe. Each thread should have its own Client instance.
     */
    class Client {
    public:
        struct Config {
            std::string router_host = "127.0.0.1";
            uint16_t router_port = 9090;
            int max_retries = 3;
            int timeout_ms = 2000;
        };

        explicit Client(Config config);
        ~Client();

        /**
         * @brief Connects to the router.
         * * Automatically called by operations if not connected.
         * @return std::expected<void, ClientError>
         */
        std::expected<void, ClientError> connect();

        /**
         * @brief Put a key-value pair.
         * @param key The key (string or bytes).
         * @param value The value (string or bytes).
         * @return std::expected<void, ClientError>
         */
        std::expected<void, ClientError> put(std::string_view key, std::string_view value);

        /**
         * @brief Get a value by key.
         * @param key The key to retrieve.
         * @return std::expected<std::vector<core::Byte>, ClientError> The value bytes, or error.
         */
        std::expected<std::vector<core::Byte>, ClientError> get(std::string_view key);

        /**
         * @brief Delete a key.
         * @param key The key to delete.
         * @return std::expected<void, ClientError>
         */
        std::expected<void, ClientError> del(std::string_view key);

        /**
         * @brief Check if a key exists.
         * @param key The key to check.
         * @return std::expected<bool, ClientError> True if exists, False if not.
         */
        std::expected<bool, ClientError> exists(std::string_view key);

    private:
        Config config_;
        std::unique_ptr<net::Socket> sock_;
        uint64_t next_req_id_ = 1;

        // Internal helpers
        std::expected<core::MessageFrame, ClientError> send_and_receive(core::OpCode op, std::string_view key, std::string_view value = {});
        
        bool ensure_connection();
        void disconnect();
    };

} // namespace kv::client