#pragma once

/**
 * @file core.hpp
 * @brief Defines the core vocabulary, types, and protocol for the Key-Value Store.
 * * This file contains the strong types, the wire format structure, compile-time hashing logic,
 * and serialization/deserialization interface used by both the Router and Shards.
 */

#include <cstddef>
#include <cstdint>
#include <vector>
#include <span>
#include <string_view>
#include <expected>
#include <array>
#include <concepts>

namespace kv::core {

    // -----------------------------------------------------------------------
    // Strong Types & Concepts
    // -----------------------------------------------------------------------

    /// @brief Type alias for std::byte to represent raw memory.
    using Byte = std::byte;

    /// @brief Strong type for Request IDs (64-bit unsigned).
    using RequestId = uint64_t;

    /// @brief Non-owning view of a key (byte span).
    using KeyView = std::span<const Byte>;

    /// @brief Non-owning view of a value (byte span).
    using ValueView = std::span<const Byte>;

    /// @brief Error codes for protocol parsing and validation.
    enum class ProtocolError {
        /// @brief The buffer does not contain enough data to form a full message or header.
        IncompleteData,
        /// @brief The message format violates the protocol specification.
        InvalidMagic,
        /// @brief The CRC32 checksum of the body does not match the header.
        ChecksumMismatch,
        /// @brief The message exceeds the maximum allowed size.
        MessageTooLarge
    };

    /// @brief Result type for storage operations. Returns an owning vector of bytes on success, or an error.
    using OperationResult = std::expected<std::vector<Byte>, ProtocolError>;

    /**
     * @brief Operation Codes for the Wire Protocol.
     * * Mapped directly to the design document specification.
     */
    enum class OpCode : uint8_t {
        Get      = 0x00, ///< Retrieve a value by key.
        Put      = 0x01, ///< Store or update a value by key.
        Delete   = 0x02, ///< Remove a value by key.
        Exists   = 0x03, ///< Check if a key exists.
        
        // Responses
        OkGet    = 0x64, ///< (100) Operation successful, value returned.
        OkPut    = 0x65, ///< (101) Operation successful, no value returned.
        NotFound = 0xC8, ///< (200) Key not found.
        Error    = 0xC9  /// (201) Generic error or bad request.
    };

    // -----------------------------------------------------------------------
    // Wire Format Structure
    // -----------------------------------------------------------------------

    /**
     * @brief Represents a parsed protocol message.
     * * This is a "View" type. It does not own the memory for the key or value.
     * It points into the buffer used during deserialization.
     */
    struct MessageFrame {
        OpCode opcode;   ///< The operation command.
        RequestId req_id;///< Unique ID for request/response correlation.
        KeyView key;     ///< View into the key bytes.
        ValueView value; ///< View into the value bytes.
    };

    // -----------------------------------------------------------------------
    // Hashing (Constexpr Murmur3)
    // -----------------------------------------------------------------------

    /**
     * @brief Compile-time hashing utilities.
     * * Implements Murmur3 (32-bit) in a constexpr context to ensure consistency
     * between the stateless router and the shards.
     */
    struct Hasher {
        /**
         * @brief Computes the Murmur3 32-bit hash of a byte span.
         * * @tparam T The underlying type of the span (must be 1 byte, e.g., char, uint8_t, std::byte).
         * @param key The data to hash.
         * @param seed Optional seed value (default 0).
         * @return uint32_t The computed hash.
         */
        template <typename T>
        requires (sizeof(T) == 1)
        static constexpr uint32_t hash(std::span<const T> key, uint32_t seed = 0) {
            auto data = key.data();
            size_t len = key.size();
            uint32_t h1 = seed;
            const uint32_t c1 = 0xcc9e2d51;
            const uint32_t c2 = 0x1b873593;

            const size_t nblocks = len / 4;
            for (size_t i = 0; i < nblocks; i++) {
                uint32_t k1 = 0;
                // Manual byte assembly to be endian-safe and constexpr friendly.
                // We cast to uint8_t first to avoid sign extension issues if T is signed char.
                k1 |= static_cast<uint32_t>(static_cast<uint8_t>(data[i * 4 + 0])) << 0;
                k1 |= static_cast<uint32_t>(static_cast<uint8_t>(data[i * 4 + 1])) << 8;
                k1 |= static_cast<uint32_t>(static_cast<uint8_t>(data[i * 4 + 2])) << 16;
                k1 |= static_cast<uint32_t>(static_cast<uint8_t>(data[i * 4 + 3])) << 24;

                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= c2;

                h1 ^= k1;
                h1 = (h1 << 13) | (h1 >> 19);
                h1 = h1 * 5 + 0xe6546b64;
            }

            // Tail processing
            const T* tail = data + nblocks * 4;
            uint32_t k1 = 0;
            switch (len & 3) {
                case 3: k1 ^= static_cast<uint32_t>(static_cast<uint8_t>(tail[2])) << 16; [[fallthrough]];
                case 2: k1 ^= static_cast<uint32_t>(static_cast<uint8_t>(tail[1])) << 8;  [[fallthrough]];
                case 1: k1 ^= static_cast<uint32_t>(static_cast<uint8_t>(tail[0]));
                        k1 *= c1; k1 = (k1 << 15) | (k1 >> 17); k1 *= c2; h1 ^= k1;
            }

            // Finalization
            h1 ^= len;
            h1 ^= (h1 >> 16);
            h1 *= 0x85ebca6b;
            h1 ^= (h1 >> 13);
            h1 *= 0xc2b2ae35;
            h1 ^= (h1 >> 16);

            return h1;
        }

        /**
         * @brief Helper to hash a std::string_view or string literal.
         * * @param key The string to hash.
         * @param seed Optional seed value.
         * @return uint32_t The computed hash.
         */
        static constexpr uint32_t hash(std::string_view key, uint32_t seed = 0) {
            return hash(std::span<const char>(key.data(), key.size()), seed);
        }
    };

    // -----------------------------------------------------------------------
    // Serialization / Deserialization
    // -----------------------------------------------------------------------

    /// @brief Size of the fixed header in bytes (excluding length prefix).
    constexpr size_t HEADER_SIZE = 17; // OpCode(1) + ReqId(8) + KeyLen(2) + ValLen(2) + Checksum(4)
    
    /// @brief Size of the length prefix in bytes.
    constexpr size_t LENGTH_PREFIX_SIZE = 2;

    /**
     * @brief Serializes a message frame into a flat byte vector.
     * * @param frame The populated message frame to encode.
     * @return std::vector<Byte> A buffer containing the length prefix followed by the payload.
     * @note Handles Host-to-Network byte order conversion (Big Endian).
     */
    [[nodiscard]] std::vector<Byte> encode_frame(const MessageFrame& frame);

    /**
     * @brief Decodes the 2-byte length prefix from the start of a buffer.
     * * @param buffer The buffer to read from. Must be at least 2 bytes.
     * @return std::expected<size_t, ProtocolError> The length of the payload, or an error.
     */
    [[nodiscard]] std::expected<size_t, ProtocolError> decode_length_prefix(std::span<const Byte> buffer);

    /**
     * @brief Parses a message frame from the payload buffer.
     * * @param buffer The payload buffer (excluding the 2-byte length prefix).
     * @return std::expected<MessageFrame, ProtocolError> A view into the buffer representing the message.
     * @note Performs CRC32 validation. If the checksum fails, returns ProtocolError::ChecksumMismatch.
     */
    [[nodiscard]] std::expected<MessageFrame, ProtocolError> decode_frame_body(std::span<const Byte> buffer);

} // namespace kv::core