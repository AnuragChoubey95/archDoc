/**
 * @file core.cpp
 * @brief Implementation of the core protocol logic.
 *
 * This file contains the implementation of serialization, deserialization,
 * and data integrity verification (CRC32) for the wire protocol.
 */

#include "kv/core.hpp"
#include <bit>
#include <cstring>
#include <array>
#include <numeric>

namespace kv::core {

    namespace {
        /**
         * @brief Computes the CRC32 checksum of a byte span.
         * * Uses the standard IEEE 802.3 polynomial (0xEDB88320).
         * This function is used to verify the integrity of the message payload.
         *
         * @param data The data to checksum.
         * @return uint32_t The computed CRC32 value.
         */
        uint32_t crc32(std::span<const Byte> data) {
            uint32_t crc = 0xFFFFFFFF;
            for (auto b : data) {
                crc ^= std::to_integer<uint8_t>(b);
                for (int i = 0; i < 8; ++i) {
                    crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
                }
            }
            return ~crc;
        }

        /**
         * @brief Writes an integer to a buffer in Big Endian (Network Byte Order).
         * * Handles the conversion from host byte order to network byte order.
         * If the host is Little Endian, the bytes are swapped.
         *
         * @tparam T The integer type to write (uint16_t, uint32_t, uint64_t).
         * @param buf The destination buffer.
         * @param value The value to write.
         */
        template <typename T>
        void write_be(std::vector<Byte>& buf, T value) {
            constexpr auto size = sizeof(T);
            if constexpr (std::endian::native == std::endian::big) {
                // System is big-endian, just copy
                const Byte* ptr = reinterpret_cast<const Byte*>(&value);
                buf.insert(buf.end(), ptr, ptr + size);
            } else {
                // System is little-endian, swap bytes
                auto val = std::byteswap(value);
                const Byte* ptr = reinterpret_cast<const Byte*>(&val);
                buf.insert(buf.end(), ptr, ptr + size);
            }
        }

        /**
         * @brief Reads an integer from a buffer in Big Endian (Network Byte Order).
         * * Handles the conversion from network byte order to host byte order.
         *
         * @tparam T The integer type to read.
         * @param buf The source buffer.
         * @param offset The byte offset to start reading from.
         * @return T The integer value in host byte order.
         */
        template <typename T>
        T read_be(std::span<const Byte> buf, size_t offset) {
            T value;
            std::memcpy(&value, buf.data() + offset, sizeof(T));
            if constexpr (std::endian::native == std::endian::little) {
                return std::byteswap(value);
            }
            return value;
        }
    }

    /**
     * @details
     * The serialization process follows this layout:
     * 1. Calculates total payload size.
     * 2. Writes 2-byte Length Prefix (Big Endian).
     * 3. Writes 1-byte OpCode.
     * 4. Writes 8-byte Request ID.
     * 5. Writes 2-byte Key Length and 2-byte Value Length.
     * 6. Appends Key and Value bytes.
     * 7. Computes CRC32 over the payload (steps 3-6).
     * 8. Appends 4-byte Checksum.
     */
    std::vector<Byte> encode_frame(const MessageFrame& frame) {
        std::vector<Byte> buffer;
        
        // Calculate sizes
        uint16_t k_len = static_cast<uint16_t>(frame.key.size());
        uint16_t v_len = static_cast<uint16_t>(frame.value.size());
        
        // Payload = OpCode(1) + ReqID(8) + KLen(2) + VLen(2) + Key + Val + Checksum(4)
        uint16_t payload_size = 1 + 8 + 2 + 2 + k_len + v_len + 4;

        // Reserve to avoid reallocations
        buffer.reserve(LENGTH_PREFIX_SIZE + payload_size);

        // 1. Length Prefix (2 bytes)
        write_be(buffer, payload_size);

        // Start of Payload for CRC calculation
        size_t payload_start_idx = buffer.size();

        // 2. OpCode (1 byte)
        buffer.push_back(static_cast<Byte>(frame.opcode));

        // 3. Request ID (8 bytes)
        write_be(buffer, frame.req_id);

        // 4. Key Length (2 bytes)
        write_be(buffer, k_len);

        // 5. Value Length (2 bytes)
        write_be(buffer, v_len);

        // 6. Key Bytes
        buffer.insert(buffer.end(), frame.key.begin(), frame.key.end());

        // 7. Value Bytes
        buffer.insert(buffer.end(), frame.value.begin(), frame.value.end());

        // Calculate Checksum (OpCode ... End of Value)
        std::span<const Byte> payload_view(buffer.data() + payload_start_idx, buffer.size() - payload_start_idx);
        uint32_t checksum = crc32(payload_view);

        // 8. Checksum (4 bytes)
        write_be(buffer, checksum);

        return buffer;
    }

    /**
     * @details
     * Simply extracts the first 2 bytes and treats them as a big-endian uint16_t.
     * Does not validate the rest of the buffer.
     */
    std::expected<size_t, ProtocolError> decode_length_prefix(std::span<const Byte> buffer) {
        if (buffer.size() < LENGTH_PREFIX_SIZE) {
            return std::unexpected(ProtocolError::IncompleteData);
        }
        return read_be<uint16_t>(buffer, 0);
    }

    /**
     * @details
     * Performs strict validation of the frame body:
     * 1. Checks minimum size requirements.
     * 2. Extracts fixed header fields (OpCode, ID, Lengths).
     * 3. Validates that the buffer contains enough data for the specified Key/Value lengths.
     * 4. Creates `std::span` views into the key and value (zero-copy).
     * 5. Computes CRC32 of the received payload and compares it with the footer.
     */
    std::expected<MessageFrame, ProtocolError> decode_frame_body(std::span<const Byte> buffer) {
        // Layout:
        // [0]   Opcode
        // [1-8] ReqId
        // [9-10] KeyLen
        // [11-12] ValLen
        // [13...] Key
        // [... ] Value
        // [Last 4] Checksum

        // Note: The input `buffer` here is the PAYLOAD (excluding the 2-byte length prefix)
        
        constexpr size_t MIN_PAYLOAD = 1 + 8 + 2 + 2 + 4; // 17 bytes
        if (buffer.size() < MIN_PAYLOAD) {
            return std::unexpected(ProtocolError::IncompleteData);
        }

        size_t cursor = 0;

        // 1. Opcode
        OpCode op = static_cast<OpCode>(buffer[cursor++]);

        // 2. Request ID
        RequestId rid = read_be<uint64_t>(buffer, cursor);
        cursor += 8;

        // 3. Key Len
        uint16_t k_len = read_be<uint16_t>(buffer, cursor);
        cursor += 2;

        // 4. Value Len
        uint16_t v_len = read_be<uint16_t>(buffer, cursor);
        cursor += 2;

        // Verify total size matches expected
        // Expected = Header fields + Key + Value + Checksum
        if (buffer.size() != (cursor + k_len + v_len + 4)) {
            // This usually means the frame read from socket didn't match the internal logic
            return std::unexpected(ProtocolError::IncompleteData);
        }

        // 5. Key View
        KeyView k_view = buffer.subspan(cursor, k_len);
        cursor += k_len;

        // 6. Value View
        ValueView v_view = buffer.subspan(cursor, v_len);
        cursor += v_len;

        // 7. Checksum Verification
        uint32_t received_crc = read_be<uint32_t>(buffer, cursor);
        
        // Calculate CRC on everything BEFORE the checksum field
        uint32_t calculated_crc = crc32(buffer.first(cursor));

        if (received_crc != calculated_crc) {
            return std::unexpected(ProtocolError::ChecksumMismatch);
        }

        return MessageFrame{
            .opcode = op,
            .req_id = rid,
            .key = k_view,
            .value = v_view
        };
    }

} // namespace kv::core