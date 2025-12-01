#include <gtest/gtest.h>
#include "../include/kv/core.hpp"
#include <string>
#include <vector>
#include <cstring>

using namespace kv::core;

// ---------------------------------------------------------------------------
// Hasher Tests
// ---------------------------------------------------------------------------

TEST(HasherTest, CompileTimeHash) {
    // This MUST happen at compile time for the static_assert to pass
    constexpr uint32_t hash_val = Hasher::hash("user:12345");
    static_assert(hash_val != 0, "Hash should be non-zero");

    // Verify deterministic behavior against a known seed
    constexpr uint32_t hash_val_seeded = Hasher::hash("user:12345", 100);
    static_assert(hash_val != hash_val_seeded, "Seeded hash should differ");
}

TEST(HasherTest, ShardPartitioningLogic) {
    constexpr uint32_t hash_val = Hasher::hash("user:12345");
    constexpr size_t shard_idx = hash_val % 2;
    
    // Ensure the logic is sound in a runtime context as well
    EXPECT_GE(shard_idx, 0);
    EXPECT_LE(shard_idx, 1);
}

// ---------------------------------------------------------------------------
// Protocol Tests
// ---------------------------------------------------------------------------

class ProtocolTest : public ::testing::Test {
protected:
    std::string key_str = "user:12345";
    std::string val_str = "{\"name\":\"Alice\",\"role\":\"admin\"}";
    RequestId req_id = 0xAABBCCDDEEFF0011;

    std::vector<std::byte> encode_test_frame() {
        auto key_bytes = std::as_bytes(std::span{key_str});
        auto val_bytes = std::as_bytes(std::span{val_str});

        MessageFrame req {
            .opcode = OpCode::Put,
            .req_id = req_id,
            .key = key_bytes,
            .value = val_bytes
        };
        return encode_frame(req);
    }
};

TEST_F(ProtocolTest, EncodeFrameHasCorrectSize) {
    auto raw_data = encode_test_frame();
    
    // Expected size calculation
    // Prefix(2) + Op(1) + ID(8) + KLen(2) + VLen(2) + Key(10) + Val(32) + CRC(4)
    size_t expected_size = 2 + 1 + 8 + 2 + 2 + key_str.size() + val_str.size() + 4;
    
    EXPECT_EQ(raw_data.size(), expected_size);
}

TEST_F(ProtocolTest, DecodeLengthPrefix) {
    auto raw_data = encode_test_frame();
    std::span<const std::byte> raw_span(raw_data);

    auto result = decode_length_prefix(raw_span);
    ASSERT_TRUE(result.has_value());
    
    // The prefix stores the size of the PAYLOAD (Total - 2 bytes for prefix)
    EXPECT_EQ(result.value(), raw_data.size() - 2);
}

TEST_F(ProtocolTest, DecodeLengthPrefixFailsOnShortBuffer) {
    std::vector<std::byte> short_buf{std::byte{0x00}}; // Only 1 byte
    auto result = decode_length_prefix(short_buf);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ProtocolError::IncompleteData);
}

TEST_F(ProtocolTest, DecodeFrameBodySuccess) {
    auto raw_data = encode_test_frame();
    std::span<const std::byte> raw_span(raw_data);

    // Skip the 2-byte prefix to get the payload
    auto payload_span = raw_span.subspan(2);

    auto result = decode_frame_body(payload_span);
    ASSERT_TRUE(result.has_value());

    const auto& frame = result.value();
    
    EXPECT_EQ(frame.opcode, OpCode::Put);
    EXPECT_EQ(frame.req_id, req_id);
    
    // Check Key
    std::string decoded_key(reinterpret_cast<const char*>(frame.key.data()), frame.key.size());
    EXPECT_EQ(decoded_key, key_str);

    // Check Value
    std::string decoded_val(reinterpret_cast<const char*>(frame.value.data()), frame.value.size());
    EXPECT_EQ(decoded_val, val_str);
}

TEST_F(ProtocolTest, DecodeFrameFailsOnBadChecksum) {
    auto raw_data = encode_test_frame();
    
    // Corrupt the last byte (part of CRC)
    raw_data.back() = static_cast<std::byte>(static_cast<uint8_t>(raw_data.back()) ^ 0xFF);

    std::span<const std::byte> raw_span(raw_data);
    auto payload_span = raw_span.subspan(2);

    auto result = decode_frame_body(payload_span);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ProtocolError::ChecksumMismatch);
}

TEST_F(ProtocolTest, DecodeFrameFailsOnIncompleteBody) {
    auto raw_data = encode_test_frame();
    
    // Remove the last 4 bytes (Checksum)
    raw_data.resize(raw_data.size() - 4);

    std::span<const std::byte> raw_span(raw_data);
    auto payload_span = raw_span.subspan(2);

    auto result = decode_frame_body(payload_span);
    EXPECT_FALSE(result.has_value());
    // Depending on logic, this might be IncompleteData
    EXPECT_EQ(result.error(), ProtocolError::IncompleteData);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}