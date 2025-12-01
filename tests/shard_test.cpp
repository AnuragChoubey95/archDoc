#include <gtest/gtest.h>
#include "kv/shard.hpp"
#include <thread>
#include <future>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace kv::shard;
using namespace kv::core;
using namespace kv::net;

class ShardTest : public ::testing::Test {
protected:
    std::unique_ptr<ShardService> service;
    std::jthread service_thread;
    const uint16_t SHARD_PORT = 9095;

    void SetUp() override {
        ShardService::Config config;
        config.port = SHARD_PORT;

        service = std::make_unique<ShardService>(std::move(config));
        
        service_thread = std::jthread([this]() {
            service->run();
        });

        // Allow startup time
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        service->stop();
    }

    // Helper to perform a sync request
    MessageFrame send_sync(Socket& client, const MessageFrame& req) {
        auto raw = encode_frame(req);
        client.write(raw);

        Byte buf[4096];
        auto read_res = client.read(buf);
        if (!read_res) throw std::runtime_error("Read failed");

        // Skip prefix (2 bytes)
        std::span<Byte> raw_span(buf, read_res.value());
        auto body = raw_span.subspan(LENGTH_PREFIX_SIZE);
        
        auto res = decode_frame_body(body);
        if (!res) throw std::runtime_error("Decode failed");
        
        return res.value();
    }
};

TEST_F(ShardTest, FullLifecycle) {
    Socket client(::socket(AF_INET, SOCK_STREAM, 0));
    auto conn = client.connect("127.0.0.1", SHARD_PORT);
    ASSERT_TRUE(conn.has_value());

    std::string key = "test_k";
    std::string val = "test_v";
    RequestId rid = 1;

    // 1. PUT
    MessageFrame put_req {
        .opcode = OpCode::Put,
        .req_id = rid++,
        .key = std::as_bytes(std::span{key}),
        .value = std::as_bytes(std::span{val})
    };
    
    auto put_resp = send_sync(client, put_req);
    EXPECT_EQ(put_resp.opcode, OpCode::OkPut);
    EXPECT_EQ(put_resp.req_id, put_req.req_id);

    // 2. GET
    MessageFrame get_req {
        .opcode = OpCode::Get,
        .req_id = rid++,
        .key = std::as_bytes(std::span{key}),
        .value = {}
    };

    auto get_resp = send_sync(client, get_req);
    EXPECT_EQ(get_resp.opcode, OpCode::OkGet);
    std::string fetched_val(reinterpret_cast<const char*>(get_resp.value.data()), get_resp.value.size());
    EXPECT_EQ(fetched_val, val);

    // 3. EXISTS
    MessageFrame ex_req {
        .opcode = OpCode::Exists,
        .req_id = rid++,
        .key = std::as_bytes(std::span{key}),
        .value = {}
    };
    auto ex_resp = send_sync(client, ex_req);
    EXPECT_EQ(ex_resp.opcode, OpCode::OkPut); // Found

    // 4. DELETE
    MessageFrame del_req {
        .opcode = OpCode::Delete,
        .req_id = rid++,
        .key = std::as_bytes(std::span{key}),
        .value = {}
    };
    auto del_resp = send_sync(client, del_req);
    EXPECT_EQ(del_resp.opcode, OpCode::OkPut);

    // 5. GET (Should Fail)
    MessageFrame get_req_2 {
        .opcode = OpCode::Get,
        .req_id = rid++,
        .key = std::as_bytes(std::span{key}),
        .value = {}
    };
    auto get_resp_2 = send_sync(client, get_req_2);
    EXPECT_EQ(get_resp_2.opcode, OpCode::NotFound);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}