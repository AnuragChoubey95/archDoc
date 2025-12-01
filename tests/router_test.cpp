#include <gtest/gtest.h>
#include "kv/router.hpp"
#include <thread>
#include <future>
#include <vector>

using namespace kv::router;
using namespace kv::core;
using namespace kv::net;
#include <sys/socket.h> // Required for ::socket, AF_INET, SOCK_STREAM
#include <netinet/in.h> // Required for struct sockaddr_in
#include <arpa/inet.h>  // Required for INADDR_ANY, inet_addr

// Helper to create a listening socket on a random port
struct MockServer {
    Socket sock;
    uint16_t port;
    std::vector<Byte> received_data;

    MockServer() {
        sock = Socket(::socket(AF_INET, SOCK_STREAM, 0));
        sock.set_reuse_addr();
        
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = 0; // Random port

        ::bind(sock.native_handle(), (struct sockaddr*)&addr, sizeof(addr));
        
        socklen_t len = sizeof(addr);
        ::getsockname(sock.native_handle(), (struct sockaddr*)&addr, &len);
        port = ntohs(addr.sin_port);
        
        sock.listen();
    }

    Socket accept_connection() {
        auto res = sock.accept();
        if (!res) throw std::runtime_error("Accept failed");
        return std::move(res.value());
    }
};

class RouterTest : public ::testing::Test {
protected:
    MockServer shard0;
    MockServer shard1;
    std::unique_ptr<RouterService> router;
    std::jthread router_thread;
    const uint16_t ROUTER_PORT = 9090;

    void SetUp() override {
        RouterService::Config config;
        config.port = ROUTER_PORT;
        config.shard_addresses = {
            {"127.0.0.1", shard0.port},
            {"127.0.0.1", shard1.port}
        };

        router = std::make_unique<RouterService>(std::move(config));
        
        // Start router in background thread
        router_thread = std::jthread([this]() {
            router->run();
        });

        // Give router time to start
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        router->stop();
    }
};

TEST_F(RouterTest, ConnectionEstablishment) {
    // Router should attempt to connect to our mock shards
    auto s0_conn = shard0.accept_connection();
    auto s1_conn = shard1.accept_connection();

    EXPECT_TRUE(s0_conn.valid());
    EXPECT_TRUE(s1_conn.valid());
}

TEST_F(RouterTest, RoutingLogic) {
    // Accept shard connections first to stabilize router
    auto s0_conn = shard0.accept_connection();
    auto s1_conn = shard1.accept_connection();

    // Client connects to router
    Socket client(::socket(AF_INET, SOCK_STREAM, 0));
    auto conn_res = client.connect("127.0.0.1", ROUTER_PORT);
    ASSERT_TRUE(conn_res.has_value());

    // 1. Create a Key that maps to Shard 0
    // "key_shard_0" -> Hash ? -> % 2 = 0 (We need to find one)
    // Let's brute force find keys for test stability
    std::string key0 = "a"; 
    while (Hasher::hash(key0) % 2 != 0) key0 += "a";

    std::string key1 = "b";
    while (Hasher::hash(key1) % 2 != 1) key1 += "b";

    // 2. Send Request for Key 0
    MessageFrame req0 {
        .opcode = OpCode::Get,
        .req_id = 100,
        .key = std::as_bytes(std::span{key0}),
        .value = {}
    };
    auto raw_req0 = encode_frame(req0);
    client.write(raw_req0);

    // 3. Check Shard 0 received it
    Byte buffer[1024];
    auto n0 = s0_conn.read(buffer);
    ASSERT_TRUE(n0.has_value());
    EXPECT_GT(n0.value(), 0);
    
    // Verify payload matches
    auto rec_frame_res = decode_frame_body(std::span<Byte>(buffer + 2, n0.value() - 2));
    ASSERT_TRUE(rec_frame_res.has_value());
    EXPECT_EQ(rec_frame_res->req_id, 100);

    // 4. Send Request for Key 1
    MessageFrame req1 {
        .opcode = OpCode::Get,
        .req_id = 101,
        .key = std::as_bytes(std::span{key1}),
        .value = {}
    };
    client.write(encode_frame(req1));

    // 5. Check Shard 1 received it
    auto n1 = s1_conn.read(buffer);
    ASSERT_TRUE(n1.has_value());
    EXPECT_GT(n1.value(), 0);
    
    auto rec_frame1 = decode_frame_body(std::span<Byte>(buffer + 2, n1.value() - 2));
    EXPECT_EQ(rec_frame1->req_id, 101);
}

TEST_F(RouterTest, ResponseForwarding) {
    auto s0_conn = shard0.accept_connection();
    auto s1_conn = shard1.accept_connection();
    
    Socket client(::socket(AF_INET, SOCK_STREAM, 0));
    client.connect("127.0.0.1", ROUTER_PORT);

    // 1. Send Request
    std::string key = "key"; // assumes shard 0 or 1
    size_t shard_idx = Hasher::hash(key) % 2;
    
    MessageFrame req { .opcode=OpCode::Get, .req_id=999, .key=std::as_bytes(std::span{key}), .value={} };
    client.write(encode_frame(req));

    // 2. Mock Shard reads request
    auto& active_shard = (shard_idx == 0) ? s0_conn : s1_conn;
    Byte buf[1024];
    active_shard.read(buf);

    // 3. Mock Shard sends Response
    MessageFrame resp { .opcode=OpCode::OkGet, .req_id=999, .key={}, .value={} };
    active_shard.write(encode_frame(resp));

    // 4. Client should receive response
    auto n = client.read(buf);
    ASSERT_TRUE(n.has_value());
    
    auto res_frame = decode_frame_body(std::span<Byte>(buf + 2, n.value() - 2));
    EXPECT_EQ(res_frame->opcode, OpCode::OkGet);
    EXPECT_EQ(res_frame->req_id, 999);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}