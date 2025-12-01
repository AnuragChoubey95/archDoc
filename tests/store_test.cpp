#include <gtest/gtest.h>
#include "kv/store.hpp"
#include <string>
#include <vector>
#include <random>
#include <algorithm>
#include <set>

using namespace kv::store;
using namespace kv::core;

class StoreTest : public ::testing::Test {
protected:
    // Start with small buckets (16) to force collisions and rehashing logic to run frequently
    Map map{16}; 
};

// ---------------------------------------------------------------------------
// Basic Functionality (Regression Tests)
// ---------------------------------------------------------------------------

TEST_F(StoreTest, PutAndGet) {
    std::string key = "user:1";
    std::string val = "Alice";

    auto err = map.put(
        std::as_bytes(std::span{key}), 
        std::as_bytes(std::span{val})
    );
    EXPECT_FALSE(err.has_value());

    auto res = map.get(std::as_bytes(std::span{key}));
    ASSERT_TRUE(res.has_value());
    
    std::string fetched_val(reinterpret_cast<const char*>(res->data()), res->size());
    EXPECT_EQ(fetched_val, val);
}

TEST_F(StoreTest, UpdateExistingKey) {
    std::string key = "config";
    std::string v1 = "on";
    std::string v2 = "off";

    map.put(std::as_bytes(std::span{key}), std::as_bytes(std::span{v1}));
    
    // Update
    map.put(std::as_bytes(std::span{key}), std::as_bytes(std::span{v2}));

    auto res = map.get(std::as_bytes(std::span{key}));
    std::string fetched_val(reinterpret_cast<const char*>(res->data()), res->size());
    EXPECT_EQ(fetched_val, v2);
}

TEST_F(StoreTest, DeleteKey) {
    std::string key = "temp";
    std::string val = "data";

    map.put(std::as_bytes(std::span{key}), std::as_bytes(std::span{val}));
    
    auto err = map.del(std::as_bytes(std::span{key}));
    EXPECT_FALSE(err.has_value());

    auto res = map.get(std::as_bytes(std::span{key}));
    EXPECT_FALSE(res.has_value());
}

// ---------------------------------------------------------------------------
// Aggressive & Edge Case Tests
// ---------------------------------------------------------------------------

TEST_F(StoreTest, BinaryKeySafety) {
    // Keys in this system are just bytes. They can contain nulls, newlines, etc.
    std::vector<std::byte> bin_key = { 
        std::byte{0x00}, std::byte{0xFF}, std::byte{0x10}, std::byte{0x00} 
    };
    std::vector<std::byte> bin_val = { 
        std::byte{0xCA}, std::byte{0xFE}, std::byte{0xBA}, std::byte{0xBE} 
    };

    auto err = map.put(bin_key, bin_val);
    ASSERT_FALSE(err.has_value());

    auto res = map.get(bin_key);
    ASSERT_TRUE(res.has_value());
    
    // Verify exact byte match
    EXPECT_EQ(res->size(), bin_val.size());
    EXPECT_TRUE(std::equal(res->begin(), res->end(), bin_val.begin()));
}

TEST_F(StoreTest, MassiveInsertionAndRehashing) {
    // Insert enough items to force multiple table resizes.
    // Initial buckets = 16. Load factor 0.8.
    // The map should grow: 16 -> 32 -> 64 ... -> ~4096 buckets.
    
    constexpr int COUNT = 2000;
    for(int i = 0; i < COUNT; ++i) {
        std::string k = "load_key_" + std::to_string(i);
        std::string v = "load_val_" + std::to_string(i);
        
        auto err = map.put(std::as_bytes(std::span{k}), std::as_bytes(std::span{v}));
        ASSERT_FALSE(err.has_value()) << "Failed to put at index " << i;
    }

    // Verify ALL items are still present and correct after rehashing
    for(int i = 0; i < COUNT; ++i) {
        std::string k = "load_key_" + std::to_string(i);
        std::string v = "load_val_" + std::to_string(i);
        
        auto res = map.get(std::as_bytes(std::span{k}));
        ASSERT_TRUE(res.has_value()) << "Missing key at index " << i << " (Rehashing lost data?)";
        
        std::string fetched(reinterpret_cast<const char*>(res->data()), res->size());
        EXPECT_EQ(fetched, v);
    }
}

TEST_F(StoreTest, MaxPayloadExceeded) {
    // BLOCK_SIZE is 2048 bytes in our mocked allocator.
    // Overhead per Node is sizeof(Node) (~16-24 bytes).
    // Available payload space ~ 2020 bytes.
    
    std::string key = "test_key";
    
    // Case 1: Value fits (1023 bytes)
    std::vector<std::byte> large_val(1023, std::byte{'A'});
    auto err = map.put(std::as_bytes(std::span{key}), large_val);
    if (err.has_value()) {
        // CAST the error to int so you can read it
        std::cout << "DEBUG: Put failed with error code: " 
                  << static_cast<int>(err.value()) << "\n";
    }
    EXPECT_FALSE(err.has_value());
    
    // Case 2: Value definitely fails (3000 bytes)
    std::vector<std::byte> huge_val(3000, std::byte{'B'});
    auto err_huge = map.put(std::as_bytes(std::span{key}), huge_val);
    
    ASSERT_TRUE(err_huge.has_value());
    EXPECT_EQ(err_huge.value(), ProtocolError::MessageTooLarge);
}

TEST_F(StoreTest, InterleavedOperationsFuzz) {
    // Randomized sequence of Put, Get, Delete to find state inconsistencies
    std::mt19937 rng(42); // Deterministic seed
    std::uniform_int_distribution<int> key_dist(0, 49); // Operate on 50 distinct keys
    std::uniform_int_distribution<int> op_dist(0, 9);   // 10 possible op codes (probabilistic)

    // Shadow map to verify correctness
    std::vector<std::string> shadow_state(50, ""); 

    for(int i=0; i<10000; ++i) {
        int key_id = key_dist(rng);
        std::string key = "rk_" + std::to_string(key_id);
        std::string val = "val_" + std::to_string(i); // Unique value per op

        int op = op_dist(rng);

        if (op < 5) { 
            // 50% chance PUT
            auto err = map.put(std::as_bytes(std::span{key}), std::as_bytes(std::span{val}));
            if (!err) shadow_state[key_id] = val;
        } 
        else if (op < 8) { 
            // 30% chance GET
            auto res = map.get(std::as_bytes(std::span{key}));
            if (shadow_state[key_id].empty()) {
                ASSERT_FALSE(res.has_value()) << "Found key " << key << " but it should be deleted";
            } else {
                ASSERT_TRUE(res.has_value()) << "Could not find key " << key;
                std::string s(reinterpret_cast<const char*>(res->data()), res->size());
                ASSERT_EQ(s, shadow_state[key_id]);
            }
        } 
        else { 
            // 20% chance DELETE
            map.del(std::as_bytes(std::span{key}));
            shadow_state[key_id] = "";
        }
    }
}

// ---------------------------------------------------------------------------
// Allocator Tests
// ---------------------------------------------------------------------------

TEST_F(StoreTest, AllocatorCapacityLimit) {
    // Create a pool with exactly 2 blocks
    PoolAllocator small_pool(2);
    
    void* p1 = small_pool.allocate();
    EXPECT_NE(p1, nullptr);
    
    void* p2 = small_pool.allocate();
    EXPECT_NE(p2, nullptr);
    
    // Should be full now
    void* p3 = small_pool.allocate();
    EXPECT_EQ(p3, nullptr); 
    
    // Free one
    small_pool.deallocate(p1);
    
    // Should be able to allocate again
    void* p4 = small_pool.allocate(); 
    EXPECT_NE(p4, nullptr);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}