#pragma once

/**
 * @file store.hpp
 * @brief Defines the in-memory Storage Engine and custom Memory Allocators.
 */

#include "kv/core.hpp"
#include <vector>
#include <mutex>
#include <atomic>
#include <optional>
#include <expected>

namespace kv::store {

    /**
     * @brief Constants for the Storage Engine.
     */
    constexpr size_t MAX_KEY_SIZE = 256;         ///< Maximum key length in bytes.
    constexpr size_t MAX_VALUE_SIZE = 1024;      ///< Maximum value length in bytes.
    
    // Calculate node size to ensure it fits in a fixed block.
    // Node = Headers + Key + Value.
    // We use a fixed block size (e.g., 2048 bytes) for simplicity in this assignment.
    constexpr size_t BLOCK_SIZE = 2048;

    /**
     * @brief The layout of a Node in the hash table chain.
     * @note This is a POD (Plain Old Data) structure meant to sit inside the custom pool.
     */
    struct Node {
        uint16_t key_len;           ///< Length of the key.
        uint16_t val_len;           ///< Length of the value.
        Node* next;                 ///< Pointer to the next node in the chain (collision resolution).
        
        // Flexible data area. 
        // In reality, we lay out [KeyBytes...][ValueBytes...] immediately after `next`.
        // We use a helper method to access them to avoid undefined behavior warnings.
        std::byte data[]; 

        /**
         * @brief Get a view of the key stored in this node.
         */
        core::KeyView key_view() const {
            return { data, key_len };
        }

        /**
         * @brief Get a view of the value stored in this node.
         */
        core::ValueView value_view() const {
            return { data + key_len, val_len };
        }
    };

    /**
     * @brief A Fixed-Size Block Allocator backed by mmap.
     * * Manages a large region of memory divided into fixed-size blocks (Slots).
     * Uses a free-list to track available slots.
     */
    class PoolAllocator {
    public:
        /**
         * @brief Initialize the pool.
         * @param capacity_blocks Total number of blocks to pre-allocate.
         */
        explicit PoolAllocator(size_t capacity_blocks);
        
        /// @brief Destructor. Unmaps memory.
        ~PoolAllocator();

        // Non-copyable
        PoolAllocator(const PoolAllocator&) = delete;
        PoolAllocator& operator=(const PoolAllocator&) = delete;

        /**
         * @brief Allocates a single block.
         * @return void* Pointer to the block, or nullptr if full.
         */
        void* allocate();

        /**
         * @brief Returns a block to the pool.
         * @param ptr Pointer to the block to free.
         */
        void deallocate(void* ptr);

        /**
         * @brief Get current usage metrics.
         * @return float Load factor (used / capacity).
         */
        float load_factor() const;

    private:
        size_t capacity_;       ///< Total blocks.
        size_t used_ = 0;       ///< Used blocks.
        void* memory_start_;    ///< Pointer to the start of the mmap'd region.
        size_t memory_size_;    ///< Total size in bytes.
        
        struct FreeNode {
            FreeNode* next;
        };
        FreeNode* free_list_ = nullptr; ///< Head of the free list.
    };

    /**
     * @brief The Hash Table (Storage Engine).
     * * Implements a hash map with separate chaining. 
     * Uses the PoolAllocator for node storage.
     */
    class Map {
    public:
        /**
         * @brief Construct a new Map.
         * @param initial_buckets Number of buckets.
         */
        explicit Map(size_t initial_buckets = 1024);

        ~Map() = default;

        /**
         * @brief Retrieve a value.
         * @param key The key to look up.
         * @return core::OperationResult The value bytes or error.
         */
        core::OperationResult get(core::KeyView key);

        /**
         * @brief Insert or Update a value.
         * @param key The key.
         * @param value The value.
         * @return std::optional<core::ProtocolError> std::nullopt on success, error otherwise.
         */
        std::optional<core::ProtocolError> put(core::KeyView key, core::ValueView value);

        /**
         * @brief Delete a key.
         * @param key The key to delete.
         * @return std::optional<core::ProtocolError> std::nullopt on success, error otherwise.
         */
        std::optional<core::ProtocolError> del(core::KeyView key);

    private:
        PoolAllocator pool_;
        std::vector<Node*> buckets_;
        size_t count_ = 0;
        
        // We use a mutex to support the "Optional: Thread Safety" requirement 
        // if the caller decides to run shards multi-threaded later.
        // For the single-threaded mandate, this overhead is negligible (uncontended lock).
        mutable std::mutex mutex_;

        /**
         * @brief Computes bucket index for a key.
         */
        size_t bucket_index(core::KeyView key) const;

        /**
         * @brief Resizes the hash table when load factor > 0.8.
         */
        void rehash_if_needed();
    };

} // namespace kv::store