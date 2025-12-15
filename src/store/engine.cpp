/**
 * @file engine.cpp
 * @brief Implementation of PoolAllocator (via mmap) and Map logic.
 */

#include "kv/store.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <cstring>
#include <iostream>
#include <algorithm>

namespace kv::store {

    // -----------------------------------------------------------------------
    // PoolAllocator Implementation
    // -----------------------------------------------------------------------

    PoolAllocator::PoolAllocator(size_t capacity_blocks) 
        : capacity_(capacity_blocks) 
    {
        memory_size_ = capacity_ * BLOCK_SIZE;

        // "map": Ask OS for a raw chunk of memory (Anonymous, Private)
        memory_start_ = ::mmap(nullptr, memory_size_, 
                               PROT_READ | PROT_WRITE, 
                               MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

        if (memory_start_ == MAP_FAILED) {
            throw std::bad_alloc();
        }

        // Initialize Free List
        // We iterate through the raw memory and link each block to the next.
        auto* byte_ptr = static_cast<std::byte*>(memory_start_);
        
        for (size_t i = 0; i < capacity_ - 1; ++i) {
            auto* current = reinterpret_cast<FreeNode*>(byte_ptr + (i * BLOCK_SIZE));
            auto* next    = reinterpret_cast<FreeNode*>(byte_ptr + ((i + 1) * BLOCK_SIZE));
            current->next = next;
        }

        // Last node points to null
        auto* last = reinterpret_cast<FreeNode*>(byte_ptr + ((capacity_ - 1) * BLOCK_SIZE));
        last->next = nullptr;

        // Head points to first block
        free_list_ = reinterpret_cast<FreeNode*>(memory_start_);
    }

    PoolAllocator::~PoolAllocator() {
        if (memory_start_ && memory_start_ != MAP_FAILED) {
            // "unmap": Return memory to OS
            ::munmap(memory_start_, memory_size_);
        }
    }

    void* PoolAllocator::allocate() {
        if (!free_list_) {
            return nullptr; // OOM
        }

        // Pop from head
        void* block = free_list_;
        free_list_ = free_list_->next;
        used_++;
        return block;
    }

    void PoolAllocator::deallocate(void* ptr) {
        if (!ptr) return;

        // Push to head
        auto* node = static_cast<FreeNode*>(ptr);
        node->next = free_list_;
        free_list_ = node;
        used_--;
    }

    float PoolAllocator::load_factor() const {
        return static_cast<float>(used_) / static_cast<float>(capacity_);
    }

    // -----------------------------------------------------------------------
    // Map Implementation
    // -----------------------------------------------------------------------

    Map::Map(size_t initial_buckets) 
        : pool_(100000) // Pre-allocate 100k slots (~200MB) for this assignment
        , buckets_(initial_buckets, nullptr) 
    {
    }

    size_t Map::bucket_index(core::KeyView key) const {
        // Use the core hasher, but modulo the *current* bucket size
        return core::Hasher::hash(key) % buckets_.size();
    }

    core::OperationResult Map::get(core::KeyView key) {
        std::lock_guard<std::mutex> lock(mutex_);
        
        size_t idx = bucket_index(key);
        Node* curr = buckets_[idx];

        while (curr) {
            core::KeyView stored_key = curr->key_view();
            // std::span equality check
            if (std::ranges::equal(stored_key, key)) {
                // Found! Copy to vector for safe return
                core::ValueView val = curr->value_view();
                std::vector<std::byte> result(val.begin(), val.end());
                return result;
            }
            curr = curr->next;
        }

        return std::unexpected(core::ProtocolError::InvalidMagic); // Map "Not Found" roughly to this or custom error
    }

    std::optional<core::ProtocolError> Map::put(core::KeyView key, core::ValueView value) {

        // 1. Validation
        if (key.size() > MAX_KEY_SIZE || value.size() > MAX_VALUE_SIZE) {
            return core::ProtocolError::MessageTooLarge;
        }

        size_t idx = bucket_index(key);
        Node* curr = buckets_[idx];
        Node* prev = nullptr;

        // 2. Check for update
        while (curr) {
            if (std::ranges::equal(curr->key_view(), key)) {
                // Key exists. 
                // In a simple fixed-block allocator, we can overwrite IF it fits.
                // But since we allocate fixed blocks (BLOCK_SIZE), it ALWAYS fits.
                // Update lengths and copy data.
                
                // IMPORTANT: In a real "Node" with flexible array, we'd need to ensure 
                // the new value doesn't overflow the *allocated* space if we had variable blocks.
                // Since we use fixed BLOCK_SIZE (2KB), we just verify total size.
                if (sizeof(Node) + key.size() + value.size() > BLOCK_SIZE) {
                    return core::ProtocolError::MessageTooLarge;
                }

                curr->val_len = static_cast<uint16_t>(value.size());
                // Key is same, skip copy. Value copy:
                std::memcpy(curr->data + curr->key_len, value.data(), value.size());
                return std::nullopt;
            }
            curr = curr->next;
        }

        // 3. Insert New
        void* block = pool_.allocate();
        if (!block) {
            return core::ProtocolError::InvalidMagic; // internal error/oom
        }

        Node* node = new (block) Node(); // Placement new (construct header)
        node->key_len = static_cast<uint16_t>(key.size());
        node->val_len = static_cast<uint16_t>(value.size());
        
        // Copy Key
        std::memcpy(node->data, key.data(), key.size());
        // Copy Value
        std::memcpy(node->data + key.size(), value.data(), value.size());

        // Link at Head (O(1))
        node->next = buckets_[idx];
        buckets_[idx] = node;
        count_++;

        rehash_if_needed();

        return std::nullopt;
    }

    std::optional<core::ProtocolError> Map::del(core::KeyView key) {
        
        size_t idx = bucket_index(key);
        Node* curr = buckets_[idx];
        Node* prev = nullptr;

        while (curr) {
            if (std::ranges::equal(curr->key_view(), key)) {
                // Unlink
                if (prev) {
                    prev->next = curr->next;
                } else {
                    buckets_[idx] = curr->next;
                }
                
                // Return to pool
                // Note: No need to call destructor for POD types usually, 
                // but good practice if Node becomes complex.
                pool_.deallocate(curr);
                count_--;
                return std::nullopt;
            }
            prev = curr;
            curr = curr->next;
        }

        return core::ProtocolError::InvalidMagic; // Not Found
    }

    void Map::rehash_if_needed() {
        
        float load = static_cast<float>(count_) / buckets_.size();
        if (load <= 0.8f) return;

        size_t new_size = buckets_.size() * 2;
        std::vector<Node*> new_buckets(new_size, nullptr);

        // Iterate over ALL old buckets
        for (Node* curr : buckets_) {
            while (curr) {
                Node* next = curr->next; // save next

                // Re-calculate hash for new size
                // Note: We need to re-read key from the node data
                core::KeyView k = curr->key_view();
                uint32_t h = core::Hasher::hash(k);
                size_t new_idx = h % new_size;

                // Move node to new bucket list
                curr->next = new_buckets[new_idx];
                new_buckets[new_idx] = curr;

                curr = next;
            }
        }

        buckets_ = std::move(new_buckets);
    }

} // namespace kv::store