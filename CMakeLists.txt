cmake_minimum_required(VERSION 3.20)
project(DistributedKVStore LANGUAGES CXX)

# -----------------------------------------------------------------------------
# Compiler Settings (C++23)
# -----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Enable strict warnings to catch issues early
if(MSVC)
    add_compile_options(/W4)
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
endif()

# -----------------------------------------------------------------------------
# Dependencies
# -----------------------------------------------------------------------------
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")

# Find Threads (Required for POSIX threads on Linux/Mac)
find_package(Threads REQUIRED)

# Find GoogleTest
# If you installed via Homebrew, ensure CMAKE_PREFIX_PATH includes /opt/homebrew
find_package(GTest REQUIRED)

# Include the local header directory globally
include_directories(include)

# -----------------------------------------------------------------------------
# Library Targets (Modular Architecture)
# -----------------------------------------------------------------------------

# 1. Core Library (Types, Protocol, Hashing)
add_library(kv_core src/core.cpp)

# 2. Networking Library (Reactor, Sockets)
# Links PUBLIC to kv_core because net.hpp includes core.hpp
add_library(kv_net src/net/socket.cpp src/net/reactor.cpp)
target_link_libraries(kv_net PUBLIC kv_core)

# 3. Storage Library (Hash Map, Allocators)
# Links PUBLIC to kv_core because store.hpp includes core.hpp
add_library(kv_store src/store/engine.cpp)
target_link_libraries(kv_store PUBLIC kv_core)

# 4. Router Service Library (Business Logic)
add_library(kv_router src/router/service.cpp)
target_link_libraries(kv_router PUBLIC kv_net kv_core)

# 5. Shard Service Library (Business Logic)
add_library(kv_shard src/shard/service.cpp)
target_link_libraries(kv_shard PUBLIC kv_net kv_store kv_core)

# 6. Client Library
add_library(kv_client src/client.cpp)
target_link_libraries(kv_client PUBLIC kv_net kv_core)

# -----------------------------------------------------------------------------
# Main Executables
# -----------------------------------------------------------------------------

# Router Binary
add_executable(router_service src/bin/router_main.cpp)
target_link_libraries(router_service PRIVATE kv_router Threads::Threads)

# Shard Binary
add_executable(shard_service src/bin/shard_main.cpp)
target_link_libraries(shard_service PRIVATE kv_shard Threads::Threads)

add_executable(demo_app src/bin/demo_client_main.cpp)
target_link_libraries(demo_app PRIVATE kv_client Threads::Threads)

add_executable(load_test tests/load_test.cpp)
target_link_libraries(load_test PRIVATE kv_client Threads::Threads)

# -----------------------------------------------------------------------------
# Testing
# -----------------------------------------------------------------------------
enable_testing()

# Helper macro to define a test executable
macro(add_kv_test test_name source_file linked_libs)
    add_executable(${test_name} ${source_file})
    # Link GTest::gtest (not main, because our tests define their own main)
    target_link_libraries(${test_name} PRIVATE 
        ${linked_libs} 
        GTest::gtest 
        Threads::Threads
    )
    # Register with CTest
    add_test(NAME ${test_name} COMMAND ${test_name})
endmacro()

# Define Individual Test Suites
add_kv_test(core_test   tests/core_test.cpp   kv_core)
add_kv_test(net_test    tests/net_test.cpp    kv_net)
add_kv_test(store_test  tests/store_test.cpp  kv_store)
add_kv_test(router_test tests/router_test.cpp kv_router)
add_kv_test(shard_test  tests/shard_test.cpp  kv_shard)

