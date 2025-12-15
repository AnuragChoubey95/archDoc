#include "kv/client.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <atomic>
#include <chrono>
#include <random>
#include <iomanip>
#include <mutex>

using namespace kv::client;

// Default Configuration
int NUM_CLIENTS = 10;
int OPS_PER_CLIENT = 100;
uint16_t ROUTER_PORT = 9090;

// Statistics
std::atomic<long> g_success{0};
std::atomic<long> g_fail{0};
std::atomic<int> g_connected_clients{0};
std::mutex print_mutex;

void client_task(int id) {
    {
        std::lock_guard<std::mutex> lock(print_mutex);
        std::cout << "[Thread-" << id << "] Started. Connecting...\n" << std::flush;
    }

    Client::Config config;
    config.router_port = ROUTER_PORT;
    config.timeout_ms = 2000;
    config.max_retries = 3; 
    
    Client client(config);

    // Stagger connection slightly
    std::this_thread::sleep_for(std::chrono::milliseconds(id * 10));

    if (!client.connect()) {
        std::lock_guard<std::mutex> lock(print_mutex);
        std::cerr << "[Thread-" << id << "] Connect FAILED.\n" << std::flush;
        g_fail += OPS_PER_CLIENT;
        return;
    }
    g_connected_clients++;

    std::mt19937 rng(id + 12345);
    std::uniform_int_distribution<int> key_dist(0, 1000); 
    std::uniform_int_distribution<int> op_dist(0, 9); 

    for (int i = 0; i < OPS_PER_CLIENT; ++i) {
        int k = key_dist(rng);
        std::string key = "k_" + std::to_string(k);
        
        bool success = false;
        if (op_dist(rng) >= 5) { // PUT
            success = client.put(key, "val").has_value();
        } else { // GET
            auto res = client.get(key);
            success = (res.has_value() || res.error() == ClientError::KeyNotFound);
        }

        if (success) g_success++;
        else g_fail++;
    }
    
    {
        std::lock_guard<std::mutex> lock(print_mutex);
        std::cout << "[Thread-" << id << "] Finished.\n" << std::flush;
    }
}

int main(int argc, char** argv) {
    if (argc >= 2) {
        ROUTER_PORT = static_cast<uint16_t>(std::stoi(argv[1]));
    }
    if (argc >= 3) {
        NUM_CLIENTS = std::stoi(argv[2]);
    }
    if (argc >= 4) {
        OPS_PER_CLIENT = std::stoi(argv[3]);
    }

    if (argc < 2 || argc > 4) {
        std::cout << "Usage: load_test <router_port> [num_clients] [ops_per_client]\n";
        std::cout << "Defaults: Clients=" << NUM_CLIENTS << ", Ops=" << OPS_PER_CLIENT << "\n";
        // We don't exit, just continue with defaults/parsed values
    }

    std::cout << "=== Distributed KV Load Tester ===\n";
    std::cout << "Target: 127.0.0.1:" << ROUTER_PORT << "\n";
    std::cout << "Clients: " << NUM_CLIENTS << "\n";
    std::cout << "Ops/Client: " << OPS_PER_CLIENT << "\n";
    std::cout << "Total Requests: " << ((long)NUM_CLIENTS * OPS_PER_CLIENT) << "\n";
    std::cout << "Starting...\n" << std::flush;

    auto start_time = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> clients;
    clients.reserve(NUM_CLIENTS);

    for (int i = 0; i < NUM_CLIENTS; ++i) {
        clients.emplace_back(client_task, i);
    }

    // Monitor Loop
    long total_expected = (long)NUM_CLIENTS * OPS_PER_CLIENT;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        long s = g_success.load();
        long f = g_fail.load();
        long total = s + f;
        int con = g_connected_clients.load();
        
        std::cout << "--- Status: Connected=" << con 
                  << " Ops=" << total << "/" << total_expected 
                  << " (Succ=" << s << " Fail=" << f << ") ---\n" << std::flush;

        if (total >= total_expected) break;
        if (con == NUM_CLIENTS && total == total_expected) break; 
    }

    for (auto& t : clients) {
        if (t.joinable()) t.join();
    }

    std::cout << "\n=== DONE ===\n" << std::flush;
    return 0;
}