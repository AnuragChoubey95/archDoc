#include "kv/client.hpp"
#include <iostream>
#include <string>

using namespace kv::client;

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: demo <router_port>\n";
        return 1;
    }

    uint16_t router_port = static_cast<uint16_t>(std::stoi(argv[1]));

    std::cout << "=== Distributed KV Store Demo ===\n";
    std::cout << "Connecting to router at 127.0.0.1:" << router_port << "...\n";

    Client::Config config;
    config.router_port = router_port;
    Client client(config);

    auto conn_res = client.connect();
    if (!conn_res) {
        std::cerr << "Failed to connect to router.\n";
        return 1;
    }
    std::cout << "Connected!\n";

    // 1. PUT
    std::cout << "\n[1] Storing 'user:100' -> 'Alice'...\n";
    auto put_res = client.put("user:100", "Alice");
    if (put_res) std::cout << "Success.\n";
    else std::cout << "Failed.\n";

    // 2. GET
    std::cout << "\n[2] Retrieving 'user:100'...\n";
    auto get_res = client.get("user:100");
    if (get_res) {
        std::string val(reinterpret_cast<const char*>(get_res->data()), get_res->size());
        std::cout << "Found: " << val << "\n";
    } else {
        std::cout << "Not Found.\n";
    }

    // 3. EXISTS
    std::cout << "\n[3] Checking existence of 'user:100'...\n";
    auto ex_res = client.exists("user:100");
    if (ex_res && ex_res.value()) std::cout << "Key exists.\n";
    else std::cout << "Key does not exist.\n";

    // 4. DELETE
    std::cout << "\n[4] Deleting 'user:100'...\n";
    auto del_res = client.del("user:100");
    if (del_res) std::cout << "Success.\n";
    else std::cout << "Failed.\n";

    // 5. GET Again
    std::cout << "\n[5] Retrieving 'user:100' after delete...\n";
    auto get_res_2 = client.get("user:100");
    if (!get_res_2 && get_res_2.error() == ClientError::KeyNotFound) {
        std::cout << "Correctly returned Not Found.\n";
    } else {
        std::cout << "Unexpected result.\n";
    }

    return 0;
}