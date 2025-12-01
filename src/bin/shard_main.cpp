#include "kv/shard.hpp"
#include <iostream>
#include <csignal>

using namespace kv::shard;

std::unique_ptr<ShardService> service;

void signal_handler(int) {
    if (service) service->stop();
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: shard_service <port>\n";
        return 1;
    }

    uint16_t port = static_cast<uint16_t>(std::stoi(argv[1]));
    
    ShardService::Config config;
    config.port = port;

    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    try {
        service = std::make_unique<ShardService>(std::move(config));
        service->run();
    } catch (const std::exception& e) {
        std::cerr << "Shard crashed: " << e.what() << "\n";
        return 1;
    }

    return 0;
}