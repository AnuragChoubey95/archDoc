#include "kv/router.hpp"
#include <iostream>
#include <csignal>
#include <vector>
#include <unistd.h>
#include <sys/wait.h>
#include <thread> // for hardware_concurrency
#include <cerrno>

// Platform specific for CPU affinity
#ifdef __linux__
    #include <sched.h>
    #include <pthread.h>
#elif defined(__APPLE__)
    #include <mach/mach.h>
    #include <mach/thread_policy.h>
    #include <pthread.h>
#endif

using namespace kv::router;

// Global service pointer for the signal handler (Worker usage)
std::unique_ptr<RouterService> worker_service;
// Global list of children PIDs (Master usage)
std::vector<pid_t> worker_pids;
bool is_master = true;

void run_worker(int worker_id, RouterService::Config config);

void signal_handler(int signum) {
    if (is_master) {
        // Master: Forward signal to all workers
        std::cerr << "[Master] Caught signal " << signum << ", killing workers...\n";
        for (pid_t pid : worker_pids) {
            kill(pid, SIGTERM);
        }
        // The master will fall through to its wait loop to clean up
    } else {
        // Worker: Stop the service gracefully
        if (worker_service) worker_service->stop();
    }
}

// ---------------------------------------------------------------------------
// Cross-Platform CPU Affinity and Naming (Omitted for brevity, assumed correct)
// ---------------------------------------------------------------------------

void pin_to_core(int core_id) {
    // ... Implementation (Linux or macOS) ...
    #ifdef __linux__
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(core_id, &cpuset);
        if (sched_setaffinity(0, sizeof(cpu_set_t), &cpuset) != 0) {
            std::cerr << "[Worker] Failed to set affinity to core " << core_id << "\n";
        } else {
            // std::cout << "[Worker] Pinned to Core " << core_id << "\n";
        }
    #elif defined(__APPLE__)
        // macOS Affinity logic here...
    #endif
}

void set_process_name(const std::string& name) {
    #ifdef __linux__
        pthread_setname_np(pthread_self(), name.c_str());
    #elif defined(__APPLE__)
        pthread_setname_np(name.c_str());
    #endif
}


void run_worker(int worker_id, RouterService::Config config) {
    is_master = false;
    std::string name = "Worker-" + std::to_string(worker_id);
    
    set_process_name(name);

    int num_cores = std::thread::hardware_concurrency();
    if (num_cores > 0) {
        pin_to_core(worker_id % num_cores);
    }

    try {
        std::cout << "[" << name << "] Starting (PID " << getpid() << ")...\n";
        worker_service = std::make_unique<RouterService>(std::move(config));
        worker_service->run();
    } catch (const std::exception& e) {
        // Only Worker 0 fails if multiple workers attempt to bind BEFORE SO_REUSEPORT fix
        // is fully applied/functional.
        if (std::string(e.what()) == "Failed to bind router port") {
            // Fatal error for worker, but expected if REUSEPORT failed.
            std::cerr << "[" << name << "] Crashed: " << e.what() << "\n";
        } else {
            std::cerr << "[" << name << "] Runtime exception: " << e.what() << "\n";
        }
        std::exit(1);
    }
}

int main(int argc, char** argv) {
    // ... (Configuration parsing is fine) ...
    if (argc < 2) {
        std::cerr << "Usage: router <port> [shard0_ip:port] [shard1_ip:port]...\n";
        return 1;
    }

    RouterService::Config config;
    config.port = static_cast<uint16_t>(std::stoi(argv[1]));
    
    if (argc == 2) {
        config.shard_addresses.push_back({"127.0.0.1", 8001});
        config.shard_addresses.push_back({"127.0.0.1", 8002});
    } else {
        for(int i=2; i<argc; ++i) {
            std::string arg = argv[i];
            size_t colon = arg.find(':');
            std::string ip = arg.substr(0, colon);
            uint16_t port = static_cast<uint16_t>(std::stoi(arg.substr(colon+1)));
            config.shard_addresses.push_back({ip, port});
        }
    }

    // 1. Setup Signal Handling
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // 2. Spawn Workers
    int num_workers = 4;
    std::cout << "[Master] Spawning " << num_workers << " worker processes...\n";

    for (int i = 0; i < num_workers; ++i) {
        pid_t pid = fork();
        
        if (pid < 0) {
            std::cerr << "[Master] Failed to fork worker " << i << "\n";
            continue;
        }

        if (pid == 0) {
            // CHILD PROCESS: Runs the router service loop and exits.
            run_worker(i, config);
            return 0; 
        } else {
            // PARENT PROCESS: Collects PIDs and continues the loop
            worker_pids.push_back(pid);
        }
    }
    
    // 3. Master Loop (Supervisor)
    // The master process must block indefinitely, waiting for child processes to exit 
    // (either due to crashing or a clean shutdown signal from the user/OS).
    int children_remaining = num_workers;
    while (children_remaining > 0) {
        int status;
        pid_t dead_pid = wait(&status); // Blocks until a child changes state
        
        if (dead_pid > 0) {
            children_remaining--;
            // Logging exit status here is good practice
            
        } else if (dead_pid == -1) {
            if (errno == EINTR) continue; // Interrupted by a signal, try again
            if (errno == ECHILD) break; // No more children to wait for
            break;
        }
    }
    
    std::cout << "[Master] All workers cleaned up. Exiting.\n";
    return 0;
}
