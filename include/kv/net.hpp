#pragma once

/**
 * @file net.hpp
 * @brief Defines the networking primitives and the Reactor event loop.
 */

#include "kv/core.hpp"
#include <sys/types.h>
#include <functional>
#include <chrono>
#include <memory>
#include <vector>
#include <optional>

namespace kv::net {

    using namespace kv::core;
    
    /**
     * @brief RAII Wrapper for a File Descriptor (Socket).
     *
     * Ensures the file descriptor is closed when the object goes out of scope.
     * Non-copyable, but movable.
     */
    class Socket {
    public:
        /// @brief Constructs an invalid socket (-1).
        Socket() = default;

        /**
         * @brief Takes ownership of an existing file descriptor.
         * @param fd The file descriptor to manage.
         */
        explicit Socket(int fd) noexcept;

        /// @brief Destructor. Closes the fd if valid.
        ~Socket();

        // Delete copy
        Socket(const Socket&) = delete;
        Socket& operator=(const Socket&) = delete;

        // Allow move
        Socket(Socket&& other) noexcept;
        Socket& operator=(Socket&& other) noexcept;

        /**
         * @brief Check if the socket is valid.
         * @return true if fd > 0.
         */
        [[nodiscard]] bool valid() const { return fd_ != -1; }

        /**
         * @brief Get the raw file descriptor.
         * @return int The raw fd.
         */
        [[nodiscard]] int native_handle() const { return fd_; }

        /**
         * @brief Sets the O_NONBLOCK flag on the socket.
         * @return std::expected<void, int> Void on success, errno on failure.
         */
        std::expected<void, int> set_non_blocking();

        /**
         * @brief Sets SO_REUSEADDR on the socket.
         * @return std::expected<void, int> Void on success, errno on failure.
         */
        std::expected<void, int> set_reuse_addr();

        /**
         * @brief Sets SO_REUSEPORT on the socket.
         * * Required for multiple processes to bind the same port (Prefork model).
         * @return std::expected<void, int> Void on success, errno on failure.
         */
        std::expected<void, int> set_reuse_port();

        /**
         * @brief Binds the socket to a specific port on INADDR_ANY.
         * @param port The port to bind to (host order).
         * @return std::expected<void, int> Void on success, errno on failure.
         */
        std::expected<void, int> bind_inaddr_any(uint16_t port);

        /**
         * @brief Starts listening for incoming connections.
         * @return std::expected<void, int> Void on success, errno on failure.
         */
        std::expected<void, int> listen();

        /**
         * @brief Accepts a new connection.
         * @return std::expected<Socket, int> A new Socket object for the client, or errno.
         */
        std::expected<Socket, int> accept();

        /**
         * @brief Connects to a remote address.
         * @param ip The IP address string (e.g., "127.0.0.1").
         * @param port The port number.
         * @return std::expected<void, int> Void on success, errno.
         */
        std::expected<void, int> connect(std::string_view ip, uint16_t port);

        /**
         * @brief Writes data to the socket.
         * @param data The buffer to write.
         * @return std::expected<size_t, int> Bytes written, or errno.
         */
        std::expected<size_t, int> write(std::span<const Byte> data);

        /**
         * @brief Reads data from the socket.
         * @param buffer The buffer to read into.
         * @return std::expected<size_t, int> Bytes read, or errno. 0 indicates EOF.
         */
        std::expected<size_t, int> read(std::span<Byte> buffer);

        /// @brief Closes the socket immediately and resets fd to -1.
        void close();

    private:
        int fd_ = -1;
    };

    /**
     * @brief The Reactor (Event Loop).
     *
     * Manages IO multiplexing via epoll and handles timer events.
     */
    class EventLoop {
    public:
        using Clock = std::chrono::steady_clock;
        using TimePoint = Clock::time_point;
        using Duration = Clock::duration;
        
        /// @brief Callback type for IO events (Readable/Writable).
        using IoHandler = std::function<void(int fd)>;
        
        /// @brief Callback type for Timer events.
        using TimerHandler = std::function<void()>;

        EventLoop();
        ~EventLoop();

        // Non-copyable (simplifies pointer management in callbacks)
        EventLoop(const EventLoop&) = delete;
        EventLoop& operator=(const EventLoop&) = delete;

        /**
         * @brief Register a file descriptor for READ events.
         * @param fd The file descriptor (must remain valid).
         * @param handler The callback to invoke when data is available.
         */
        void on_read(int fd, IoHandler handler);

        /**
         * @brief Register a file descriptor for WRITE events.
         * @param fd The file descriptor.
         * @param handler The callback to invoke when buffer space is available.
         */
        void on_write(int fd, IoHandler handler);

        /**
         * @brief Stop monitoring a file descriptor.
         * @param fd The file descriptor to remove.
         */
        void remove(int fd);

        /**
         * @brief Schedules a task to run after a specific duration.
         * @param delay The duration to wait.
         * @param handler The callback to invoke.
         * @return uint64_t A unique timer ID (can be used to cancel).
         */
        uint64_t schedule_after(Duration delay, TimerHandler handler);

        /**
         * @brief Runs the event loop.
         * * Blocks until stop() is called.
         */
        void run();

        /**
         * @brief Signals the loop to terminate.
         * * Thread-safe.
         */
        void stop();

    private:
        int epoll_fd_;
        bool running_ = false;

        struct Timer {
            uint64_t id;
            TimePoint deadline;
            TimerHandler callback;

            // Min-heap ordering (earliest deadline first)
            bool operator>(const Timer& other) const {
                return deadline > other.deadline;
            }
        };

        std::vector<Timer> timers_; // Used as a heap
        uint64_t next_timer_id_ = 1;

        // Map to keep handlers alive.
        struct HandlerContext {
            IoHandler read_cb;
            IoHandler write_cb;
        };
        // Note: Using raw FD as key. In a full system, might need a generation ID.
        std::vector<std::unique_ptr<HandlerContext>> handlers_; 

        void ensure_handlers_size(int fd);
        void process_timers();
    };

} // namespace kv::net