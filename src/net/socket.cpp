/**
 * @file socket.cpp
 * @brief Implementation of the RAII Socket wrapper.
 */

#include "kv/net.hpp"
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <cerrno>
#include <utility>

namespace kv::net {

    Socket::Socket(int fd) noexcept : fd_(fd) {}

    Socket::~Socket() {
        close();
    }

    Socket::Socket(Socket&& other) noexcept : fd_(other.fd_) {
        other.fd_ = -1;
    }

    Socket& Socket::operator=(Socket&& other) noexcept {
        if (this != &other) {
            close();
            fd_ = other.fd_;
            other.fd_ = -1;
        }
        return *this;
    }

    void Socket::close() {
        if (fd_ != -1) {
            ::close(fd_);
            fd_ = -1;
        }
    }

    std::expected<void, int> Socket::set_non_blocking() {
        int flags = ::fcntl(fd_, F_GETFL, 0);
        if (flags == -1) return std::unexpected(errno);

        if (::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
            return std::unexpected(errno);
        }
        return {};
    }

    std::expected<void, int> Socket::set_reuse_addr() {
        int opt = 1;
        if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
            return std::unexpected(errno);
        }
        return {};
    }

    std::expected<void, int> Socket::set_reuse_port() {
#ifdef SO_REUSEPORT
        int opt = 1;
        if (::setsockopt(fd_, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) < 0) {
            return std::unexpected(errno);
        }
        return {};
#else
        // Platform doesn't support REUSEPORT (e.g., older Linux or Windows).
        // For this assignment, we fail because the architecture relies on it.
        return std::unexpected(EOPNOTSUPP);
#endif
    }

    std::expected<void, int> Socket::bind_inaddr_any(uint16_t port) {
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        if (::bind(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            return std::unexpected(errno);
        }
        return {};
    }

    std::expected<void, int> Socket::listen() {
        if (::listen(fd_, SOMAXCONN) < 0) {
            return std::unexpected(errno);
        }
        return {};
    }

    std::expected<Socket, int> Socket::accept() {
        struct sockaddr_in client_addr{};
        socklen_t len = sizeof(client_addr);
        int client_fd = ::accept(fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &len);

        if (client_fd < 0) {
            // EAGAIN/EWOULDBLOCK are normal in non-blocking mode, caller handles them
            return std::unexpected(errno);
        }
        return Socket(client_fd);
    }

    std::expected<void, int> Socket::connect(std::string_view ip, uint16_t port) {
        struct sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        
        // Note: inet_pton usually requires a null-terminated string, copy to string to be safe
        std::string ip_str(ip);
        if (::inet_pton(AF_INET, ip_str.c_str(), &addr.sin_addr) <= 0) {
            return std::unexpected(EINVAL);
        }

        if (::connect(fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            if (errno != EINPROGRESS) {
                return std::unexpected(errno);
            }
            // EINPROGRESS is expected for non-blocking connect
        }
        return {};
    }

    std::expected<size_t, int> Socket::write(std::span<const Byte> data) {
        ssize_t n = ::write(fd_, data.data(), data.size());
        if (n < 0) {
            return std::unexpected(errno);
        }
        return static_cast<size_t>(n);
    }

    std::expected<size_t, int> Socket::read(std::span<Byte> buffer) {
        ssize_t n = ::read(fd_, buffer.data(), buffer.size());
        if (n < 0) {
            return std::unexpected(errno);
        }
        return static_cast<size_t>(n);
    }

} // namespace kv::net