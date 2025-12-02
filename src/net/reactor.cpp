/**
 * @file reactor.cpp
 * @brief Implementation of the EventLoop and Timer logic using epoll (Linux) or kqueue (macOS).
 */

#include "kv/net.hpp"
#include <unistd.h>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <vector>

// Platform-specific includes
#ifdef __linux__
    #include <sys/epoll.h>
#elif defined(__APPLE__) || defined(__FreeBSD__)
    #include <sys/types.h>
    #include <sys/event.h>
    #include <sys/time.h>
#else
    #error "Platform not supported. Linux or macOS required."
#endif

namespace kv::net {

    EventLoop::EventLoop() {
#ifdef __linux__
        epoll_fd_ = ::epoll_create1(0);
#elif defined(__APPLE__)
        epoll_fd_ = ::kqueue();
#endif
        if (epoll_fd_ < 0) {
            throw std::runtime_error("Failed to create event queue instance");
        }
    }

    EventLoop::~EventLoop() {
        if (epoll_fd_ >= 0) {
            ::close(epoll_fd_);
        }
    }

    void EventLoop::ensure_handlers_size(int fd) {
        if (fd >= static_cast<int>(handlers_.size())) {
            handlers_.resize(fd + 1);
        }
        if (!handlers_[fd]) {
            handlers_[fd] = std::make_unique<HandlerContext>();
        }
    }

    // src/net/reactor.cpp

    void EventLoop::on_read(int fd, IoHandler handler) {
        ensure_handlers_size(fd);
        handlers_[fd]->read_cb = std::move(handler);
        
#ifdef __linux__
        struct epoll_event ev{};
        ev.data.fd = fd;
        
        // DYNAMIC LOGIC: Only ask for events if we have a handler
        if (handlers_[fd]->read_cb) ev.events |= EPOLLIN;
        if (handlers_[fd]->write_cb) ev.events |= EPOLLOUT;
        
        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            if (errno == EEXIST) {
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
            }
        }
#elif defined(__APPLE__)
        // macOS kqueue handles enable/disable via EV_ENABLE/EV_DISABLE
        struct kevent ev;
        uint16_t flags = (handlers_[fd]->read_cb) ? (EV_ADD | EV_ENABLE) : (EV_DELETE);
        EV_SET(&ev, fd, EVFILT_READ, flags, 0, 0, nullptr);
        ::kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr);
#endif
    }

    void EventLoop::on_write(int fd, IoHandler handler) {
        ensure_handlers_size(fd);
        handlers_[fd]->write_cb = std::move(handler);

#ifdef __linux__
        struct epoll_event ev{};
        ev.data.fd = fd;

        // DYNAMIC LOGIC: Only ask for events if we have a handler
        if (handlers_[fd]->read_cb) ev.events |= EPOLLIN;
        if (handlers_[fd]->write_cb) ev.events |= EPOLLOUT;

        if (::epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            if (errno == EEXIST) {
                ::epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &ev);
            }
        }
#elif defined(__APPLE__)
        struct kevent ev;
        uint16_t flags = (handlers_[fd]->write_cb) ? (EV_ADD | EV_ENABLE) : (EV_DELETE);
        EV_SET(&ev, fd, EVFILT_WRITE, flags, 0, 0, nullptr);
        ::kevent(epoll_fd_, &ev, 1, nullptr, 0, nullptr);
#endif
    }

    void EventLoop::remove(int fd) {
#ifdef __linux__
        ::epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
#elif defined(__APPLE__)
        struct kevent ev[2];
        int n = 0;
        EV_SET(&ev[n++], fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
        EV_SET(&ev[n++], fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
        // Best effort removal, ignore errors (e.g. if only read was registered)
        ::kevent(epoll_fd_, ev, n, nullptr, 0, nullptr);
#endif

        if (fd < static_cast<int>(handlers_.size())) {
            handlers_[fd].reset();
        }
    }

    uint64_t EventLoop::schedule_after(Duration delay, TimerHandler handler) {
        auto now = Clock::now();
        Timer t{
            .id = next_timer_id_++,
            .deadline = now + delay,
            .callback = std::move(handler)
        };
        
        timers_.push_back(std::move(t));
        std::push_heap(timers_.begin(), timers_.end(), std::greater<>{});
        
        return t.id;
    }

    void EventLoop::process_timers() {
        auto now = Clock::now();
        
        while (!timers_.empty()) {
            if (timers_.front().deadline > now) {
                break;
            }

            std::pop_heap(timers_.begin(), timers_.end(), std::greater<>{});
            Timer expired = std::move(timers_.back());
            timers_.pop_back();

            if (expired.callback) {
                expired.callback();
            }
        }
    }

    void EventLoop::run() {
        running_ = true;
        constexpr int MAX_EVENTS = 64;

#ifdef __linux__
        struct epoll_event events[MAX_EVENTS];
#elif defined(__APPLE__)
        struct kevent events[MAX_EVENTS];
#endif

        while (running_) {
            // Calculate timeout
            int timeout_ms = -1;
            if (!timers_.empty()) {
                auto now = Clock::now();
                auto next_deadline = timers_.front().deadline;
                if (next_deadline <= now) {
                    timeout_ms = 0; 
                } else {
                    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(next_deadline - now);
                    timeout_ms = static_cast<int>(duration.count());
                }
            }

            int n = 0;
#ifdef __linux__
            n = ::epoll_wait(epoll_fd_, events, MAX_EVENTS, timeout_ms);
#elif defined(__APPLE__)
            struct timespec ts;
            struct timespec* ts_ptr = nullptr;
            if (timeout_ms >= 0) {
                ts.tv_sec = timeout_ms / 1000;
                ts.tv_nsec = (timeout_ms % 1000) * 1000000;
                ts_ptr = &ts;
            }
            n = ::kevent(epoll_fd_, nullptr, 0, events, MAX_EVENTS, ts_ptr);
#endif

            if (n < 0) {
                if (errno == EINTR) continue;
                break; // Error
            }

            for (int i = 0; i < n; ++i) {
#ifdef __linux__
                int fd = events[i].data.fd;
                uint32_t flags = events[i].events;
                bool is_read = (flags & (EPOLLIN | EPOLLERR | EPOLLHUP));
                bool is_write = (flags & EPOLLOUT);
#elif defined(__APPLE__)
                int fd = static_cast<int>(events[i].ident);
                int16_t filter = events[i].filter;
                bool is_read = (filter == EVFILT_READ);
                bool is_write = (filter == EVFILT_WRITE);
                // Handle EV_EOF (disconnect) as a read event so read() returns 0/error
                if (events[i].flags & EV_EOF) is_read = true;
#endif

                if (fd >= static_cast<int>(handlers_.size()) || !handlers_[fd]) continue;

                if (is_read && handlers_[fd]->read_cb) {
                    handlers_[fd]->read_cb(fd);
                }

                if (fd < static_cast<int>(handlers_.size()) && handlers_[fd]) {
                    if (is_write && handlers_[fd]->write_cb) {
                        handlers_[fd]->write_cb(fd);
                    }
                }
            }

            process_timers();
        }
    }

    void EventLoop::stop() {
        running_ = false;
    }

} // namespace kv::net