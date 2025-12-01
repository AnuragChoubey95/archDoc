#include <gtest/gtest.h>
#include "kv/net.hpp"
#include <thread>
#include <sys/socket.h>
#include <fcntl.h> // Added for F_GETFD

using namespace kv::net;

// ---------------------------------------------------------------------------
// Socket RAII Tests
// ---------------------------------------------------------------------------

TEST(SocketTest, RAIIClosesFD) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GT(fd, 0);
    
    {
        Socket s(fd);
        EXPECT_TRUE(s.valid());
        // s goes out of scope here
    }

    // Check if fd is closed using fcntl
    int flags = ::fcntl(fd, F_GETFD);
    EXPECT_EQ(flags, -1);
    EXPECT_EQ(errno, EBADF);
}

TEST(SocketTest, MoveSemantics) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    Socket s1(fd);
    
    Socket s2 = std::move(s1);
    
    EXPECT_FALSE(s1.valid());
    EXPECT_TRUE(s2.valid());
    EXPECT_EQ(s2.native_handle(), fd);
}

// ---------------------------------------------------------------------------
// Reactor Tests
// ---------------------------------------------------------------------------

class ReactorTest : public ::testing::Test {
protected:
    EventLoop loop;
};

TEST_F(ReactorTest, TimerExecution) {
    bool executed = false;
    
    // Schedule a timer for 50ms
    loop.schedule_after(std::chrono::milliseconds(50), [&]() {
        executed = true;
        loop.stop();
    });

    // Run loop (should block for approx 50ms then stop)
    loop.run();

    EXPECT_TRUE(executed);
}

TEST_F(ReactorTest, SocketReadEvent) {
    // Create a socket pair
    int fds[2];
    ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);

    Socket reader(fds[0]);
    Socket writer(fds[1]); // Used to trigger the event

    bool read_triggered = false;

    // Register reader with loop
    loop.on_read(reader.native_handle(), [&](int fd) {
        read_triggered = true;
        
        // Read data to clear socket buffer
        char buf[16];
        ::read(fd, buf, sizeof(buf));
        
        loop.stop();
    });

    // Write data to writer
    std::string msg = "hello";
    writer.write(std::as_bytes(std::span{msg}));

    loop.run();

    EXPECT_TRUE(read_triggered);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}