/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client/fuse/upgrade/handover_server.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <future>
#include <memory>
#include <string>
#include <thread>

#include "client/fuse/upgrade/handover_channel.h"

#define FUSE_USE_VERSION 317
#include "fuse_lowlevel.h"  // struct fuse_buf

namespace dingofs {
namespace client {
namespace fuse {

class HandoverServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    socket_path_ =
        "/tmp/handover-server-test." + std::to_string(getpid()) + ".sock";
    unlink(socket_path_.c_str());

    // A stand-in for the /dev/fuse fd to hand off: the read end of a pipe. The
    // server only passes the int across SCM_RIGHTS; it never calls libfuse on
    // it, so any real fd works and lets the test prove the handoff.
    ASSERT_EQ(pipe(pipefd_), 0);

    init_data_ = "FAKE-INIT-MESSAGE";
    init_buf_.mem = init_data_.data();
    init_buf_.mem_size = init_data_.size();
    init_buf_.size = init_data_.size();
  }

  void TearDown() override {
    close(pipefd_[0]);
    close(pipefd_[1]);
    unlink(socket_path_.c_str());
  }

  std::unique_ptr<HandoverServer> MakeServer() {
    return std::make_unique<HandoverServer>(
        socket_path_, [this]() { return pipefd_[0]; }, &init_buf_);
  }

  std::string socket_path_;
  int pipefd_[2];
  std::string init_data_;
  struct fuse_buf init_buf_ {};
};

// A connector receives the /dev/fuse fd and the INIT payload, and the fd refers
// to the same open file the server handed off.
TEST_F(HandoverServerTest, TakeoverDeliversFdAndInit) {
  auto server = MakeServer();
  server->Start();

  char buf[128] = {0};
  size_t real_size = 0;
  int comm_fd = -1;
  int fuse_fd =
      GetFuseFd(socket_path_.c_str(), buf, sizeof(buf), &real_size, &comm_fd);

  ASSERT_GT(fuse_fd, 2);
  EXPECT_EQ(std::string(buf, real_size), init_data_);

  ASSERT_EQ(write(pipefd_[1], "Z", 1), 1);
  char z = 0;
  ASSERT_EQ(read(fuse_fd, &z, 1), 1);
  EXPECT_EQ(z, 'Z');

  close(fuse_fd);
  if (comm_fd >= 0) close(comm_fd);
  server->Stop();
}

// A second connector while the first handover is still live (fail-closed):
// IsHandoverInFlight() sees a live peer and rejects, so the second gets no fd.
TEST_F(HandoverServerTest, ConcurrentConnectionRejected) {
  auto server = MakeServer();
  server->Start();

  // First connector stays open -> in flight (do not close comm1 yet).
  char buf1[128] = {0};
  size_t real1 = 0;
  int comm1 = -1;
  int fd1 = GetFuseFd(socket_path_.c_str(), buf1, sizeof(buf1), &real1, &comm1);
  ASSERT_GT(fd1, 2);

  // Second connector: server rejects (closes without SendFd) -> GetFd EOF -> -1.
  char buf2[128] = {0};
  size_t real2 = 0;
  int comm2 = -1;
  int fd2 = GetFuseFd(socket_path_.c_str(), buf2, sizeof(buf2), &real2, &comm2);
  EXPECT_EQ(fd2, -1) << "concurrent upgrade must be rejected";

  close(fd1);
  if (comm1 >= 0) close(comm1);
  if (comm2 >= 0) close(comm2);
  server->Stop();
}

// H1: once the handover is committed (NotifyReadyToExit succeeded),
// the server rejects ALL further connectors -- even after the committed peer's
// fd is cleared -- so the /dev/fuse fd is never handed out twice.
TEST_F(HandoverServerTest, CommittedRejectsLateConnection) {
  auto server = MakeServer();
  server->Start();

  // new-side peer: take over, then run the kPrepare -> kReadyToExit handshake.
  // Signal once connected so the controller side reads kPrepare only after
  // client_fd_ is set.
  std::promise<void> connected;
  std::thread peer([&]() {
    char buf[128] = {0};
    size_t real = 0;
    int comm = -1;
    int fuse_fd =
        GetFuseFd(socket_path_.c_str(), buf, sizeof(buf), &real, &comm);
    ASSERT_GT(fuse_fd, 2);
    connected.set_value();

    ASSERT_TRUE(SendHandoverMessage(comm, HandoverMessage::kPrepare));
    HandoverMessage msg;
    ASSERT_TRUE(RecvHandoverMessage(comm, &msg));
    EXPECT_EQ(msg, HandoverMessage::kReadyToExit);

    close(fuse_fd);
    close(comm);
  });

  // Test thread plays the controller's peer-facing calls.
  connected.get_future().wait();
  ASSERT_TRUE(server->WaitHandoverPrepare());  // reads kPrepare
  ASSERT_TRUE(server->NotifyReadyToExit());    // commits and sends ready
  peer.join();

  // committed_ is now set: a fresh connector is rejected regardless of state.
  char buf2[128] = {0};
  size_t real2 = 0;
  int comm2 = -1;
  int fd2 = GetFuseFd(socket_path_.c_str(), buf2, sizeof(buf2), &real2, &comm2);
  EXPECT_EQ(fd2, -1) << "post-commit connection must be rejected";

  if (comm2 >= 0) close(comm2);
  server->Stop();
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
