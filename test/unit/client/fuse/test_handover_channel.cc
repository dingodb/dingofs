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

#include "client/fuse/upgrade/handover_channel.h"

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>
#include <set>
#include <string>

namespace dingofs {
namespace client {
namespace fuse {

TEST(HandoverChannelTest, MakeMagicIsBigEndianAscii) {
  EXPECT_EQ(MakeMagic('A', 'B', 'C', 'D'), 0x41424344u);
  // High bit set must not sign-extend.
  EXPECT_EQ(MakeMagic('\xFF', '\x00', '\x00', '\x00'), 0xFF000000u);
}

TEST(HandoverChannelTest, MessageMagicsAreDistinct) {
  std::set<uint32_t> magics = {
      static_cast<uint32_t>(HandoverMessage::kPrepare),
      static_cast<uint32_t>(HandoverMessage::kReadyToExit),
      static_cast<uint32_t>(HandoverMessage::kAck),
      static_cast<uint32_t>(HandoverMessage::kNack),
  };
  EXPECT_EQ(magics.size(), 4u) << "handover message magics must be unique";
}

TEST(HandoverChannelTest, MessageRoundTrip) {
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);

  ASSERT_TRUE(SendHandoverMessage(sv[0], HandoverMessage::kReadyToExit));
  HandoverMessage got;
  ASSERT_TRUE(RecvHandoverMessage(sv[1], &got));
  EXPECT_EQ(got, HandoverMessage::kReadyToExit);

  close(sv[0]);
  close(sv[1]);
}

TEST(HandoverChannelTest, RecvFromClosedPeerFails) {
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  close(sv[0]);  // peer hung up -> EOF on the other end

  HandoverMessage got;
  EXPECT_FALSE(RecvHandoverMessage(sv[1], &got));
  close(sv[1]);
}

// SendFd/GetFd pass a real fd (SCM_RIGHTS) plus an inline data payload; the
// received fd must refer to the SAME open file description as the sent one.
TEST(HandoverChannelTest, SendFdPassesFdAndData) {
  int sv[2];
  ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, sv), 0);
  int pipefd[2];
  ASSERT_EQ(pipe(pipefd), 0);

  const std::string payload = "INIT-MESSAGE-BYTES";
  int sent = SendFd(sv[0], pipefd[0], const_cast<char*>(payload.data()),
                    payload.size());
  ASSERT_EQ(sent, static_cast<int>(payload.size()));

  char buf[64] = {0};
  size_t real_size = 0;
  int recv_fd = GetFd(sv[1], buf, sizeof(buf), &real_size);
  ASSERT_GT(recv_fd, 2);
  EXPECT_EQ(real_size, payload.size());
  EXPECT_EQ(std::string(buf, real_size), payload);

  // Prove recv_fd is a dup of pipefd[0]: write to the pipe, read via recv_fd.
  ASSERT_EQ(write(pipefd[1], "Z", 1), 1);
  char z = 0;
  ASSERT_EQ(read(recv_fd, &z, 1), 1);
  EXPECT_EQ(z, 'Z');

  close(recv_fd);
  close(pipefd[0]);
  close(pipefd[1]);
  close(sv[0]);
  close(sv[1]);
}

}  // namespace fuse
}  // namespace client
}  // namespace dingofs
