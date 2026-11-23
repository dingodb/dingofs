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

/*
 * Project: DingoFS
 * Created Date: 2026-11-23
 * Author: AI
 */

#include <gtest/gtest.h>
#include <liburing.h>

#include "blockcache/core/reactor/uring.h"

namespace dingofs {
namespace blockcache {

class ScopedRing {
 public:
  ScopedRing() { rc_ = io_uring_queue_init_params(8, &ring_, &params_); }
  ~ScopedRing() {
    if (rc_ == 0) {
      io_uring_queue_exit(&ring_);
    }
  }

  ScopedRing(const ScopedRing&) = delete;
  ScopedRing& operator=(const ScopedRing&) = delete;

  int rc() const { return rc_; }
  io_uring* ring() { return &ring_; }
  const io_uring_params& params() const { return params_; }

 private:
  io_uring ring_{};
  io_uring_params params_{};
  int rc_;
};

TEST(UringTest, CheckUringSupport) {
  {
    ScopedRing ring;
    ASSERT_EQ(ring.rc(), 0);
    constexpr UringOpcode kOps[] = {{.op = IORING_OP_NOP, .name = "NOP"},
                                    {.op = IORING_OP_READ, .name = "READ"}};
    CheckUringSupport(ring.ring(), ring.params(), kOps);
  }

  {
    const auto check_unserved = [] {
      ScopedRing ring;
      constexpr UringOpcode kOps[] = {{.op = IORING_OP_NOP, .name = "NOP"},
                                      {.op = IORING_OP_LAST, .name = "LAST"}};
      CheckUringSupport(ring.ring(), ring.params(), kOps);
    };
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(check_unserved(), "missing opcodes: LAST");
  }
}

TEST(UringTest, IsUringOpSupported) {
  EXPECT_TRUE(IsUringOpSupported(IORING_OP_NOP));
  EXPECT_FALSE(IsUringOpSupported(IORING_OP_LAST));
}

}  // namespace blockcache
}  // namespace dingofs
