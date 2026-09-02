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
 * Created Date: 2026-09-02
 * Author: AI
 */

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <butil/time.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <vector>

#include "cache/iutil/bthread.h"
#include "cache/iutil/gate.h"

namespace dingofs {
namespace cache {
namespace iutil {

TEST(GateTest, EnterAndLeave) {
  Gate gate;
  EXPECT_TRUE(gate.Enter());
  EXPECT_TRUE(gate.Enter());
  gate.Leave();
  gate.Leave();

  gate.Close();
  EXPECT_FALSE(gate.Enter());
}

TEST(GateTest, CloseWaitsForInflight) {
  Gate gate;
  ASSERT_TRUE(gate.Enter());

  bthread::CountdownEvent closed(1);
  std::atomic<int> seq{0};
  int close_seq = 0;
  bthread_t tid = RunInBthread([&]() {
    gate.Close();
    close_seq = ++seq;
    closed.signal();
  });
  ASSERT_NE(tid, 0);

  while (gate.Enter()) {  // spin until Close() has started closing
    gate.Leave();
    bthread_usleep(100);
  }

  // Close() cannot return while one caller is still inside.
  EXPECT_EQ(closed.timed_wait(butil::milliseconds_from_now(50)), ETIMEDOUT);

  int leave_seq = ++seq;
  gate.Leave();
  bthread_join(tid, nullptr);

  EXPECT_LT(leave_seq, close_seq);
  EXPECT_FALSE(gate.Enter());
}

TEST(GateTest, CloseIsIdempotent) {
  Gate gate;
  gate.Close();
  gate.Close();
  EXPECT_FALSE(gate.Enter());
}

TEST(GateTest, ConcurrentChurn) {
  Gate gate;
  std::atomic<int> inside{0};
  std::atomic<int> entered{0};
  std::vector<bthread_t> tids;
  for (int i = 0; i < 8; i++) {
    tids.push_back(RunInBthread([&]() {
      for (int j = 0; j < 1000; j++) {
        if (!gate.Enter()) {
          return;
        }
        entered++;
        inside++;
        bthread_usleep(1);
        inside--;
        gate.Leave();
      }
    }));
  }

  while (entered.load() == 0) {
    bthread_usleep(100);
  }
  gate.Close();
  EXPECT_EQ(inside.load(), 0);  // strict: inside-- happens before Leave()
  EXPECT_FALSE(gate.Enter());

  for (auto tid : tids) {
    bthread_join(tid, nullptr);
  }
  EXPECT_EQ(inside.load(), 0);
  EXPECT_GT(entered.load(), 0);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
