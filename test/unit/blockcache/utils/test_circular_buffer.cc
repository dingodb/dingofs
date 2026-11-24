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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <vector>

#include "blockcache/core/reactor/task.h"
#include "blockcache/utils/circular_buffer.h"

namespace dingofs {
namespace blockcache {

// The buffer is the reactor's run queue; these stand in for its tasks.
class NoopTask final : public Task {
 public:
  void RunAndDispose() noexcept override {}
};

TEST(CircularBufferTest, PushBackAndPopFront) {
  std::vector<NoopTask> tasks(1000);

  {  // first in, first out
    CircularBuffer<Task*> queue(4);
    for (int i = 0; i < 4; i++) {
      queue.PushBack(&tasks[i]);
    }
    for (int i = 0; i < 4; i++) {
      EXPECT_EQ(queue.PopFront(), &tasks[i]) << i;
    }
  }

  {  // a full buffer doubles and keeps the order, also when it has wrapped
    CircularBuffer<Task*> queue(4);
    for (int i = 0; i < 4; i++) {
      queue.PushBack(&tasks[i]);
    }
    for (int i = 0; i < 2; i++) {
      EXPECT_EQ(queue.PopFront(), &tasks[i]) << i;
    }
    for (int i = 4; i < 7; i++) {  // 4 and 5 wrap to the front, 6 grows it
      queue.PushBack(&tasks[i]);
    }
    for (int i = 2; i < 7; i++) {
      EXPECT_EQ(queue.PopFront(), &tasks[i]) << i;
    }
  }

  {  // any initial capacity grows as far as needed, even none at all
    for (const uint32_t capacity : {0, 1, 3, 512}) {
      CircularBuffer<Task*> queue(capacity);
      for (NoopTask& task : tasks) {
        queue.PushBack(&task);
      }
      for (const NoopTask& task : tasks) {
        ASSERT_EQ(queue.PopFront(), &task) << capacity;
      }
    }
  }

  {  // interleaved, as tasks get scheduled while others run
    CircularBuffer<Task*> queue;
    size_t next = 0;
    for (size_t i = 0; i < tasks.size(); i++) {
      queue.PushBack(&tasks[i]);
      if (i % 3 == 2) {
        ASSERT_EQ(queue.PopFront(), &tasks[next++]);
      }
    }
    while (next < tasks.size()) {
      ASSERT_EQ(queue.PopFront(), &tasks[next++]);
    }
  }

  {  // a full buffer grows from any head, also after the head wraps around
    for (int head = 0; head < 8; head++) {
      CircularBuffer<Task*> queue(4);
      for (int i = 0; i < head; i++) {
        queue.PushBack(&tasks[i]);
        EXPECT_EQ(queue.PopFront(), &tasks[i]) << head;
      }
      const int first = 5 * head;  // new tasks each round: no stale slot passes
      for (int i = first; i < first + 5; i++) {  // the fifth grows it
        queue.PushBack(&tasks[i]);
      }
      for (int i = first; i < first + 5; i++) {
        EXPECT_EQ(queue.PopFront(), &tasks[i]) << head;
      }
    }
  }
}

TEST(CircularBufferTest, Empty) {
  NoopTask task;
  CircularBuffer<Task*> queue(1);
  EXPECT_TRUE(queue.empty());
  queue.PushBack(&task);
  EXPECT_FALSE(queue.empty());
  queue.PushBack(&task);
  EXPECT_FALSE(queue.empty());
  queue.PopFront();
  queue.PopFront();
  EXPECT_TRUE(queue.empty());
}

}  // namespace blockcache
}  // namespace dingofs
