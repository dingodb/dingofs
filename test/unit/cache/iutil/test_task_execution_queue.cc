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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/iutil/task_execution_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <vector>

namespace dingofs {
namespace cache {
namespace iutil {

TEST(TaskExecutionQueueTest, SubmitRunsAllTasks) {
  TaskExecutionQueue queue;
  queue.Start();

  std::atomic<int> counter{0};
  const int kTasks = 100;
  for (int i = 0; i < kTasks; ++i) {
    queue.Submit([&counter]() { counter.fetch_add(1); });
  }

  // Shutdown stops and joins the queue, guaranteeing every task has run.
  queue.Shutdown();

  EXPECT_EQ(counter.load(), kTasks);
  EXPECT_EQ(queue.Size(), 0);
}

TEST(TaskExecutionQueueTest, TasksRunInSubmissionOrder) {
  TaskExecutionQueue queue;
  queue.Start();

  // Single consumer, so execution order matches submission order. The vector is
  // only read after Shutdown joins the consumer, so no extra locking is needed.
  std::vector<int> order;
  for (int i = 0; i < 10; ++i) {
    queue.Submit([&order, i]() { order.push_back(i); });
  }
  queue.Shutdown();

  std::vector<int> expected = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
  EXPECT_EQ(order, expected);
}

TEST(TaskExecutionQueueTest, EmptyQueueShutdown) {
  TaskExecutionQueue queue;
  queue.Start();
  EXPECT_EQ(queue.Size(), 0);
  queue.Shutdown();
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
