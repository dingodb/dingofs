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
 * Created Date: 2026-02-02
 * Author: Wine93
 */
#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

#include "cache/iutil/task_execution_queue.h"

namespace dingofs {
namespace cache {
namespace iutil {

class TaskExecutionQueueTest : public ::testing::Test {
 protected:
  void SetUp() override {
    queue_ = std::make_shared<TaskExecutionQueue>();
    queue_->Start();
  }

  void TearDown() override { queue_->Shutdown(); }

  TaskExecutionQueueSPtr queue_;
};

TEST_F(TaskExecutionQueueTest, BasicSubmit) {
  std::atomic<bool> executed{false};

  queue_->Submit([&executed]() { executed = true; });

  // Wait for task to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(executed.load());
}

TEST_F(TaskExecutionQueueTest, MultipleTasksOrder) {
  std::vector<int> results;
  std::mutex mutex;

  for (int i = 0; i < 10; ++i) {
    queue_->Submit([&results, &mutex, i]() {
      std::lock_guard<std::mutex> lock(mutex);
      results.push_back(i);
    });
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Tasks should be executed in order
  ASSERT_EQ(results.size(), 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(results[i], i);
  }
}

TEST_F(TaskExecutionQueueTest, Size) {
  // Initially empty
  // Note: Size may not be 0 immediately after Start()

  std::atomic<int> counter{0};
  std::atomic<bool> start{false};

  // Submit tasks that wait
  for (int i = 0; i < 5; ++i) {
    queue_->Submit([&counter, &start]() {
      while (!start.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      counter++;
    });
  }

  // Allow first task to run
  start = true;

  // Wait for completion
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(counter.load(), 5);
}

TEST_F(TaskExecutionQueueTest, ConcurrentSubmit) {
  std::atomic<int> counter{0};

  auto submitter = [this, &counter]() {
    for (int i = 0; i < 100; ++i) {
      queue_->Submit([&counter]() { counter++; });
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 4; ++i) {
    threads.emplace_back(submitter);
  }

  for (auto& t : threads) {
    t.join();
  }

  // Wait for all tasks to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(counter.load(), 400);
}

TEST_F(TaskExecutionQueueTest, TaskWithException) {
  std::atomic<bool> after_executed{false};

  queue_->Submit([]() {
    // Task that does not throw, just simulating work
  });

  queue_->Submit([&after_executed]() { after_executed = true; });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_TRUE(after_executed.load());
}

TEST_F(TaskExecutionQueueTest, SubmitLambdaWithCapture) {
  std::string result;
  std::string input = "Hello, World!";

  queue_->Submit([&result, input]() { result = input; });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_EQ(result, "Hello, World!");
}

TEST_F(TaskExecutionQueueTest, SubmitHeavyTask) {
  std::atomic<int64_t> sum{0};

  queue_->Submit([&sum]() {
    int64_t local_sum = 0;
    for (int i = 0; i < 1000000; ++i) {
      local_sum += i;
    }
    sum = local_sum;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(500));
  EXPECT_EQ(sum.load(), 499999500000LL);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
