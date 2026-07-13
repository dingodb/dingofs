// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <thread>              // NOLINT

#include "utils/executor/bthread/bthread_executor.h"
#include "utils/executor/thread/executor_impl.h"

namespace dingofs {
namespace unit_test {

TEST(BthreadExecutorTest, StartStopReportsThreadCount) {
  BthreadExecutor executor(3);

  EXPECT_TRUE(executor.Start());
  EXPECT_EQ(executor.ThreadNum(), 3);
  EXPECT_EQ(executor.TaskNum(), 0);
  EXPECT_EQ(executor.Name(), "BthreadExecutor");

  EXPECT_TRUE(executor.Stop());
  // Stopping an already-stopped executor reports false.
  EXPECT_FALSE(executor.Stop());
}

TEST(BthreadExecutorTest, ExecuteRunsTask) {
  BthreadExecutor executor(2);
  ASSERT_TRUE(executor.Start());

  std::mutex mutex;
  std::condition_variable cv;
  bool ran = false;

  EXPECT_TRUE(executor.Execute([&] {
    std::lock_guard<std::mutex> lg(mutex);
    ran = true;
    cv.notify_one();
  }));

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait_for(lk, std::chrono::seconds(2), [&] { return ran; });
  EXPECT_TRUE(ran);

  executor.Stop();
}

TEST(BthreadExecutorTest, ScheduleRunsTaskAfterDelay) {
  BthreadExecutor executor(2);
  ASSERT_TRUE(executor.Start());

  std::mutex mutex;
  std::condition_variable cv;
  bool ran = false;

  EXPECT_TRUE(executor.Schedule(
      [&] {
        std::lock_guard<std::mutex> lg(mutex);
        ran = true;
        cv.notify_one();
      },
      10));

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait_for(lk, std::chrono::seconds(2), [&] { return ran; });
  EXPECT_TRUE(ran);

  executor.Stop();
}

TEST(ExecutorImplTest, StartStopReportsThreadCount) {
  ExecutorImpl executor("unit_test_exec", 3);

  EXPECT_TRUE(executor.Start());
  EXPECT_EQ(executor.ThreadNum(), 3);
  EXPECT_EQ(executor.TaskNum(), 0);
  EXPECT_EQ(executor.Name(), "ExecutorImpl");

  EXPECT_TRUE(executor.Stop());
  EXPECT_FALSE(executor.Stop());
}

TEST(ExecutorImplTest, ExecuteRunsTask) {
  ExecutorImpl executor("unit_test_exec", 2);
  ASSERT_TRUE(executor.Start());

  std::atomic<bool> ran(false);
  EXPECT_TRUE(executor.Execute([&] { ran.store(true); }));

  for (int i = 0; i < 200 && !ran.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(ran.load());

  executor.Stop();
}

TEST(ExecutorImplTest, ScheduleRunsTaskAfterDelay) {
  ExecutorImpl executor("unit_test_exec", 2);
  ASSERT_TRUE(executor.Start());

  std::atomic<bool> ran(false);
  EXPECT_TRUE(executor.Schedule([&] { ran.store(true); }, 10));

  for (int i = 0; i < 200 && !ran.load(); ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  EXPECT_TRUE(ran.load());

  executor.Stop();
}

}  // namespace unit_test
}  // namespace dingofs
