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

#include "utils/executor/bthread/bthread_pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>              // NOLINT
#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <thread>              // NOLINT

namespace dingofs {
namespace unit_test {

TEST(BThreadPoolTest, StartReportsConfiguredThreadCount) {
  BThreadPool pool(4);
  pool.Start();

  EXPECT_EQ(pool.GetBackgroundThreads(), 4);

  pool.Stop();
}

TEST(BThreadPoolTest, StartIsIdempotent) {
  BThreadPool pool(2);
  pool.Start();
  pool.Start();  // second call must be a no-op, not spawn extra threads

  EXPECT_EQ(pool.GetBackgroundThreads(), 2);

  pool.Stop();
}

TEST(BThreadPoolTest, StopIsIdempotent) {
  BThreadPool pool(2);
  pool.Start();
  pool.Stop();
  pool.Stop();  // second call must be a safe no-op
}

TEST(BThreadPoolTest, ExecuteRunsSubmittedTask) {
  BThreadPool pool(2);
  pool.Start();

  std::mutex mutex;
  std::condition_variable cv;
  bool done = false;

  pool.Execute([&] {
    std::lock_guard<std::mutex> lg(mutex);
    done = true;
    cv.notify_one();
  });

  std::unique_lock<std::mutex> lk(mutex);
  cv.wait_for(lk, std::chrono::seconds(2), [&] { return done; });
  EXPECT_TRUE(done);

  pool.Stop();
}

TEST(BThreadPoolTest, ExecuteRvalueOverloadRunsTask) {
  BThreadPool pool(2);
  pool.Start();

  std::atomic<int> count(0);
  std::function<void()> task = [&] { count.fetch_add(1); };

  pool.Execute(std::move(task));

  // give the worker bthread a chance to run
  for (int i = 0; i < 100 && count.load() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  EXPECT_EQ(count.load(), 1);

  pool.Stop();
}

TEST(BThreadPoolTest, GetTaskNumReflectsPendingWork) {
  BThreadPool pool(1);
  pool.Start();

  std::mutex mutex;
  std::condition_variable cv;
  bool proceed = false;

  // Block the single worker so subsequent tasks queue up.
  pool.Execute([&] {
    std::unique_lock<std::mutex> lk(mutex);
    cv.wait(lk, [&] { return proceed; });
  });

  // Give the worker time to pick up the blocking task first.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  pool.Execute([] {});
  pool.Execute([] {});

  EXPECT_GE(pool.GetTaskNum(), 1);

  {
    std::lock_guard<std::mutex> lg(mutex);
    proceed = true;
  }
  cv.notify_all();

  pool.Stop();
}

}  // namespace unit_test
}  // namespace dingofs
