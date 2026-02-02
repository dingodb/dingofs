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

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "cache/iutil/bthread.h"

namespace dingofs {
namespace cache {
namespace iutil {

class BthreadTest : public ::testing::Test {};

TEST_F(BthreadTest, RunInBthreadBasic) {
  std::atomic<bool> executed{false};

  bthread_t tid = RunInBthread([&executed]() { executed = true; });

  // Wait for bthread to complete
  if (tid != 0) {
    bthread_join(tid, nullptr);
  }

  EXPECT_TRUE(executed.load());
}

TEST_F(BthreadTest, RunInBthreadWithReturn) {
  std::atomic<int> result{0};

  bthread_t tid = RunInBthread([&result]() { result = 42; });

  if (tid != 0) {
    bthread_join(tid, nullptr);
  }

  EXPECT_EQ(result.load(), 42);
}

TEST_F(BthreadTest, RunMultipleBthreads) {
  std::atomic<int> counter{0};
  std::vector<bthread_t> tids;

  for (int i = 0; i < 10; ++i) {
    bthread_t tid = RunInBthread([&counter]() { counter++; });
    if (tid != 0) {
      tids.push_back(tid);
    }
  }

  for (auto tid : tids) {
    bthread_join(tid, nullptr);
  }

  EXPECT_EQ(counter.load(), 10);
}

TEST_F(BthreadTest, RunInBthreadWithCapture) {
  std::string result;
  std::string input = "Hello from bthread";

  bthread_t tid = RunInBthread([&result, input]() { result = input; });

  if (tid != 0) {
    bthread_join(tid, nullptr);
  }

  EXPECT_EQ(result, "Hello from bthread");
}

class BthreadJoinerTest : public ::testing::Test {
 protected:
  void SetUp() override { joiner_ = std::make_unique<BthreadJoiner>(); }

  void TearDown() override { joiner_.reset(); }

  BthreadJoinerUPtr joiner_;
};

TEST_F(BthreadJoinerTest, StartShutdown) {
  joiner_->Start();
  joiner_->Shutdown();
}

TEST_F(BthreadJoinerTest, DoubleStart) {
  joiner_->Start();
  joiner_->Start();  // Should not crash
  joiner_->Shutdown();
}

TEST_F(BthreadJoinerTest, BackgroundJoin) {
  joiner_->Start();

  std::atomic<bool> executed{false};
  bthread_t tid = RunInBthread([&executed]() {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    executed = true;
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }

  // Wait a bit for the background join to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_TRUE(executed.load());

  joiner_->Shutdown();
}

TEST_F(BthreadJoinerTest, MultipleBackgroundJoins) {
  joiner_->Start();

  std::atomic<int> counter{0};
  std::vector<bthread_t> tids;

  for (int i = 0; i < 5; ++i) {
    bthread_t tid = RunInBthread([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      counter++;
    });
    if (tid != 0) {
      joiner_->BackgroundJoin(tid);
      tids.push_back(tid);
    }
  }

  // Wait for all bthreads to complete
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_EQ(counter.load(), 5);

  joiner_->Shutdown();
}

TEST_F(BthreadJoinerTest, ShutdownWithPendingJoins) {
  joiner_->Start();

  std::atomic<int> counter{0};

  for (int i = 0; i < 10; ++i) {
    bthread_t tid = RunInBthread([&counter]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      counter++;
    });
    if (tid != 0) {
      joiner_->BackgroundJoin(tid);
    }
  }

  // Shutdown should wait for all pending joins
  joiner_->Shutdown();
  EXPECT_EQ(counter.load(), 10);
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs
