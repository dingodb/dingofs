/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>

#include "common/blockaccess/rados/osd_map_refresher.h"

namespace dingofs {
namespace blockaccess {
namespace {

using namespace std::chrono_literals;

TEST(OsdMapRefresherTest, DisabledDoesNotRun) {
  std::atomic<uint64_t> calls{0};
  OsdMapRefresher refresher(
      {.enabled = false, .interval = 10ms, .jitter_pct = 0}, [&] {
        ++calls;
        return 0;
      });

  ASSERT_TRUE(refresher.Start());
  std::this_thread::sleep_for(30ms);
  refresher.Stop();

  EXPECT_EQ(calls.load(), 0);
}

TEST(OsdMapRefresherTest, RefreshesImmediatelyAndPeriodically) {
  std::mutex mutex;
  std::condition_variable cv;
  uint64_t calls = 0;
  OsdMapRefresher refresher(
      {.enabled = true, .interval = 20ms, .jitter_pct = 0}, [&] {
        {
          std::lock_guard<std::mutex> lock(mutex);
          ++calls;
        }
        cv.notify_all();
        return 0;
      });

  ASSERT_TRUE(refresher.Start());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 1s, [&] { return calls >= 2; }));
  }
  refresher.Stop();

  EXPECT_GE(calls, 2);
}

TEST(OsdMapRefresherTest, StartIsIdempotent) {
  std::mutex mutex;
  std::condition_variable cv;
  uint64_t calls = 0;
  OsdMapRefresher refresher({.enabled = true, .interval = 10s, .jitter_pct = 0},
                            [&] {
                              {
                                std::lock_guard<std::mutex> lock(mutex);
                                ++calls;
                              }
                              cv.notify_all();
                              return 0;
                            });

  ASSERT_TRUE(refresher.Start());
  ASSERT_TRUE(refresher.Start());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 1s, [&] { return calls >= 1; }));
  }
  refresher.Stop();
  refresher.Stop();

  EXPECT_EQ(calls, 1);
}

TEST(OsdMapRefresherTest, StopInterruptsSleep) {
  std::mutex mutex;
  std::condition_variable cv;
  uint64_t calls = 0;

  OsdMapRefresher refresher({.enabled = true, .interval = 10s, .jitter_pct = 0},
                            [&] {
                              {
                                std::lock_guard<std::mutex> lock(mutex);
                                ++calls;
                              }
                              cv.notify_all();
                              return 0;
                            });

  ASSERT_TRUE(refresher.Start());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 1s, [&] { return calls == 1; }));
  }

  const auto begin = std::chrono::steady_clock::now();
  refresher.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - begin;

  EXPECT_LT(elapsed, 1s);
  EXPECT_EQ(calls, 1);
}

TEST(OsdMapRefresherTest, ContinuesAfterFailure) {
  std::mutex mutex;
  std::condition_variable cv;
  uint64_t calls = 0;
  OsdMapRefresher refresher(
      {.enabled = true, .interval = 20ms, .jitter_pct = 0}, [&] {
        uint64_t current;
        {
          std::lock_guard<std::mutex> lock(mutex);
          current = ++calls;
        }
        cv.notify_all();
        return current == 1 ? -EAGAIN : 0;
      });

  ASSERT_TRUE(refresher.Start());
  {
    std::unique_lock<std::mutex> lock(mutex);
    ASSERT_TRUE(cv.wait_for(lock, 1s, [&] { return calls >= 2; }));
  }
  refresher.Stop();

  EXPECT_GE(calls, 2);
}

}  // namespace
}  // namespace blockaccess
}  // namespace dingofs
