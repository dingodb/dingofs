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

#include <chrono>
#include <cstdint>
#include <thread>

#include "blockcache/utils/time.h"

namespace dingofs {
namespace blockcache {

TEST(TimeTest, Constants) {
  EXPECT_EQ(kNsPerUs, 1000);
  EXPECT_EQ(kNsPerMs, 1000000);
  EXPECT_EQ(kNsPerSec, 1000000000);
}

TEST(TimeTest, TimestampNs) {
  {  // never goes back
    uint64_t last = TimestampNs();
    for (int i = 0; i < 1000; i++) {
      const uint64_t now = TimestampNs();
      EXPECT_GE(now, last);
      last = now;
    }
  }

  {  // in nanoseconds
    const uint64_t start = TimestampNs();
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const uint64_t elapsed = TimestampNs() - start;
    EXPECT_GE(elapsed, 20 * kNsPerMs);
    EXPECT_LT(elapsed, 2 * kNsPerSec);
  }
}

TEST(TimeTest, TimestampSec) {
  const uint64_t before = TimestampNs();
  const uint32_t sec = TimestampSec();
  const uint64_t after = TimestampNs();
  EXPECT_GE(sec, before / kNsPerSec);
  EXPECT_LE(sec, after / kNsPerSec);
}

TEST(TimeTest, CachedTimestampNs) {
  std::thread([] {
    {  // nothing cached: every call reads the clock
      const uint64_t before = TimestampNs();
      const uint64_t first = CachedTimestampNs();
      EXPECT_GE(first, before);
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      EXPECT_GT(CachedTimestampNs(), first);
    }

    {  // once refreshed it stands still until the next refresh
      RefreshCachedTimestamp();
      const uint64_t cached = CachedTimestampNs();
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
      EXPECT_EQ(CachedTimestampNs(), cached);
      EXPECT_GT(TimestampNs(), cached);

      RefreshCachedTimestamp();
      EXPECT_GT(CachedTimestampNs(), cached);
    }

    {  // per thread: a new thread does not see what this one cached
      RefreshCachedTimestamp();
      std::thread([] {
        const uint64_t first = CachedTimestampNs();
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
        EXPECT_GT(CachedTimestampNs(), first);
      }).join();
    }
  }).join();
}

}  // namespace blockcache
}  // namespace dingofs
