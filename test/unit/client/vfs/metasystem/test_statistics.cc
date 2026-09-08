// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include "client/vfs/metasystem/mds/statistics.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

TEST(SlidingWindowTest, SlidesOverSeconds) {
  SlidingWindow w;
  const uint64_t t0 = 1000;

  // same second accumulates
  EXPECT_EQ(1, w.Inc(t0));
  EXPECT_EQ(2, w.Inc(t0));

  // consecutive seconds within window sum up
  EXPECT_EQ(3, w.Inc(t0 + 1));
  EXPECT_EQ(4, w.Inc(t0 + 2));
  EXPECT_EQ(5, w.Inc(t0 + 3));

  // t0 falls out of [t0+1, t0+4]
  EXPECT_EQ(4, w.Inc(t0 + kWindowSize));

  // jump far ahead: everything stale is dropped
  EXPECT_EQ(1, w.Inc(t0 + 100));
}

TEST(SlidingWindowTest, IgnoresDelayedIncrementAfterBucketReuse) {
  SlidingWindow window;
  constexpr uint64_t old_time_s = 1000;
  constexpr uint64_t new_time_s = old_time_s + kWindowSize;

  EXPECT_EQ(1, window.Inc(new_time_s));
  EXPECT_EQ(0, window.Inc(old_time_s));
  EXPECT_EQ(2, window.Inc(new_time_s));
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
