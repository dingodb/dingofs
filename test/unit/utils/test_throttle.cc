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

#include "utils/throttle.h"

#include <gtest/gtest.h>

#include <chrono>  // NOLINT

namespace dingofs {
namespace utils {
namespace unit_test {

TEST(ThrottleTest, AllTypesDisabledByDefault) {
  Throttle throttle;

  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_TOTAL));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_READ));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_WRITE));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::BPS_TOTAL));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::BPS_READ));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::BPS_WRITE));
}

TEST(ThrottleTest, UpdateParamsWithNonZeroLimitEnablesThrottle) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  params.iopsTotal = ThrottleParams(1000, 0, 0);

  throttle.UpdateThrottleParams(params);

  EXPECT_TRUE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_TOTAL));
  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_READ));
}

TEST(ThrottleTest, UpdateParamsWithZeroLimitDisablesThrottle) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  params.bpsWrite = ThrottleParams(500, 0, 0);
  throttle.UpdateThrottleParams(params);
  ASSERT_TRUE(throttle.IsThrottleEnabled(Throttle::Type::BPS_WRITE));

  // Re-apply with limit reset to 0: throttle should turn off again.
  ReadWriteThrottleParams disabled;
  disabled.bpsWrite = ThrottleParams(0, 0, 0);
  throttle.UpdateThrottleParams(disabled);

  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::BPS_WRITE));
}

TEST(ThrottleTest, UpdateParamsIsIdempotentWhenUnchanged) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  params.iopsRead = ThrottleParams(100, 0, 0);

  throttle.UpdateThrottleParams(params);
  ASSERT_TRUE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_READ));

  // Applying identical params again must not disable the throttle.
  throttle.UpdateThrottleParams(params);
  EXPECT_TRUE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_READ));
}

TEST(ThrottleTest, AddBlocksUntilTokensAvailableWhenEnabled) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  params.iopsWrite = ThrottleParams(1000, 0, 0);
  throttle.UpdateThrottleParams(params);

  // With an iops throttle enabled, each write Add() call consumes exactly
  // one token regardless of the requested length; this must return promptly
  // for the first (unblocked) call rather than hang.
  auto start = std::chrono::steady_clock::now();
  throttle.Add(/*isRead=*/false, /*length=*/4096);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                .count(),
            500);
}

TEST(ThrottleTest, AddIgnoresDisabledThrottleTypes) {
  Throttle throttle;  // nothing enabled
  // Should return immediately without blocking since every throttle type is
  // disabled by default.
  auto start = std::chrono::steady_clock::now();
  throttle.Add(/*isRead=*/true, /*length=*/1 << 20);
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                .count(),
            100);
}

TEST(ThrottleTest, ReadAddDoesNotConsumeWriteOnlyThrottle) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  // Only write iops throttled; reads should bypass it entirely and return
  // immediately even though the bucket has a very small limit.
  params.iopsWrite = ThrottleParams(1, 0, 0);
  throttle.UpdateThrottleParams(params);

  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < 100; ++i) {
    throttle.Add(/*isRead=*/true, /*length=*/1);
  }
  auto elapsed = std::chrono::steady_clock::now() - start;

  EXPECT_LT(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                .count(),
            500);
}

TEST(ThrottleTest, StopClearsAllThrottles) {
  Throttle throttle;
  ReadWriteThrottleParams params;
  params.iopsTotal = ThrottleParams(100, 0, 0);
  throttle.UpdateThrottleParams(params);
  ASSERT_TRUE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_TOTAL));

  throttle.Stop();

  EXPECT_FALSE(throttle.IsThrottleEnabled(Throttle::Type::IOPS_TOTAL));
}

}  // namespace unit_test
}  // namespace utils
}  // namespace dingofs
