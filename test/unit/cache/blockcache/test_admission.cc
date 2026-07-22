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
 * Created Date: 2026-07-22
 * Author: Jingli Chen (Wine93)
 */

#include <gtest/gtest.h>

#include "cache/blockcache/admission.h"

namespace dingofs {
namespace cache {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kBlockSize = 4 * kMiB;
constexpr uint64_t kNow = 1000000;

static CacheKey Key(uint64_t id) { return BlockKey(1, 1, id, 1, 0); }

TEST(AdmissionControllerTest, BothMechanismsOffAdmitsEverything) {
  AdmissionController admission;
  AdmissionController::Config off{false, 0};
  for (uint64_t i = 0; i < 100; i++) {
    ASSERT_TRUE(
        admission.Admit(off, kNow, Key(i), BlockSource::kReadMiss, kBlockSize));
  }
  ASSERT_EQ(admission.Accepts(), 0);  // fast path: not even counted
}

TEST(AdmissionControllerTest, SecondHitAdmitsOnSecondTouch) {
  AdmissionController admission;
  AdmissionController::Config config{true, 0};

  // first touch: fingerprint remembered, write rejected
  ASSERT_FALSE(admission.Admit(config, kNow, Key(1), BlockSource::kReadMiss,
                               kBlockSize));
  ASSERT_EQ(admission.RejectsSecondHit(), 1);

  // second touch within the window: admitted
  ASSERT_TRUE(admission.Admit(config, kNow + 10, Key(1),
                              BlockSource::kReadMiss, kBlockSize));
  ASSERT_EQ(admission.Accepts(), 1);
}

TEST(AdmissionControllerTest, DoorkeeperForgetsAfterTwoWindows) {
  AdmissionController admission;
  AdmissionController::Config config{true, 0};

  ASSERT_FALSE(admission.Admit(config, kNow, Key(1), BlockSource::kReadMiss,
                               kBlockSize));
  // two full windows later both rotating sets have been cleared
  ASSERT_FALSE(admission.Admit(config, kNow + 2 * 3600 + 10, Key(1),
                               BlockSource::kReadMiss, kBlockSize));
}

TEST(AdmissionControllerTest, WarmupBypassesEverything) {
  AdmissionController admission;
  AdmissionController::Config config{true, 1};  // strictest settings
  for (uint64_t i = 0; i < 10; i++) {
    ASSERT_TRUE(admission.Admit(config, kNow, Key(i), BlockSource::kWarmup,
                                kBlockSize));
  }
  ASSERT_EQ(admission.Accepts(), 10);
  ASSERT_EQ(admission.RejectsSecondHit(), 0);
}

TEST(AdmissionControllerTest, WriteBudgetThrottlesOverBudgetTraffic) {
  AdmissionController admission;
  // budget 4 MiB/s, offered load ~400 MiB/s: probability must fall to floor
  AdmissionController::Config config{false, 4};

  uint64_t admitted = 0, total = 0;
  uint64_t now = kNow;
  for (int window = 0; window < 30; window++) {
    for (int i = 0; i < 100 * 60; i++) {  // 100 blocks/s offered for 60s
      total++;
      if (admission.Admit(config, now, Key(total), BlockSource::kReadMiss,
                          kBlockSize)) {
        admitted++;
      }
    }
    now += 60;
  }

  // after convergence the admit ratio approaches the 5% probability floor
  double ratio = admitted * 1.0 / total;
  ASSERT_LT(ratio, 0.30);
  ASSERT_GT(admission.RejectsWriteBudget(), 0);
}

}  // namespace cache
}  // namespace dingofs
