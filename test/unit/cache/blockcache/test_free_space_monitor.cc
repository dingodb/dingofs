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

#include "cache/blockcache/free_space_monitor.h"

namespace dingofs {
namespace cache {

using Band = FreeSpaceMonitor::Band;

constexpr uint64_t kGiB = 1ULL << 30;

const FreeSpaceMonitor::Thresholds kThresholds{0.12, 0.10, 0.03};

static FreeSpaceMonitor::StatFS Stat(double bytes_ratio, double files_ratio) {
  return {bytes_ratio, files_ratio, 100 * kGiB, 1000000};
}

TEST(FreeSpaceMonitorTest, JudgeBands) {
  // plenty of free space
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.50, 0.50), Band::kOk),
            Band::kOk);
  // below cull: start eviction
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.09, 0.50), Band::kOk),
            Band::kCull);
  // below stop: reject
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.02, 0.50), Band::kOk),
            Band::kStop);
}

TEST(FreeSpaceMonitorTest, JudgeWorseOfBytesAndFiles) {
  // inode pressure alone triggers the band
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.50, 0.09), Band::kOk),
            Band::kCull);
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.50, 0.02), Band::kOk),
            Band::kStop);
}

TEST(FreeSpaceMonitorTest, JudgeHysteresis) {
  auto stat = Stat(0.11, 0.50);  // within [cull, run)
  // not evicting: stays ok
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, stat, Band::kOk), Band::kOk);
  // already culling: keeps culling until free climbs above run
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, stat, Band::kCull),
            Band::kCull);
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, stat, Band::kStop),
            Band::kCull);
  // above run: eviction stops
  ASSERT_EQ(FreeSpaceMonitor::Judge(kThresholds, Stat(0.13, 0.50), Band::kCull),
            Band::kOk);
}

TEST(FreeSpaceMonitorTest, ThresholdsSanitized) {
  // hot-reloaded flags may violate stop < cull <= run
  auto t = FreeSpaceMonitor::Thresholds{0.05, 0.10, 0.20}.Sanitized();
  ASSERT_GE(t.run, t.cull);
  ASSERT_LE(t.stop, t.cull);
}

TEST(FreeSpaceMonitorTest, CalcTargetFreeSpaceDeficit) {
  // free bytes at 8%, run at 12%: want 4% of 100 GiB back
  auto target = FreeSpaceMonitor::CalcTarget(
      kThresholds, Stat(0.08, 0.50),
      {/*used*/ 50 * kGiB, /*capacity*/ 60 * kGiB, /*blocks*/ 10000,
       /*inflight_bytes*/ 0, /*inflight_files*/ 0});
  ASSERT_NEAR(target.want_free_bytes, 4 * kGiB, kGiB / 100);
  ASSERT_EQ(target.want_free_files, 0);
  ASSERT_TRUE(target.NeedEvict());
}

TEST(FreeSpaceMonitorTest, CalcTargetDeductsInflight) {
  // 4 GiB deficit but 3 GiB already enqueued for unlink
  auto target = FreeSpaceMonitor::CalcTarget(
      kThresholds, Stat(0.08, 0.50),
      {50 * kGiB, 60 * kGiB, 10000, /*inflight_bytes*/ 3 * kGiB, 0});
  ASSERT_NEAR(target.want_free_bytes, 1 * kGiB, kGiB / 100);

  // in-flight exceeding the deficit clamps to zero instead of underflowing
  target = FreeSpaceMonitor::CalcTarget(
      kThresholds, Stat(0.08, 0.50),
      {50 * kGiB, 60 * kGiB, 10000, /*inflight_bytes*/ 10 * kGiB, 0});
  ASSERT_EQ(target.want_free_bytes, 0);
  ASSERT_FALSE(target.NeedEvict());
}

TEST(FreeSpaceMonitorTest, CalcTargetCapacityOvershoot) {
  // disk itself is fine but used exceeds the configured capacity
  auto target = FreeSpaceMonitor::CalcTarget(
      kThresholds, Stat(0.50, 0.50),
      {/*used*/ 60 * kGiB, /*capacity*/ 60 * kGiB, /*blocks*/ 10000, 0, 0});
  ASSERT_NEAR(target.want_free_bytes, 3 * kGiB, kGiB / 100);  // to 95%
  ASSERT_EQ(target.want_free_files, 500);                     // 5% of blocks
}

TEST(FreeSpaceMonitorTest, CalcTargetTakesWorseDimension) {
  // capacity overshoot (3 GiB) vs free-space deficit (4 GiB): pick the max;
  // in-flight only offsets the free-space dimension since used_bytes already
  // excludes enqueued deletions
  auto target = FreeSpaceMonitor::CalcTarget(
      kThresholds, Stat(0.08, 0.50),
      {60 * kGiB, 60 * kGiB, 10000, /*inflight_bytes*/ 2 * kGiB, 0});
  ASSERT_NEAR(target.want_free_bytes, 3 * kGiB, kGiB / 100);
}

}  // namespace cache
}  // namespace dingofs
