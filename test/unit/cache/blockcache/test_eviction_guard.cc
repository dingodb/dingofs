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

#include "cache/blockcache/eviction_guard.h"

namespace dingofs {
namespace cache {

constexpr uint64_t kMiB = 1ULL << 20;
constexpr uint64_t kBlockSize = 4 * kMiB;
constexpr uint64_t kCapacity = 400 * kMiB;
constexpr uint64_t kNow = 1000000;

const EvictionGuard::Config kConfig{/*protect_s=*/3600, /*max_ratio=*/0.5};

static CacheValue Warmup() {
  CacheValue value(kBlockSize, iutil::TimeSpec(kNow));
  value.source = static_cast<uint8_t>(BlockSource::kWarmup);
  return value;
}

static CacheValue ReadMiss() {
  CacheValue value(kBlockSize, iutil::TimeSpec(kNow));
  value.source = static_cast<uint8_t>(BlockSource::kReadMiss);
  return value;
}

static EvictionPolicy::FilterFunc EvictAll() {
  return [](const CacheValue&) { return FilterStatus::kEvictIt; };
}

TEST(EvictionGuardTest, OnAddStampsWarmupOnly) {
  EvictionGuard guard(kCapacity);

  auto warm = Warmup();
  guard.OnAdd(kConfig, kNow, &warm);
  ASSERT_EQ(warm.protect_until, kNow + kConfig.protect_s);
  ASSERT_EQ(guard.ProtectedBytes(), kBlockSize);

  auto miss = ReadMiss();
  guard.OnAdd(kConfig, kNow, &miss);
  ASSERT_EQ(miss.protect_until, 0);
  ASSERT_EQ(guard.ProtectedBytes(), kBlockSize);

  // protect_s = 0 disables protection entirely
  auto warm2 = Warmup();
  guard.OnAdd({0, 0.5}, kNow, &warm2);
  ASSERT_EQ(warm2.protect_until, 0);
}

TEST(EvictionGuardTest, WrapSkipsLeasedBlocksUntilExpiry) {
  EvictionGuard guard(kCapacity);
  auto warm = Warmup();
  guard.OnAdd(kConfig, kNow, &warm);

  // within the lease: skipped
  auto filter = guard.Wrap(kConfig, kNow + 10, EvictAll());
  ASSERT_EQ(filter(warm), FilterStatus::kSkip);
  ASSERT_EQ(guard.ProtectSkips(), 1);

  // after expiry: evictable
  filter = guard.Wrap(kConfig, kNow + kConfig.protect_s + 1, EvictAll());
  ASSERT_EQ(filter(warm), FilterStatus::kEvictIt);

  // unprotected blocks always pass through
  filter = guard.Wrap(kConfig, kNow + 10, EvictAll());
  ASSERT_EQ(filter(ReadMiss()), FilterStatus::kEvictIt);
}

TEST(EvictionGuardTest, OverQuotaEvictsProtectedInScanOrder) {
  EvictionGuard guard(kCapacity);

  // protect more than max_ratio (50%) of capacity
  uint64_t blocks = kCapacity * 0.6 / kBlockSize;
  for (uint64_t i = 0; i < blocks; i++) {
    auto warm = Warmup();
    guard.OnAdd(kConfig, kNow, &warm);
  }
  ASSERT_GT(guard.ProtectedBytes(), kCapacity * kConfig.max_ratio);

  // quota exceeded: leases no longer skip, eviction proceeds in scan order
  auto warm = Warmup();
  guard.OnAdd(kConfig, kNow, &warm);
  auto filter = guard.Wrap(kConfig, kNow + 10, EvictAll());
  ASSERT_EQ(filter(warm), FilterStatus::kEvictIt);
}

TEST(EvictionGuardTest, BarrenRoundForcesUnprotection) {
  EvictionGuard guard(kCapacity);
  auto warm = Warmup();
  guard.OnAdd(kConfig, kNow, &warm);

  // a pressured round that freed nothing arms the force switch
  guard.OnRoundEnd(/*under_pressure=*/true, /*freed_bytes=*/0);
  auto filter = guard.Wrap(kConfig, kNow + 10, EvictAll());
  ASSERT_EQ(filter(warm), FilterStatus::kEvictIt);  // lease ignored
  ASSERT_EQ(guard.ForceUnprotectRounds(), 1);

  // the force switch is one-shot
  guard.OnRoundEnd(true, kBlockSize);
  filter = guard.Wrap(kConfig, kNow + 10, EvictAll());
  ASSERT_EQ(filter(warm), FilterStatus::kSkip);
}

TEST(EvictionGuardTest, OnRemoveReleasesAccountingOnce) {
  EvictionGuard guard(kCapacity);
  auto warm = Warmup();
  guard.OnAdd(kConfig, kNow, &warm);
  ASSERT_EQ(guard.ProtectedBytes(), kBlockSize);

  guard.OnRemove(warm);
  ASSERT_EQ(guard.ProtectedBytes(), 0);

  // removing unprotected blocks never underflows
  guard.OnRemove(ReadMiss());
  ASSERT_EQ(guard.ProtectedBytes(), 0);
}

}  // namespace cache
}  // namespace dingofs
