/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <thread>
#include <vector>

#include "client/vfs/metasystem/mds/dir_profile.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {
namespace test {

constexpr uint64_t kSmallThreshold = 32 * 1024 * 1024;  // 32 MiB
constexpr uint32_t kWarmupDirMinFiles = 100;
constexpr uint32_t kWarmupOpenThreshold = 16;

class DirProfileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    saved_tiny_max_ = FLAGS_vfs_tiny_file_max_size;
    saved_batch_size_ = FLAGS_vfs_meta_warmup_small_file_batch_size;
    saved_ttl_s_ = FLAGS_vfs_meta_warmup_small_file_ttl_s;

    FLAGS_vfs_tiny_file_max_size = kSmallThreshold;
    FLAGS_vfs_meta_warmup_small_file_batch_size = 256;
    FLAGS_vfs_meta_warmup_small_file_ttl_s = 600;
  }

  void TearDown() override {
    FLAGS_vfs_tiny_file_max_size = saved_tiny_max_;
    FLAGS_vfs_meta_warmup_small_file_batch_size = saved_batch_size_;
    FLAGS_vfs_meta_warmup_small_file_ttl_s = saved_ttl_s_;
  }

  uint64_t saved_tiny_max_{0};
  uint32_t saved_batch_size_{0};
  uint32_t saved_ttl_s_{0};
};

TEST_F(DirProfileTest, CtorAndAccessors) {
  DirProfile p(42, 0);
  EXPECT_EQ(p.ParentIno(), 42u);
  EXPECT_EQ(p.TotalChildren(), 0u);
  EXPECT_EQ(p.SmallFileCount(), 0u);
  EXPECT_EQ(p.WarmedCount(), 0u);
  EXPECT_FALSE(p.IsFinalized());
  EXPECT_FALSE(p.IsSmallFileDir());
  EXPECT_EQ(p.LastActiveTimeS(), 0u);
}

TEST_F(DirProfileTest, AccumulateClassifiesEntries) {
  DirProfile p(1, 0);

  // Small file: counted in both totals and small file list.
  p.Accumulate(100, FileType::kFile, 1024);
  // Directory: counted only in totals.
  p.Accumulate(101, FileType::kDirectory, 0);
  // Symlink: counted only in totals.
  p.Accumulate(102, FileType::kSymlink, 0);
  // Large file: counted only in totals.
  p.Accumulate(103, FileType::kFile, kSmallThreshold + 1);
  // Boundary: equal to threshold should still be considered small.
  p.Accumulate(104, FileType::kFile, kSmallThreshold);

  EXPECT_EQ(p.TotalChildren(), 5u);
  EXPECT_EQ(p.SmallFileCount(), 2u);

  auto small = p.SmallFileInosForTest();
  ASSERT_EQ(small.size(), 2u);
  EXPECT_EQ(small[0], 100u);
  EXPECT_EQ(small[1], 104u);
}

TEST_F(DirProfileTest, FinalizeMarksSmallFileDirWhenAboveMin) {
  DirProfile p(1, 0);
  for (uint32_t i = 0; i < kWarmupDirMinFiles; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  EXPECT_TRUE(p.IsFinalized());
  EXPECT_TRUE(p.IsSmallFileDir());
  EXPECT_EQ(p.SmallFileCount(), kWarmupDirMinFiles);
}

TEST_F(DirProfileTest, FinalizeNotSmallFileDirWhenBelowMin) {
  DirProfile p(1, 0);
  for (uint32_t i = 0; i < kWarmupDirMinFiles - 1; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  EXPECT_TRUE(p.IsFinalized());
  EXPECT_FALSE(p.IsSmallFileDir());
}

TEST_F(DirProfileTest, FinalizeNotSmallFileDirWhenEmpty) {
  DirProfile p(1, 0);
  p.Finalize();
  EXPECT_TRUE(p.IsFinalized());
  EXPECT_FALSE(p.IsSmallFileDir());
}

TEST_F(DirProfileTest, CheckAndGenWarmupInosBelowThresholdReturnsEmpty) {
  DirProfile p(1, 0);
  for (uint32_t i = 0; i < 200; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  for (uint32_t i = 0; i < kWarmupOpenThreshold - 1; ++i) {
    auto inos = p.CheckAndGenWarmupInos(2000 + i);
    EXPECT_TRUE(inos.empty());
  }
  EXPECT_EQ(p.WarmedCount(), 0u);
}

TEST_F(DirProfileTest, CheckAndGenWarmupInosAtThresholdReturnsBatch) {
  DirProfile p(1, 0);
  // 200 small files, none of which collide with the open inos used below.
  for (uint32_t i = 0; i < 200; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  std::vector<Ino> result;
  for (uint32_t i = 0; i < kWarmupOpenThreshold; ++i) {
    result = p.CheckAndGenWarmupInos(5000 + i);
  }

  // Last call (i.e. the kWarmupOpenThreshold-th) should yield candidates.
  // Batch size capped by FLAGS_vfs_meta_warmup_small_file_batch_size = 256, so
  // we expect the full set of small files.
  ASSERT_EQ(result.size(), 200u);
  EXPECT_EQ(result.front(), 1000u);
  EXPECT_EQ(result.back(), 1199u);

  // last_active_time_s_ is updated.
  EXPECT_GT(p.LastActiveTimeS(), 0u);
}

TEST_F(DirProfileTest, CheckAndGenWarmupInosRespectsBatchSize) {
  FLAGS_vfs_meta_warmup_small_file_batch_size = 32;

  DirProfile p(1, 0);
  for (uint32_t i = 0; i < 200; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  std::vector<Ino> result;
  for (uint32_t i = 0; i < kWarmupOpenThreshold; ++i) {
    result = p.CheckAndGenWarmupInos(5000 + i);
  }
  EXPECT_EQ(result.size(), 32u);
  EXPECT_EQ(result.front(), 1000u);
  EXPECT_EQ(result.back(), 1031u);
}

TEST_F(DirProfileTest, CheckAndGenWarmupInosExcludesOpenInos) {
  FLAGS_vfs_meta_warmup_small_file_batch_size = 16;

  DirProfile p(1, 0);
  // Use small file inos that overlap with the open inos so they get filtered.
  for (uint32_t i = 0; i < 200; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  std::vector<Ino> result;
  // Open inos are 1000..1015 — exactly the first batch of small-file inos.
  for (uint32_t i = 0; i < kWarmupOpenThreshold; ++i) {
    result = p.CheckAndGenWarmupInos(1000 + i);
  }

  ASSERT_EQ(result.size(), 16u);
  // First 16 small file inos are filtered, so we get 1016..1031.
  EXPECT_EQ(result.front(), 1016u);
  EXPECT_EQ(result.back(), 1031u);
}

TEST_F(DirProfileTest, WarmedPosAdvancesAcrossInvocations) {
  FLAGS_vfs_meta_warmup_small_file_batch_size = 16;

  DirProfile p(1, 0);
  for (uint32_t i = 0; i < 200; ++i) {
    p.Accumulate(1000 + i, FileType::kFile, 1024);
  }
  p.Finalize();

  // First batch.
  std::vector<Ino> first;
  for (uint32_t i = 0; i < kWarmupOpenThreshold; ++i) {
    first = p.CheckAndGenWarmupInos(5000 + i);
  }
  ASSERT_EQ(first.size(), 16u);
  EXPECT_EQ(first.front(), 1000u);
  EXPECT_EQ(first.back(), 1015u);

  // After GenWarmupInos, open_inos_ is cleared. Trigger another batch.
  std::vector<Ino> second;
  for (uint32_t i = 0; i < kWarmupOpenThreshold; ++i) {
    second = p.CheckAndGenWarmupInos(6000 + i);
  }
  ASSERT_EQ(second.size(), 16u);
  EXPECT_EQ(second.front(), 1016u);
  EXPECT_EQ(second.back(), 1031u);
}

TEST_F(DirProfileTest, CheckAndGenWarmupInosLastActiveTimeUpdated) {
  DirProfile p(1, 0);
  uint64_t before = utils::Timestamp();
  p.CheckAndGenWarmupInos(123);
  uint64_t after = utils::Timestamp();

  uint64_t active = p.LastActiveTimeS();
  EXPECT_GE(active, before);
  EXPECT_LE(active, after);
}

// ---------------------------------------------------------------------------
// DirProfileCache tests
// ---------------------------------------------------------------------------

class DirProfileCacheTest : public DirProfileTest {};

TEST_F(DirProfileCacheTest, GetMissingReturnsNullptr) {
  auto cache = DirProfileCache::New();
  EXPECT_EQ(cache->Get(42), nullptr);
  EXPECT_EQ(cache->Size(), 0u);
}

TEST_F(DirProfileCacheTest, PutAndGet) {
  auto cache = DirProfileCache::New();
  DirProfileSPtr p = std::make_shared<DirProfile>(7, 0);
  cache->Put(p);

  auto got = cache->Get(7);
  ASSERT_NE(got, nullptr);
  EXPECT_EQ(got->ParentIno(), 7u);
  EXPECT_EQ(cache->Size(), 1u);
}

TEST_F(DirProfileCacheTest, PutKeepsExistingWhenChildCountNotGreater) {
  auto cache = DirProfileCache::New();

  DirProfileSPtr first = std::make_shared<DirProfile>(9, 0);
  for (int i = 0; i < 10; ++i) {
    first->Accumulate(100 + i, FileType::kFile, 1024);
  }
  cache->Put(first);

  // Same total_children → should NOT replace.
  DirProfileSPtr same = std::make_shared<DirProfile>(9, 0);
  for (int i = 0; i < 10; ++i) {
    same->Accumulate(200 + i, FileType::kFile, 1024);
  }
  cache->Put(same);

  EXPECT_EQ(cache->Get(9).get(), first.get());

  // Fewer total_children → should NOT replace.
  DirProfileSPtr smaller = std::make_shared<DirProfile>(9, 0);
  smaller->Accumulate(300, FileType::kFile, 1024);
  cache->Put(smaller);

  EXPECT_EQ(cache->Get(9).get(), first.get());
}

TEST_F(DirProfileCacheTest, PutReplacesWhenChildCountGreater) {
  auto cache = DirProfileCache::New();

  DirProfileSPtr first = std::make_shared<DirProfile>(9, 0);
  for (int i = 0; i < 10; ++i) {
    first->Accumulate(100 + i, FileType::kFile, 1024);
  }
  cache->Put(first);

  DirProfileSPtr bigger = std::make_shared<DirProfile>(9, 0);
  for (int i = 0; i < 11; ++i) {
    bigger->Accumulate(200 + i, FileType::kFile, 1024);
  }
  cache->Put(bigger);

  EXPECT_EQ(cache->Get(9).get(), bigger.get());
  EXPECT_EQ(cache->Size(), 1u);
}

TEST_F(DirProfileCacheTest, Erase) {
  auto cache = DirProfileCache::New();
  DirProfileSPtr p = std::make_shared<DirProfile>(11, 0);
  cache->Put(p);
  EXPECT_NE(cache->Get(11), nullptr);

  cache->Erase(11);
  EXPECT_EQ(cache->Get(11), nullptr);
  EXPECT_EQ(cache->Size(), 0u);

  // Erasing a non-existent ino is a no-op.
  cache->Erase(11);
  EXPECT_EQ(cache->Size(), 0u);
}

TEST_F(DirProfileCacheTest, SizeAcrossShards) {
  auto cache = DirProfileCache::New();
  // Insert enough entries to land on multiple shards (kShardNum = 32).
  constexpr int kNum = 200;
  for (int i = 0; i < kNum; ++i) {
    DirProfileSPtr p = std::make_shared<DirProfile>(static_cast<Ino>(i + 1), 0);
    cache->Put(p);
  }
  ASSERT_EQ(cache->Size(), static_cast<size_t>(kNum));

  for (int i = 0; i < kNum; ++i) {
    EXPECT_NE(cache->Get(static_cast<Ino>(i + 1)), nullptr);
  }
}

TEST_F(DirProfileCacheTest, CleanExpiredRemovesStaleEntries) {
  auto cache = DirProfileCache::New();

  // Two entries, neither has been "touched" yet (LastActiveTimeS == 0).
  DirProfileSPtr a = std::make_shared<DirProfile>(1, 0);
  DirProfileSPtr b = std::make_shared<DirProfile>(2, 0);
  cache->Put(a);
  cache->Put(b);

  // Touch `b` so its LastActiveTimeS becomes "now".
  b->CheckAndGenWarmupInos(999);
  uint64_t now_s = utils::Timestamp();

  // Cutoff slightly in the past: only never-touched entries (a) qualify.
  cache->CleanExpired(now_s - 1);

  EXPECT_EQ(cache->Get(1), nullptr);
  EXPECT_NE(cache->Get(2), nullptr);
  EXPECT_EQ(cache->Size(), 1u);
}

TEST_F(DirProfileCacheTest, CleanExpiredRemovesAllWhenCutoffInFuture) {
  auto cache = DirProfileCache::New();
  for (int i = 0; i < 10; ++i) {
    DirProfileSPtr p = std::make_shared<DirProfile>(static_cast<Ino>(i + 1), 0);
    p->CheckAndGenWarmupInos(100);  // populate LastActiveTimeS
    cache->Put(p);
  }

  cache->CleanExpired(utils::Timestamp() + 3600);
  EXPECT_EQ(cache->Size(), 0u);
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
