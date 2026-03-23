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

#include <set>
#include <thread>
#include <vector>

#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_xattr.h"

namespace dingofs {
namespace client {
namespace vfs {

// ─── FhGenerator ─────────────────────────────────────────────────────────────

TEST(FhGeneratorTest, GenFh_MonotonicSequence) {
  // Drain the counter to a known baseline so the sequence is predictable
  // within this test even if other tests ran first.
  uint64_t base = FhGenerator::GetNextFh();
  uint64_t prev = base;

  for (int i = 0; i < 100; ++i) {
    uint64_t fh = FhGenerator::GenFh();
    EXPECT_GT(fh, prev - 1);  // each result must be >= previous
    prev = fh;
  }
}

TEST(FhGeneratorTest, GenFh_Concurrent_NoCollision) {
  constexpr int kThreads = 8;
  constexpr int kPerThread = 1000;

  std::vector<std::vector<uint64_t>> results(kThreads);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&results, t]() {
      results[t].reserve(kPerThread);
      for (int i = 0; i < kPerThread; ++i) {
        results[t].push_back(FhGenerator::GenFh());
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }

  std::set<uint64_t> all_fhs;
  for (const auto& vec : results) {
    for (uint64_t fh : vec) {
      auto [it, inserted] = all_fhs.insert(fh);
      EXPECT_TRUE(inserted) << "Duplicate fh: " << fh;
    }
  }
  EXPECT_EQ(static_cast<int>(all_fhs.size()), kThreads * kPerThread);
}

TEST(FhGeneratorTest, UpdateNextFh_ThenGetNextFh) {
  constexpr uint64_t kTarget = 99999;
  FhGenerator::UpdateNextFh(kTarget);
  EXPECT_EQ(FhGenerator::GetNextFh(), kTarget);
}

// ─── IsSpecialXAttr ──────────────────────────────────────────────────────────

TEST(VfsXAttrTest, IsSpecialXAttr_DirFiles_True) {
  EXPECT_TRUE(IsSpecialXAttr(XATTR_DIR_FILES));
}

TEST(VfsXAttrTest, IsSpecialXAttr_DirSubdirs_True) {
  EXPECT_TRUE(IsSpecialXAttr(XATTR_DIR_SUBDIRS));
}

TEST(VfsXAttrTest, IsSpecialXAttr_DirEntries_True) {
  EXPECT_TRUE(IsSpecialXAttr(XATTR_DIR_ENTRIES));
}

TEST(VfsXAttrTest, IsSpecialXAttr_DirFbytes_True) {
  EXPECT_TRUE(IsSpecialXAttr(XATTR_DIR_FBYTES));
}

TEST(VfsXAttrTest, IsSpecialXAttr_DirPrefix_True) {
  EXPECT_TRUE(IsSpecialXAttr(XATTR_DIR_PREFIX));
}

TEST(VfsXAttrTest, IsSpecialXAttr_UnknownKey_False) {
  EXPECT_FALSE(IsSpecialXAttr("user.myattr"));
  EXPECT_FALSE(IsSpecialXAttr("security.selinux"));
  EXPECT_FALSE(IsSpecialXAttr(""));
}

TEST(VfsXAttrTest, IsSpecialXAttr_WarmupOp_False) {
  // XATTR_WARMUP_OP is NOT in the special-xattr map
  EXPECT_FALSE(IsSpecialXAttr(XATTR_WARMUP_OP));
}

// ─── IsWarmupXAttr ───────────────────────────────────────────────────────────

TEST(VfsXAttrTest, IsWarmupXAttr_WarmupOp_True) {
  EXPECT_TRUE(IsWarmupXAttr(XATTR_WARMUP_OP));
}

TEST(VfsXAttrTest, IsWarmupXAttr_OtherKey_False) {
  EXPECT_FALSE(IsWarmupXAttr("dingofs.warmup.other"));
  EXPECT_FALSE(IsWarmupXAttr(XATTR_DIR_FILES));
  EXPECT_FALSE(IsWarmupXAttr(""));
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
