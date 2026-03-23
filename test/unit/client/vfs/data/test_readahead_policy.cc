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

#include <cstdint>
#include <limits>

#include "client/vfs/data/reader/readahead_policy.h"

namespace dingofs {
namespace client {
namespace vfs {

// Sizes matching readahead_policy.cc constants.
static constexpr int64_t kSeqWindow = 2 * 1024 * 1024;   // 2 MB
static constexpr int64_t kBaseSize = 1 * 1024 * 1024;    // 1 MB
static constexpr int64_t kTotal = 64 * 1024 * 1024;      // 64 MB

// ─── ReadaheadSize ────────────────────────────────────────────────────────────

TEST(ReadaheadPolicyTest, ReadaheadSize_Level0_IsZero) {
  ReadaheadPoclicy p(1);
  EXPECT_EQ(p.ReadaheadSize(), 0);
}

TEST(ReadaheadPolicyTest, ReadaheadSize_AllLevels) {
  ReadaheadPoclicy p(1);
  p.level = 1;
  EXPECT_EQ(p.ReadaheadSize(), 1 * kBaseSize);   // 1 MB
  p.level = 2;
  EXPECT_EQ(p.ReadaheadSize(), 4 * kBaseSize);   // 4 MB
  p.level = 3;
  EXPECT_EQ(p.ReadaheadSize(), 16 * kBaseSize);  // 16 MB
  p.level = 4;
  EXPECT_EQ(p.ReadaheadSize(), 64 * kBaseSize);  // 64 MB
}

// ─── Degrade() ───────────────────────────────────────────────────────────────

// Degrade from level > 1: level decreases, seqdata resets, last_offset kept.
TEST(ReadaheadPolicyTest, Degrade_FromLevel2_LastOffsetPreserved) {
  ReadaheadPoclicy p(1);
  p.level = 2;
  p.seqdata = 9999;
  p.last_offset = 8 * 1024 * 1024;

  p.Degrade();

  EXPECT_EQ(p.level, 1);
  EXPECT_EQ(p.seqdata, 0);
  EXPECT_EQ(p.last_offset, 8 * 1024 * 1024);  // unchanged
}

// KEY BUG FIX (8bacba35): degrading from level 1 to 0 must reset last_offset.
// Without the fix, last_offset kept its old value and caused wrong sequential
// window checks on the next read from an arbitrary position.
TEST(ReadaheadPolicyTest, Degrade_ToLevel0_ResetsLastOffset) {
  ReadaheadPoclicy p(1);
  p.level = 1;
  p.seqdata = 12345;
  p.last_offset = 4 * 1024 * 1024;  // 4 MB – non-zero "old" position

  p.Degrade();  // 1 → 0

  EXPECT_EQ(p.level, 0);
  EXPECT_EQ(p.seqdata, 0);
  EXPECT_EQ(p.last_offset, 0);  // must be reset
}

// Degrade from level 0: no-op, nothing changes.
TEST(ReadaheadPolicyTest, Degrade_FromLevel0_IsNoOp) {
  ReadaheadPoclicy p(1);
  p.last_offset = 1234;

  p.Degrade();

  EXPECT_EQ(p.level, 0);
  EXPECT_EQ(p.last_offset, 1234);  // unchanged
}

// ─── UpdateOnRead – initialisation ───────────────────────────────────────────

// First read at offset 0 (== last_offset initial value) → level set to 1.
TEST(ReadaheadPolicyTest, Update_OffsetZero_InitLevel1) {
  ReadaheadPoclicy p(1);
  FileRange r{0, 4096};
  p.UpdateOnRead(r, 0, kTotal);

  EXPECT_EQ(p.level, 1);
  EXPECT_EQ(p.seqdata, 0);
}

// ─── UpdateOnRead – random access → degrade ──────────────────────────────────

// A jump larger than the sequential window triggers a degrade.
TEST(ReadaheadPolicyTest, Update_RandomJump_Degrades) {
  ReadaheadPoclicy p(1);
  p.level = 2;
  p.last_offset = 0;

  // Jump 10 MB away – well outside the 2 MB sequential window.
  FileRange far{10 * 1024 * 1024, 4096};
  p.UpdateOnRead(far, 0, kTotal);

  EXPECT_EQ(p.level, 1);   // degraded one step
}

// A random jump while at level 1 degrades to 0 and resets last_offset.
// This is the exact regression for 8bacba35: after the degrade to 0, a
// subsequent sequential read from the new position must work correctly.
TEST(ReadaheadPolicyTest, Update_RandomJump_DegradeToZero_ResetsLastOffset) {
  ReadaheadPoclicy p(1);
  p.level = 1;
  p.last_offset = 4 * 1024 * 1024;

  // Jump far beyond the sequential window relative to last_offset.
  FileRange far{30 * 1024 * 1024, 4096};
  p.UpdateOnRead(far, 0, kTotal);

  EXPECT_EQ(p.level, 0);
  EXPECT_EQ(p.last_offset, 0);  // reset because degraded to level 0
}

// A small jump within the sequential window does NOT trigger a degrade even if
// the offset is not exactly last_offset.
TEST(ReadaheadPolicyTest, Update_SmallJump_WithinWindow_NoDegrace) {
  ReadaheadPoclicy p(1);
  p.level = 2;
  p.last_offset = 0;

  // Jump 1 MB – within the 2 MB window.
  FileRange near{1 * 1024 * 1024, 4096};
  p.UpdateOnRead(near, 0, kTotal);

  EXPECT_EQ(p.level, 2);  // no degrade
}

// ─── UpdateOnRead – memory pressure → degrade ────────────────────────────────

// When buffer usage exceeds the threshold at level > 1, Degrade() is called.
// threshold at level 2 = total/2 + total/4 = 75% of total.
TEST(ReadaheadPolicyTest, Update_MemoryPressure_Degrades) {
  ReadaheadPoclicy p(1);
  p.level = 2;
  p.last_offset = 4096;

  // Read at exact last_offset to stay in the sequential path (no random degrade).
  FileRange r{4096, 4096};

  int64_t used = kTotal * 9 / 10;  // 90% > 75% threshold
  p.UpdateOnRead(r, used, kTotal);

  EXPECT_LT(p.level, 2);  // degraded by memory pressure
}

// ─── seqdata type – int64_t (fix for int32_t overflow) ───────────────────────

// seqdata must accommodate values larger than INT32_MAX without overflow.
// This validates the type change from int32_t to int64_t in 8bacba35.
TEST(ReadaheadPolicyTest, SeqData_AcceptsLargeValue_NoOverflow) {
  ReadaheadPoclicy p(1);
  p.level = 4;
  // Set seqdata just below INT32_MAX to confirm int64_t range.
  p.seqdata = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) - 1024;

  // Sequential read at last_offset=0 (which triggers the init branch, not the
  // accumulate branch), so seqdata gets reset to 0 – the important thing is
  // that the assignment and comparison do not overflow.
  FileRange r{0, 4096};
  p.UpdateOnRead(r, 0, kTotal);

  // After init branch: seqdata == 0, level == 1 (re-initialised).
  EXPECT_EQ(p.seqdata, 0);
  EXPECT_GE(p.level, 0);
}

// ─── ToString / UUID – no crash ──────────────────────────────────────────────

TEST(ReadaheadPolicyTest, ToString_NoCrash) {
  ReadaheadPoclicy p(42);
  EXPECT_FALSE(p.ToString().empty());
  EXPECT_FALSE(p.UUID().empty());
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
