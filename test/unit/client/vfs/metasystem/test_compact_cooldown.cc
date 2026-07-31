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

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include <thread>

#include "client/vfs/metasystem/mds/compact.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

// Defined alongside the processor, in this namespace.
DECLARE_uint32(vfs_compact_cooldown_s);

namespace test {

// CompactProcessor only spawns workers in Init(); construction is cheap, so
// the cooldown gate can be exercised on its own.
class CompactCooldownTest : public ::testing::Test {
 protected:
  void SetUp() override { saved_cooldown_s_ = FLAGS_vfs_compact_cooldown_s; }
  void TearDown() override { FLAGS_vfs_compact_cooldown_s = saved_cooldown_s_; }

 private:
  uint32_t saved_cooldown_s_{0};
};

TEST_F(CompactCooldownTest, IdleProcessorIsNotInCooldown) {
  CompactProcessor processor;
  EXPECT_FALSE(processor.InCooldown());
}

TEST_F(CompactCooldownTest, EnterCooldownSuspendsCompaction) {
  FLAGS_vfs_compact_cooldown_s = 300;

  CompactProcessor processor;
  processor.EnterCooldown();
  EXPECT_TRUE(processor.InCooldown());
}

TEST_F(CompactCooldownTest, ReEnteringKeepsCompactionSuspended) {
  FLAGS_vfs_compact_cooldown_s = 300;

  CompactProcessor processor;
  processor.EnterCooldown();
  processor.EnterCooldown();
  EXPECT_TRUE(processor.InCooldown());
}

TEST_F(CompactCooldownTest, ZeroCooldownDisablesSuspension) {
  FLAGS_vfs_compact_cooldown_s = 0;

  CompactProcessor processor;
  processor.EnterCooldown();
  EXPECT_FALSE(processor.InCooldown());
}

TEST_F(CompactCooldownTest, CompactionResumesAfterCooldownExpires) {
  FLAGS_vfs_compact_cooldown_s = 1;

  CompactProcessor processor;
  processor.EnterCooldown();
  ASSERT_TRUE(processor.InCooldown());

  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  EXPECT_FALSE(processor.InCooldown());
}

// A cooldown shortened at runtime must not keep an older, longer window alive.
TEST_F(CompactCooldownTest, ShortenedFlagAppliesToTheNextCooldown) {
  FLAGS_vfs_compact_cooldown_s = 300;

  CompactProcessor processor;
  processor.EnterCooldown();
  ASSERT_TRUE(processor.InCooldown());

  FLAGS_vfs_compact_cooldown_s = 0;
  processor.EnterCooldown();
  EXPECT_FALSE(processor.InCooldown());
}

}  // namespace test
}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
