/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-09-04
 * Author: Jingli Chen (Wine93)
 */

#include <memory>

#include "cache/blockcache/disk_cache_layout.h"
#include "glog/logging.h"
#include "gtest/gtest.h"

namespace dingofs {
namespace cache {

class DiskCacheLayoutTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

TEST_F(DiskCacheLayoutTest, Basic) {
  auto layout = std::make_shared<DiskCacheLayout>("/mnt/data");
  ASSERT_EQ(layout->GetRootDir(), "/mnt/data");
  ASSERT_EQ(layout->GetStageDir(), "/mnt/data/stage");
  ASSERT_EQ(layout->GetCacheDir(), "/mnt/data/cache");
  ASSERT_EQ(layout->GetProbeDir(), "/mnt/data/probe");
  ASSERT_EQ(layout->GetLockPath(), "/mnt/data/.lock");
  ASSERT_EQ(layout->GetStagePath(BlockKey(1, 1, 1, 1, 0)),
            "/mnt/data/stage/blocks/0/0/1_1_1_1_0");
  ASSERT_EQ(layout->GetCachePath(BlockKey(1, 1, 1, 1, 0)),
            "/mnt/data/cache/blocks/0/0/1_1_1_1_0");
}

}  // namespace cache
}  // namespace dingofs
