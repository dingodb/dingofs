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
 * Created Date: 2026-06-21
 * Author: AI
 */

#include "cache/blockcache/disk_cache_layout.h"

#include <absl/strings/match.h>
#include <gtest/gtest.h>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

TEST(DiskCacheLayoutTest, Directories) {
  DiskCacheLayout layout(2, "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20");

  EXPECT_EQ(layout.CacheIndex(), 2);
  EXPECT_EQ(layout.GetRootDir(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20");
  EXPECT_EQ(layout.GetStageDir(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20/stage");
  EXPECT_EQ(layout.GetCacheDir(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20/cache");
  EXPECT_EQ(layout.GetProbeDir(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20/probe");
  EXPECT_EQ(layout.GetDetectPath(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20/.detect");
  EXPECT_EQ(layout.GetLockPath(),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20/.lock");
}

TEST(DiskCacheLayoutTest, BlockPaths) {
  DiskCacheLayout layout(0, "/root");
  BlockHandle handle(4098, BlockKey(4098, 1, 4194304));

  // StoreKey buckets by id: blocks/{id/1e6}/{id/1e3}/{filename}
  EXPECT_EQ(handle.StoreKey(), "blocks/0/4/4098_1_4194304");
  EXPECT_EQ(layout.GetStagePath(handle),
            "/root/stage/4098/blocks/0/4/4098_1_4194304");
  EXPECT_EQ(layout.GetCachePath(handle),
            "/root/cache/blocks/0/4/4098_1_4194304");
}

TEST(DiskCacheLayoutTest, RealCacheDir) {
  EXPECT_EQ(RealCacheDir("/data/cache", "950c9813-ea26-4726-96fd-383b0cd22b20"),
            "/data/cache/950c9813-ea26-4726-96fd-383b0cd22b20");
}

TEST(DiskCacheLayoutTest, TempFilepath) {
  std::string path = "/root/cache/blocks/0/4/4098_1_4194304";
  std::string temp = TempFilepath(path);

  EXPECT_TRUE(IsTempFilepath(temp));
  EXPECT_TRUE(absl::StartsWith(temp, path + "."));
  EXPECT_TRUE(absl::EndsWith(temp, ".tmp"));

  EXPECT_FALSE(IsTempFilepath(path));
  EXPECT_FALSE(IsTempFilepath("/root/cache/blocks/0/4/4098_1_4194304.bak"));
}

}  // namespace cache
}  // namespace dingofs
