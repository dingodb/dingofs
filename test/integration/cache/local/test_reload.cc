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
 * Created Date: 2026-06-22
 * Author: AI
 */

#include <fstream>

#include "cache/local/disk_cache_layout.h"
#include "common/block/tensor_key.h"
#include "test/integration/cache/local/fixture.h"

namespace dingofs {
namespace cache {
namespace integration {

// Blocks cached on disk survive a cache restart: a new TierBlockCache over the
// same cache_dir recovers them via the DiskCacheLoader.
TEST_F(LocalCacheRawTest, ReloadRecoversCachedBlocks) {
  ASSERT_TRUE(client_.Open(storage_dir_).ok());
  auto* cache = client_.cache();

  constexpr uint32_t kSize = 256 * 1024;
  for (uint64_t id = 1; id <= 3; ++id) {
    ASSERT_TRUE(cache
                    ->Cache(MakeHandle(kFsId, id, 0, kSize),
                            MakeBlock(PatternFor(id, 0, kSize)))
                    .ok());
  }

  client_.ShutdownCacheOnly();
  ASSERT_TRUE(client_.ReopenCache().ok());
  cache = client_.cache();

  for (uint64_t id = 1; id <= 3; ++id) {
    auto h = MakeHandle(kFsId, id, 0, kSize);
    ASSERT_TRUE(WaitUntil([&] { return cache->IsCached(h); }))
        << "block id=" << id << " not recovered after reload";
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(
        cache
            ->Range(h, 0, kSize, &buf,
                    {.retrieve_storage = false, .block_whole_length = kSize})
            .ok());
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize));
  }
}

// With a short expiry, a cached block is dropped from the cache after its TTL.
TEST_F(LocalCacheRawTest, ExpiredBlockIsEvicted) {
  FLAGS_cache_expire_s = 1;
  FLAGS_cache_cleanup_expire_interval_ms = 100;
  ASSERT_TRUE(client_.Open(storage_dir_).ok());
  auto* cache = client_.cache();

  constexpr uint32_t kSize = 64 * 1024;
  auto h = MakeHandle(kFsId, 1, 0, kSize);
  ASSERT_TRUE(cache->Cache(h, MakeBlock(PatternFor(1, 0, kSize))).ok());

  // Sleep past the TTL without probing IsCached/Range (a hit refreshes the
  // entry's atime and would keep it alive), then verify a single no-reflow
  // read misses.
  std::this_thread::sleep_for(std::chrono::seconds(3));

  IOBuffer buf = MakeReadBuf(kSize);
  EXPECT_TRUE(
      cache
          ->Range(h, 0, kSize, &buf,
                  {.retrieve_storage = false, .block_whole_length = kSize})
          .IsNotFound());
}

// Under a small cache budget, writing far more than fits still serves every
// block correctly (evicted blocks reflow from the backend).
TEST_F(LocalCacheRawTest, EvictionStillServesViaReflow) {
  FLAGS_cache_size_mb = 8;  // ~2 x 4MiB blocks fit
  ASSERT_TRUE(client_.Open(storage_dir_).ok());
  auto* cache = client_.cache();

  constexpr uint32_t kSize = 4u * 1024 * 1024;
  constexpr uint64_t kBlocks = 8;
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    ASSERT_TRUE(cache
                    ->Put(MakeHandle(kFsId, id, 0, kSize),
                          MakeBlock(PatternFor(id, 0, kSize)),
                          {.writeback = true})
                    .ok());
  }
  for (uint64_t id = 1; id <= kBlocks; ++id) {
    auto h = MakeHandle(kFsId, id, 0, kSize);
    IOBuffer buf = MakeReadBuf(kSize);
    ASSERT_TRUE(
        cache
            ->Range(h, 0, kSize, &buf,
                    {.retrieve_storage = true, .block_whole_length = kSize})
            .ok())
        << "id=" << id;
    EXPECT_EQ(ReadAll(buf), PatternFor(id, 0, kSize)) << "id=" << id;
  }
}

// An orphan stage file -- one whose cache hardlink is lost (Link() failed in
// Stage(), or the hardlink was removed after a promote-then-evict) -- must
// still be uploaded after a reload. The re-upload used to read only the cache
// path, get NotFound and abort as terminal, leaking the stage file and its
// cache_blocks/cache_bytes accounting forever; Load now falls back to the
// stage file.
TEST_F(LocalCacheRawTest, OrphanStageBlockDrainsOnReload) {
  constexpr uint32_t kSize = 64 * 1024;
  auto h = MakeHandle(kFsId, 1, 0, kSize);
  auto content = PatternFor(1, 0, kSize);

  // Craft the orphan directly under the on-disk layout the loader walks: a
  // stage file with no cache hardlink.
  DiskCacheLayout layout(0, RealCacheDir(FLAGS_cache_dir,
                                         FLAGS_cache_dir_uuid));
  auto stage_path = layout.GetStagePath(h);
  std::filesystem::create_directories(
      std::filesystem::path(stage_path).parent_path());
  {
    std::ofstream ofs(stage_path, std::ios::binary);
    ofs.write(content.data(), content.size());
  }

  ASSERT_TRUE(client_.Open(storage_dir_).ok());

  // The loader re-registers the orphan and re-enqueues its upload, which must
  // succeed from the stage file and then remove it.
  EXPECT_TRUE(WaitUntil([&] { return client_.StorageHas(h); }))
      << "orphan stage block never uploaded";
  EXPECT_TRUE(WaitUntil([&] { return !std::filesystem::exists(stage_path); }))
      << "stage file not removed after upload";
  EXPECT_EQ(client_.ReadStorageFile(h), content);
}

// TensorKey-handle blocks also survive a disk-cache reload (distinct StoreKey
// layout "tensor/<hh>/...").
TEST_F(LocalCacheRawTest, TensorKeyReloadRecovers) {
  ASSERT_TRUE(client_.Open(storage_dir_).ok());
  auto* cache = client_.cache();

  BlockHandle h(TensorKey("model-x", 2, 0, "cafebabe1234", "float32"));
  auto content = PatternFor(0xABC, 0, 65536);
  ASSERT_TRUE(cache->Cache(h, MakeBlock(content)).ok());
  ASSERT_TRUE(cache->IsCached(h));

  client_.ShutdownCacheOnly();
  ASSERT_TRUE(client_.ReopenCache().ok());
  cache = client_.cache();

  ASSERT_TRUE(WaitUntil([&] { return cache->IsCached(h); }));
  IOBuffer buf = MakeReadBuf(content.size());
  ASSERT_TRUE(cache
                  ->Range(h, 0, content.size(), &buf,
                          {.retrieve_storage = false,
                           .block_whole_length = content.size()})
                  .ok());
  EXPECT_EQ(ReadAll(buf), content);
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
