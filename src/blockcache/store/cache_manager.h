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

#ifndef DINGOFS_BLOCKCACHE_STORE_CACHE_MANAGER_H_
#define DINGOFS_BLOCKCACHE_STORE_CACHE_MANAGER_H_

#include <cstdint>
#include <deque>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/store/eviction.h"
#include "blockcache/store/layout.h"
#include "blockcache/store/stats.h"
#include "blockcache/utils/gate.h"

namespace dingofs {
namespace blockcache {

class CacheManager {
 public:
  struct Entry {
    BlockHandle handle;
    bool staged;
  };

  CacheManager(const DiskCacheLayout& layout, uint64_t capacity);

  CacheManager(const CacheManager&) = delete;
  CacheManager& operator=(const CacheManager&) = delete;

  Future<> Start();
  Future<> Shutdown();

  void Insert(BlockHandle handle, uint32_t atime, bool staged);
  std::optional<Entry> Find(BlockHandle handle) const;
  std::optional<Entry> Touch(BlockHandle handle);
  void EraseCache(BlockHandle handle);
  void EraseStage(BlockHandle handle);
  bool Exist(BlockHandle handle) const { return entries_.contains(handle); }

  bool StageFull() const { return stage_full_; }
  bool CacheFull() const { return cache_full_; }
  CacheStats GetStats() const;

 private:
  struct DelItem {
    std::string path;
    uint32_t size;
  };
  struct DeleteBatch {
    std::vector<DelItem> items;
    const char* reason;
  };

  void EraseEntry(BlockIndex::iterator it);
  void CleanupFull(uint64_t want_free_bytes, uint64_t want_free_files);

  Future<> CheckFreeSpace();
  Future<> CleanupExpire();
  void EnterDeleteQueue(const Evicted& to_del, bool expired);
  Future<> DeleteBlocks();

  bool running_ = false;
  DiskCacheLayout layout_;
  EvictionPolicyUPtr policy_;
  BlockIndex entries_;
  std::deque<DeleteBatch> unlink_queue_;
  Gate gate_;

  uint64_t used_bytes_ = 0;
  const uint64_t capacity_;
  bool stage_full_ = false;
  bool cache_full_ = false;
  uint64_t staged_blocks_ = 0;
  uint64_t evicted_blocks_ = 0;
  uint64_t evicted_bytes_ = 0;
  uint64_t expired_blocks_ = 0;
};

using CacheManagerUPtr = std::unique_ptr<CacheManager>;

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_STORE_CACHE_MANAGER_H_
