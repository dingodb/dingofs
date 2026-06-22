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

/*
 * Project: DingoFS
 * Created Date: 2025-05-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_

#include "cache/api/block_cache.h"
#include "cache/local/cache_store.h"
#include "cache/common/storage_client.h"
#include "cache/iutil/bthread.h"
#include "cache/iutil/inflight_tracker.h"
#include "common/blockaccess/block_accesser.h"

namespace dingofs {
namespace cache {

class TierBlockCache final : public BlockCache {
 public:
  explicit TierBlockCache(StorageClientUPtr storage_client);
  explicit TierBlockCache(blockaccess::BlockAccesser* block_accesser);

  Status Start() override;
  Status Shutdown() override;

  Status Put(BlockHandle handle, IOBuffer block,
             PutOption option = {}) override;
  Status Range(BlockHandle handle, off_t offset, size_t length,
               IOBuffer* buffer, RangeOption option = {}) override;
  Status Cache(BlockHandle handle, IOBuffer block,
               CacheOption option = {}) override;
  Status Prefetch(BlockHandle handle, size_t length,
                  PrefetchOption option = {}) override;

  void AsyncPut(BlockHandle handle, IOBuffer block, AsyncCallback cb,
                PutOption option = {}) override;
  void AsyncRange(BlockHandle handle, off_t offset, size_t length,
                  IOBuffer* buffer, AsyncCallback cb,
                  RangeOption option = {}) override;
  void AsyncCache(BlockHandle handle, IOBuffer block, AsyncCallback cb,
                  CacheOption option = {}) override;
  void AsyncPrefetch(BlockHandle handle, size_t length, AsyncCallback cb,
                     PrefetchOption option = {}) override;

  bool IsEnabled() const override {
    return remote_block_cache_->IsEnabled() || local_block_cache_->IsEnabled();
  }

  bool EnableStage() const override {
    return EnableLocalStage() || EnableRemoteStage();
  }

  bool EnableCache() const override {
    return EnableLocalCache() || EnableRemoteCache();
  }

  bool IsCached(const BlockHandle& handle) const override {
    return local_block_cache_->IsCached(handle) ||
           remote_block_cache_->IsCached(handle);
  }

  bool Dump(Json::Value& value) const override {
    return local_block_cache_->Dump(value) && remote_block_cache_->Dump(value);
  }

 private:
  BlockCache* GetSelfPtr() { return this; }

  bool EnableLocalStage() const { return local_block_cache_->EnableStage(); }
  bool EnableLocalCache() const { return local_block_cache_->EnableCache(); }
  bool EnableRemoteStage() const { return remote_block_cache_->EnableStage(); }
  bool EnableRemoteCache() const { return remote_block_cache_->EnableCache(); }

  // Linearize a possibly multi-block IOBuf into a single contiguous backing
  // block, required by S3 upload (storage_client → IOBuffer::Fetch1()).
  IOBuffer CopyBlock(const IOBuffer& block);
  void FillGroupCache(const BlockHandle& handle, const IOBuffer& block);

  // Returns the effective tier for an operation. If the caller already pinned
  // a tier (kLocal or kRemote) it is honored as-is. Otherwise, small blocks
  // are automatically pinned to local when FLAGS_small_block_size_kb > 0
  // and local cache is enabled.
  CacheTier ResolveTier(const BlockHandle& handle, CacheTier hint) const;

  // The behavior of local block cache is same as remote block cache,
  // the biggest difference is that the local block cache will read/write data
  // from/to the local disk, while the remote block cache will read/write data
  // from/to the remote cache group node.
  std::atomic<bool> running_;
  StorageClientUPtr storage_client_;
  BlockCacheUPtr local_block_cache_;
  BlockCacheUPtr remote_block_cache_;
  iutil::InflightTrackerSPtr cache_tracker_;
  iutil::InflightTrackerSPtr prefetch_tracker_;
  iutil::BthreadJoinerUPtr joiner_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TIERCACHE_TIER_BLOCK_CACHE_H_
