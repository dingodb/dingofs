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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
#define DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_

#include "cache/blockcache/block_cache.h"
#include "cache/common/context.h"
#include "cache/iutil/bthread.h"
#include "cache/remotecache/block_fetcher.h"
#include "cache/remotecache/upstream.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

struct RemoteBlockCacheVarsCollector {
  static double GetCacheHitRatio(void* arg) {
    auto* vars = reinterpret_cast<RemoteBlockCacheVarsCollector*>(arg);
    const double hit = vars->cache_hit_count_per_second.get_value();
    const double miss = vars->cache_miss_count_per_second.get_value();
    const double total = hit + miss;
    return total > 0 ? (hit * 100.0 / total) : 0.0;
  }

  inline static const std::string prefix = "dingofs_remote_cache_";
  bvar::Adder<uint64_t> cache_hit_count{prefix, "hit_count"};
  bvar::Adder<uint64_t> cache_miss_count{prefix, "miss_count"};
  bvar::PerSecond<bvar::Adder<uint64_t>> cache_hit_count_per_second{
      prefix, "hit_count_per_second", &cache_hit_count, 1};
  bvar::PerSecond<bvar::Adder<uint64_t>> cache_miss_count_per_second{
      prefix, "miss_count_per_second", &cache_miss_count, 1};
  bvar::PassiveStatus<double> cache_hit_ratio{prefix, "hit_ratio",
                                              GetCacheHitRatio, this};
};

using RemoteBlockCacheVarsCollectorUPtr =
    std::unique_ptr<RemoteBlockCacheVarsCollector>;

class RemoteBlockCacheImpl final : public BlockCache {
 public:
  explicit RemoteBlockCacheImpl(StorageClient* storage_client);

  Status Start() override;
  Status Shutdown() override;

  Status Put(ContextSPtr ctx, const BlockKey& key, const Block& block,
             PutOption option) override;
  Status Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, RangeOption option) override;
  Status Cache(ContextSPtr ctx, const BlockKey& key, const Block& block,
               CacheOption option) override;
  Status Prefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                  PrefetchOption option) override;

  void AsyncPut(ContextSPtr ctx, const BlockKey& key, const Block& block,
                AsyncCallback cb, PutOption option) override;
  void AsyncRange(ContextSPtr ctx, const BlockKey& key, off_t offset,
                  size_t length, IOBuffer* buffer, AsyncCallback cb,
                  RangeOption option) override;
  void AsyncCache(ContextSPtr ctx, const BlockKey& key, const Block& block,
                  AsyncCallback cb, CacheOption option) override;
  void AsyncPrefetch(ContextSPtr ctx, const BlockKey& key, size_t length,
                     AsyncCallback cb, PrefetchOption option) override;

  // We gurantee that cache node is always enable stage and cache.
  bool IsEnabled() const override { return !FLAGS_cache_group.empty(); }
  bool EnableStage() const override { return IsEnabled(); }
  bool EnableCache() const override { return IsEnabled(); }
  bool IsCached(const BlockKey&) const override { return IsEnabled(); }

 private:
  BlockCache* GetSelfPtr() { return this; }

  std::atomic<bool> running_;
  UpstreamUPtr upstream_;
  CacheRetrieverUPtr retriever_;
  iutil::BthreadJoinerUPtr joiner_;
  RemoteBlockCacheVarsCollectorUPtr vars_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_REMOTECACHE_REMOTE_BLOCK_CACHE_H_
