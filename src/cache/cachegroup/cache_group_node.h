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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_

#include <brpc/server.h>
#include <bthread/execution_queue.h>
#include <butil/iobuf.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/cachegroup/async_cache.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_member.h"
#include "cache/cachegroup/cache_group_node_metric.h"
#include "cache/storage/buffer.h"
#include "cache/utils/data_accesser_pool.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

class CacheGroupNode {
 public:
  virtual ~CacheGroupNode() = default;

  virtual Status Start() = 0;
  virtual Status Stop() = 0;

  virtual std::string GetListenIp() = 0;
  virtual uint32_t GetListenPort() = 0;

  virtual Status HandleRangeRequest(const blockcache::BlockKey& block_key,
                                    size_t block_size, off_t offset,
                                    size_t length,
                                    storage::IOBuffer* buffer) = 0;
};

class CacheGroupNodeImpl : public CacheGroupNode {
 public:
  explicit CacheGroupNodeImpl(CacheGroupNodeOption option);
  ~CacheGroupNodeImpl() override = default;

  Status Start() override;
  Status Stop() override;

  std::string GetListenIp() override;
  uint32_t GetListenPort() override;

  Status HandleRangeRequest(const blockcache::BlockKey& block_key,
                            size_t block_size, off_t offset, size_t length,
                            storage::IOBuffer* buffer) override;

 private:
  void RewriteCacheDir();
  Status InitBlockCache();

  Status HandleBlockCached(const blockcache::BlockKey& block_key, off_t offset,
                           size_t length, storage::IOBuffer* buffer);
  Status HandleBlockMissed(const blockcache::BlockKey& block_key,
                           size_t block_size, off_t offset, size_t length,
                           storage::IOBuffer* buffer);

 private:
  std::atomic<bool> running_;
  CacheGroupNodeOption option_;
  std::shared_ptr<stub::rpcclient::MDSBaseClient> mds_base_;
  std::shared_ptr<stub::rpcclient::MdsClient> mds_client_;
  std::shared_ptr<blockcache::BlockCache> block_cache_;  // inited by later
  std::unique_ptr<utils::DataAccesserPool> data_accesser_pool_;
  std::unique_ptr<AsyncCache> async_cache_;  // inited by later
  std::shared_ptr<CacheGroupNodeMember> member_;
  std::shared_ptr<CacheGroupNodeMetric> metric_;
  std::unique_ptr<CacheGroupNodeHeartbeat> heartbeat_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
