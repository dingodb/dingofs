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
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/io_buffer.h"
#include "cache/common/s3_client_pool.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using cache::blockcache::Block;
using cache::blockcache::BlockCache;
using cache::blockcache::BlockKey;
using cache::common::CacheGroupNodeOptions;
using cache::common::Errno;
using cache::common::IOBuffer;
using cache::common::S3ClientPool;
using stub::rpcclient::MdsClient;

class CacheGroupNode {
 public:
  virtual ~CacheGroupNode() = default;

  virtual std::string GetListenIp() = 0;

  virtual uint32_t GetListenPort() = 0;

  virtual bool Start() = 0;

  virtual bool Stop() = 0;

  virtual Errno HandleRange(const BlockKey& block_key, uint64_t block_size,
                            uint64_t offset, uint64_t length,
                            butil::IOBuf* buffer) = 0;
};

class CacheGroupNodeImpl : public CacheGroupNode {
 public:
  CacheGroupNodeImpl(CacheGroupNodeOptions options);

  ~CacheGroupNodeImpl() override = default;

  std::string GetListenIp() override;

  uint32_t GetListenPort() override;

  bool Start() override;

  bool Stop() override;

  Errno HandleRange(const BlockKey& block_key, uint64_t block_size,
                    uint64_t offset, uint64_t length,
                    butil::IOBuf* buffer) override;

 private:
  Errno HandleCached(const BlockKey& block_key, uint64_t offset,
                     uint64_t length, butil::IOBuf* buffer);

  Errno HandleMissed(const BlockKey& block_key, uint64_t block_size,
                     uint64_t offset, uint64_t length, butil::IOBuf* buffer);

  bool BuildBlockCache();

 private:
  std::atomic<bool> running_;
  CacheGroupNodeOptions options_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unique_ptr<CacheGroupNodeMember> member_;
  std::unique_ptr<BlockCache> block_cache_;
  std::unique_ptr<AsyncCache> async_cache_;
  std::unique_ptr<CacheGroupNodeMetric> metric_;
  std::unique_ptr<S3ClientPool> s3_client_pool_;
  std::unique_ptr<CacheGroupNodeHeartbeat> heartbeat_;
};

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_CACHE_GROUP_NODE_H_
