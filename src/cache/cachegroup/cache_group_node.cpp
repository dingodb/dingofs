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
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/cachegroup/cache_group_node.h"

#include <butil/iobuf.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <string>

#include "base/math/math.h"
#include "base/string/string.h"
#include "base/time/time.h"
#include "base/timer/timer_impl.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/cachegroup/cache_group_node_heartbeat.h"
#include "cache/cachegroup/cache_group_node_metric.h"
#include "cache/common/config.h"
#include "cache/common/errno.h"
#include "cache/common/local_filesystem.h"
#include "client/common/config.h"
#include "dingofs/cachegroup.pb.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace cachegroup {

using base::math::kKiB;
using base::string::TrimSpace;
using base::time::TimeNow;
using cache::blockcache::BlockKey;
using cache::blockcache::Errno;
using cache::common::LocalFileSystem;
using pb::mds::cachegroup::CacheGroupErrCode_Name;
using pb::mds::cachegroup::CacheGroupMember;
using pb::mds::cachegroup::CacheGroupNodeMeta;
using pb::mds::cachegroup::CacheGroupOk;
using stub::rpcclient::MdsClientImpl;

static void BufferDeleter(void* ptr) { delete[] static_cast<char*>(ptr); }

CacheGroupNodeImpl::CacheGroupNodeImpl(CacheGroupNodeOptions options)
    : running_(false), options_(options) {
  mds_client_ = std::make_shared<MdsClientImpl>();
}

// 9301
// 65536

bool CacheGroupNodeImpl::Start() {
  if (!running_.exchange(true)) {
    bool succ = member_->JoinGroup();
    if (succ) {
      succ = BuildBlockCache();
    }
    return succ;
  }
  return true;
}

bool CacheGroupNodeImpl::Stop() {
  if (running_.exchange(false)) {
  }
  return true;
}

bool CacheGroupNodeImpl::BuildBlockCache() {
  auto member_id = member_->GetMemberId();
  CHECK_GT(member_id, 0);
  auto disks_options = options_.block_cache_options.disks;
  for (auto& options : disks_options) {
    options.cache_dir =
        PathJoin(options.cache_dir, StrFormat("cache-grou-%d", member_id));
  }
  block_cache_ = std::make_shared<BlockCacheImpl>(options_.block_cache_options);
  return block_cache_->Init();
}

Errno CacheGroupNodeImpl::HandleCached(const BlockKey& block_key,
                                       uint64_t offset, uint64_t length,
                                       IOBuffer* buffer) {
  return block_cache_->Range(block_key, offset, length, buffer, false);
}

Errno CacheGroupNodeImpl::HandleMissed(const BlockKey& block_key,
                                       uint64_t block_size, uint64_t offset,
                                       uint64_t length, butil::IOBuf* buffer) {
  std::shared_ptr<S3Client> s3_client;
  auto rc = s3_client_pool_->Get(block_key.fs_id, &s3_client);
  if (rc != Errno::kOk) {
    return rc;
  } else if (length <= option_.max_range_size_kb * kKiB) {
    return s3_client->Range(block_key.StoreKey(), offset, length, buffer);
  }

  rc = s3_client->Range(block_key.StoreKey(), 0, block_size, buffer);
  if (rc == Errno::OK) {
    async_cache_->Cache(block_key, buffer);
  }
  // TODO(trimed)
  return rc;
}

Errno CacheGroupNodeImpl::HandleRange(const BlockKey& block_key,
                                      uint64_t block_size, uint64_t offset,
                                      uint64_t length, butil::IOBuf* buffer) {
  auto rc = HandleCached(block_key, offset, length, buffer);
  if (rc == Errno::OK) {
    metric_->AddCacheHit();
  } else if (rc == Errno::NOT_FOUND) {
    metric_->AddCacheMiss();
    rc = HandleMissed(block_key, block_size, offset, length, buffer);
  }
  return rc;
}

}  // namespace cachegroup
}  // namespace cache
}  // namespace dingofs
