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

#include <butil/binary_printer.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace cache {

DEFINE_string(group_name, "default", "Which group this cache node belongs to");
DEFINE_string(listen_ip, "127.0.0.1",
              "IP address to listen on for this cache group node");
DEFINE_uint32(listen_port, 9300, "Port to listen on for this cache group node");
DEFINE_uint32(group_weight, 100,
              "Weight of this cache group node, used for consistent hashing");
DEFINE_uint32(max_range_size_kb, 128,
              "Retrive the whole block if length of range request is larger "
              "than this value");
DEFINE_string(metadata_filepath, "/var/log/cache_group_meta",
              "Filepath to store metadata of cache group node");  // Use dir
DEFINE_uint32(send_heartbeat_interval_ms, 1000,
              "Interval to send heartbeat to MDS in milliseconds");

CacheGroupNodeImpl::CacheGroupNodeImpl(CacheGroupNodeOption option)
    : running_(false),
      option_(option),
      mds_base_(std::make_shared<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_shared<stub::rpcclient::MdsClientImpl>()),
      member_(std::make_shared<CacheGroupNodeMemberImpl>(option, mds_client_)),
      heartbeat_(
          std::make_unique<CacheGroupNodeHeartbeatImpl>(member_, mds_client_)),
      storage_pool_(std::make_shared<StoragePoolImpl>(mds_client_)) {}

Status CacheGroupNodeImpl::Start() {
  CHECK_NOTNULL(mds_base_);
  CHECK_NOTNULL(mds_client_);
  CHECK_NOTNULL(member_);
  CHECK_NOTNULL(heartbeat_);
  CHECK_NOTNULL(storage_pool_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node is starting...";

  auto rc = mds_client_->Init(option_.mds_option, mds_base_.get());
  if (rc != PBFSStatusCode::OK) {
    LOG(ERROR) << "Init mds client failed: rc = "
               << pb::mds::FSStatusCode_Name(rc);
    return Status::Internal("init mds client failed");
  }

  auto status = member_->JoinGroup();
  if (!status.ok()) {
    LOG(ERROR) << "Join node to cache group failed: " << status.ToString();
    return status;
  }

  // Cache directory name depends node member uuid, so init after join group
  status = InitBlockCache();
  if (!status.ok()) {
    LOG(ERROR) << "Init block cache failed: " << status.ToString();
    return status;
  }

  async_cacher_ = std::make_unique<AsyncCacherImpl>(block_cache_);
  status = async_cacher_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start async cacher failed: " << status.ToString();
    return status;
  }

  heartbeat_->Start();

  LOG(INFO) << "Cache group node is up.";

  CHECK_RUNNING("Cache group node");
  return Status::OK();
}

Status CacheGroupNodeImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Cache group node is shutting down...";

  heartbeat_->Shutdown();

  Status status = member_->LeaveGroup();
  if (!status.ok()) {
    return status;
  }

  status = async_cacher_->Shutdown();
  if (!status.ok()) {
    return status;
  }

  status = block_cache_->Shutdown();
  if (!status.ok()) {
    return status;
  }

  LOG(INFO) << "Cache group node is down.";

  CHECK_DOWN("Cache group node");
  return status;
}

void CacheGroupNodeImpl::RewriteCacheDir() {
  auto member_uuid = member_->GetMemberUuid();
  CHECK(!member_uuid.empty());
  auto& disk_cache_options = option_.block_cache_option.disk_cache_options;
  for (auto& option : disk_cache_options) {
    option.cache_dir = RealCacheDir(option.cache_dir, member_uuid);
  }
}

Status CacheGroupNodeImpl::InitBlockCache() {
  RewriteCacheDir();

  block_cache_ = std::make_shared<BlockCacheImpl>(option_.block_cache_option,
                                                  storage_pool_);

  return block_cache_->Start();
}

Status CacheGroupNodeImpl::Put(ContextSPtr ctx, const BlockKey& key,
                               const Block& block, PutOption option) {
  Status status;

  status = block_cache_->Put(ctx, key, block, option);
  return status;
}

Status CacheGroupNodeImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 RangeOption option) {
  Status status;

  status = RangeCachedBlock(ctx, key, offset, length, buffer, option);
  if (status.ok()) {
    // do nothing
  } else if (status.IsNotFound()) {
    status = RangeStorage(ctx, key, offset, length, buffer, option);
  }
  return status;
}

Status CacheGroupNodeImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block, CacheOption option) {
  Status status;

  status = block_cache_->Cache(ctx, key, block, option);
  return status;
}

Status CacheGroupNodeImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                    size_t length, PrefetchOption option) {
  Status status;

  status = block_cache_->Prefetch(ctx, key, length, option);
  return status;
}

Status CacheGroupNodeImpl::RangeCachedBlock(ContextSPtr ctx,
                                            const BlockKey& key, off_t offset,
                                            size_t length, IOBuffer* buffer,
                                            RangeOption option) {
  option.retrive = false;
  auto status = block_cache_->Range(ctx, key, offset, length, buffer, option);
  if (status.ok()) {
    // CacheGroupNodeMetric::GetInstance().AddCacheHit();
  } else {
    // CacheGroupNodeMetric::GetInstance().AddCacheMiss();
  }
  return status;
}

Status CacheGroupNodeImpl::RangeStorage(ContextSPtr ctx, const BlockKey& key,
                                        off_t offset, size_t length,
                                        IOBuffer* buffer, RangeOption option) {
  Status status;

  StorageSPtr storage;
  status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    return status;
  }

  // Retrive range of block: unknown block size or unreach max_range_size
  auto block_size = option.block_size;
  if (block_size == 0 || length <= FLAGS_max_range_size_kb * kKiB) {
    status = storage->Range(ctx, key, offset, length, buffer);
    return status;
  }

  // Retrive the whole block
  IOBuffer block;
  status = storage->Range(ctx, key, offset, block_size, &block);
  if (!status.ok()) {
    return status;
  }

  butil::IOBuf piecs;
  block.IOBuf().append_to(&piecs, length, offset);
  *buffer = IOBuffer(piecs);

  async_cacher_->AsyncCache(ctx, key, block);

  return status;
}

}  // namespace cache
}  // namespace dingofs
