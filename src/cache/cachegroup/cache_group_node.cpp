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
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/cachegroup/stub.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "dingofs/mds.pb.h"
#include "metrics/cache/cache_group_node_metric.h"
#include "options/cache/stub.h"

namespace dingofs {
namespace cache {

DEFINE_string(group_name, "", "Which group this cache node belongs to");
DEFINE_validator(group_name, Helper::NonEmptyString);

DEFINE_string(listen_ip, "127.0.0.1",
              "IP address to listen on for this cache group node");
DEFINE_uint32(listen_port, 9300, "Port to listen on for this cache group node");
DEFINE_uint32(group_weight, 100,
              "Weight of this cache group node, used for consistent hashing");
DEFINE_string(metadata_filepath, "/var/log/cache_group_meta",
              "Filepath to store metadata of cache group node");  // Use dir
DEFINE_uint32(max_range_size_kb, 128,
              "Retrive the whole block if length of range request is larger "
              "than this value");

static const std::string kModule = kCacheGroupNodeModule;

CacheGroupNodeImpl::CacheGroupNodeImpl(CacheGroupNodeOption option)
    : running_(false),
      option_(option),
      mds_base_(std::make_shared<stub::rpcclient::MDSBaseClient>()),
      mds_client_(std::make_shared<stub::rpcclient::MdsClientImpl>()),
      member_(std::make_shared<CacheGroupNodeMemberImpl>(option, mds_client_)),
      heartbeat_(
          std::make_unique<CacheGroupNodeHeartbeatImpl>(member_, mds_client_)) {
  if (FLAGS_filesystem_mds_version == "v1") {
    storage_pool_ =
        std::make_shared<StoragePoolImpl>(NewV1GetStorageInfoFunc(mds_client_));
  } else if (FLAGS_filesystem_mds_version == "v2") {
    storage_pool_ = std::make_shared<StoragePoolImpl>(
        NewV2GetStorageInfoFunc(fLS::FLAGS_mdsv2_rpc_addr));
  } else {
    CHECK(false) << "Unsupport filesystem mds version: "
                 << FLAGS_filesystem_mds_version;
  }
}

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

  running_ = true;

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
    LOG(ERROR) << "Leave cache group failed: " << status.ToString();
    return status;
  }

  status = async_cacher_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown async cacher failed: " << status.ToString();
    return status;
  }

  status = block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown block cache failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Cache group node is down.";

  CHECK_DOWN("Cache group node");
  return status;
}

void CacheGroupNodeImpl::RewriteCacheDir() {
  auto member_uuid = member_->GetMemberUuid();
  CHECK(!member_uuid.empty()) << "Member UUID should not be empty";
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
  CHECK_RUNNING("Cache group node");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP(kLocalPut);
  status = block_cache_->Put(ctx, key, block, option);

  return status;
}

Status CacheGroupNodeImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                 off_t offset, size_t length, IOBuffer* buffer,
                                 RangeOption option) {
  CHECK_RUNNING("Cache group node");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  status = RangeCachedBlock(ctx, timer, key, offset, length, buffer, option);
  if (status.ok()) {
    // do nothing
  } else if (status.IsNotFound()) {
    status = RangeStorage(ctx, timer, key, offset, length, buffer, option);
  }
  return status;
}

Status CacheGroupNodeImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block, CacheOption option) {
  CHECK_RUNNING("Cache group node");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP(kLocalCache);
  status = block_cache_->Cache(ctx, key, block, option);

  return status;
}

Status CacheGroupNodeImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                    size_t length, PrefetchOption option) {
  CHECK_RUNNING("Cache group node");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "prefetch(%s,%zu)",
                    key.Filename(), length);
  StepTimerGuard guard(timer);

  NEXT_STEP(kLocalPrefetch);
  status = block_cache_->Prefetch(ctx, key, length, option);

  return status;
}

void CacheGroupNodeImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                    const Block& block, AsyncCallback callback,
                                    CacheOption option) {
  CHECK_RUNNING("Cache group node");

  auto timer = std::make_shared<StepTimer>();
  timer->Start();

  auto cb = [timer, ctx, key, block, callback](Status status) {
    TraceLogGuard log(ctx, status, *timer, kModule, "async_cache(%s,%zu)",
                      key.Filename(), block.size);
    callback(status);
    timer->Stop();
  };

  block_cache_->AsyncCache(ctx, key, block, cb, option);
}

void CacheGroupNodeImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                       size_t length, AsyncCallback callback,
                                       PrefetchOption option) {
  CHECK_RUNNING("Cache group node");

  auto timer = std::make_shared<StepTimer>();
  timer->Start();

  auto cb = [timer, ctx, key, length, callback](Status status) {
    TraceLogGuard log(ctx, status, *timer, kModule, "async_prefetch(%s,%zu)",
                      key.Filename(), length);
    callback(status);
    timer->Stop();
  };

  block_cache_->AsyncPrefetch(ctx, key, length, cb, option);
}

Status CacheGroupNodeImpl::RangeCachedBlock(ContextSPtr ctx, StepTimer& timer,
                                            const BlockKey& key, off_t offset,
                                            size_t length, IOBuffer* buffer,
                                            RangeOption option) {
  NEXT_STEP(kLocalRange);
  option.retrive = false;
  auto status = block_cache_->Range(ctx, key, offset, length, buffer, option);
  if (status.ok()) {
    AddCacheHitCount(1);
  } else {
    AddCacheMissCount(1);
  }
  return status;
}

Status CacheGroupNodeImpl::RangeStorage(ContextSPtr ctx, StepTimer& timer,
                                        const BlockKey& key, off_t offset,
                                        size_t length, IOBuffer* buffer,
                                        RangeOption option) {
  NEXT_STEP(kGetStorage)
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    return status;
  }

  // Retrive range of block: unknown block size or unreach max_range_size
  auto block_size = option.block_size;
  if (block_size == 0 || length <= FLAGS_max_range_size_kb * kKiB) {
    NEXT_STEP(kS3Range)
    status = storage->Download(ctx, key, offset, length, buffer);
    if (status.ok() && block_size > 0) {
      block_cache_->AsyncPrefetch(ctx, key, block_size, [](Status status) {});
    }
    return status;
  }

  // Retrive the whole block
  NEXT_STEP(kS3Get)
  IOBuffer block;
  status = storage->Download(ctx, key, offset, block_size, &block);
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP(kAsyncCache)
  async_cacher_->AsyncCache(ctx, key, block);

  butil::IOBuf piecs;
  block.IOBuf().append_to(&piecs, length, offset);
  *buffer = IOBuffer(piecs);
  return status;
}

void CacheGroupNodeImpl::AddCacheHitCount(int64_t count) {
  CacheGroupNodeMetric::GetInstance().cache_hit_count << count;
}

void CacheGroupNodeImpl::AddCacheMissCount(int64_t count) {
  CacheGroupNodeMetric::GetInstance().cache_miss_count << count;
}

}  // namespace cache
}  // namespace dingofs
