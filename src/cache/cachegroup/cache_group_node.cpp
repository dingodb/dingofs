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

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/blockcache/disk_cache_layout.h"
#include "cache/metrics/cache_group_node_metric.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

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
  if (!running_.exchange(true)) {
    auto rc = mds_client_->Init(option_.mds_option, mds_base_.get());
    if (rc != PBFSStatusCode::OK) {
      return Status::Internal("init mds client failed");
    }

    auto status = member_->JoinGroup();
    if (!status.ok()) {
      return status;
    }

    // Cache directory name depends node member uuid, so init after join group
    status = InitBlockCache();
    if (!status.ok()) {
      return status;
    }

    async_cache_ = std::make_unique<AsyncCacheImpl>(block_cache_);
    status = async_cache_->Start();
    if (!status.ok()) {
      return status;
    }

    heartbeat_->Start();
  }
  return Status::OK();
}

Status CacheGroupNodeImpl::Stop() {
  if (running_.exchange(false)) {
    heartbeat_->Stop();

    Status status = member_->LeaveGroup();
    if (!status.ok()) {
      return status;
    }

    status = async_cache_->Stop();
    if (!status.ok()) {
      return status;
    }

    return block_cache_->Shutdown();
  }
  return Status::OK();
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

  return block_cache_->Init();
}

Status CacheGroupNodeImpl::Put(const BlockKey& key, const Block& block) {
  auto option = PutOption();
  option.writeback = true;
  return block_cache_->Put(key, block, option);
}

Status CacheGroupNodeImpl::Range(const BlockKey& key, off_t offset,
                                 size_t length, IOBuffer* buffer,
                                 size_t block_size) {
  auto status = RangeCachedBlock(key, offset, length, buffer);
  if (status.ok()) {
    // do nothing
  } else if (status.IsNotFound()) {
    status = RangeStorage(key, offset, offset, buffer, block_size);
  }
  return status;
}

Status CacheGroupNodeImpl::Cache(const BlockKey& key, const Block& block) {
  return block_cache_->Cache(key, block);
}

Status CacheGroupNodeImpl::Prefetch(const BlockKey& key, size_t length) {
  return block_cache_->Prefetch(key, length);
}

Status CacheGroupNodeImpl::RangeCachedBlock(const BlockKey& key, off_t offset,
                                            size_t length, IOBuffer* buffer) {
  RangeOption option;
  option.retrive = false;
  auto status = block_cache_->Range(key, offset, length, buffer, option);
  if (status.ok()) {
    CacheGroupNodeMetric::GetInstance().AddCacheHit();
  } else {
    CacheGroupNodeMetric::GetInstance().AddCacheMiss();
  }
  return status;
}

Status CacheGroupNodeImpl::RangeStorage(const BlockKey& key, off_t offset,
                                        size_t length, IOBuffer* buffer,
                                        size_t block_size) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    return status;
  }

  // Retrive range of block: unknown block size or unreach max_range_size
  if (block_size == 0 || length <= FLAGS_max_range_size_kb * kKiB) {
    return storage->Range(key.StoreKey(), offset, length, buffer);
  }

  // Retrive the whole block
  IOBuffer block;
  status = storage->Range(key.StoreKey(), offset, block_size, &block);
  if (!status.ok()) {
    return status;
  }

  butil::IOBuf piecs;
  block.IOBuf().append_to(&piecs, length, offset);
  *buffer = IOBuffer(piecs);

  async_cache_->Cache(key, block);

  return Status::OK();
}

}  // namespace cache
}  // namespace dingofs
