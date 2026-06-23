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

#include "cache/tier/tier_block_cache.h"

#include <absl/strings/str_format.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <utility>

#include "cache/api/block_cache.h"
#include "cache/common/macro.h"
#include "cache/common/slab_buffer.h"
#include "cache/common/storage_client.h"
#include "cache/iutil/string_util.h"
#include "cache/local/cache_store.h"
#include "cache/local/local_block_cache.h"
#include "cache/remote/remote_block_cache.h"
#include "common/blockaccess/block_accesser.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_bool(fill_group_cache, true,
            "whether the data blocks uploaded to the storage are "
            "simultaneously sent to the cache group node.");
DEFINE_validator(fill_group_cache, brpc::PassValidate);

DEFINE_uint32(prefetch_max_inflights, 16,
              "maximum inflight requests for prefetching blocks");

DEFINE_uint32(small_block_size_kb, 0,
              "blocks whose whole size is smaller than this many KB are "
              "automatically pinned to the local cache tier (when local cache "
              "is enabled). 0 disables this behavior.");

static bool UseLocal(CacheTier tier) {
  return tier == CacheTier::kDefault || tier == CacheTier::kLocal;
}

static bool UseRemote(CacheTier tier) {
  return tier == CacheTier::kDefault || tier == CacheTier::kRemote;
}

TierBlockCache::TierBlockCache(StorageClientUPtr storage_client)
    : running_(false),
      storage_client_(std::move(storage_client)),
      cache_tracker_(std::make_shared<iutil::InflightTracker>(1024)),
      prefetch_tracker_(std::make_shared<iutil::InflightTracker>(
          FLAGS_prefetch_max_inflights)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()) {
  if (FLAGS_cache_store == "disk") {
    local_block_cache_ =
        std::make_unique<LocalBlockCache>(storage_client_.get());
  } else {
    local_block_cache_ = std::make_unique<BlockCache>();
  }

  if (!FLAGS_cache_group.empty()) {
    remote_block_cache_ =
        std::make_unique<RemoteBlockCache>(storage_client_.get());
  } else {
    remote_block_cache_ = std::make_unique<BlockCache>();
  }
}

TierBlockCache::TierBlockCache(blockaccess::BlockAccesser* block_accesser)
    : TierBlockCache(std::make_unique<StorageClient>(block_accesser)) {}

Status TierBlockCache::Start() {
  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "TierBlockCache already started";
    return Status::OK();
  }

  LOG(INFO) << "TierBlockCache is starting...";

  if (FLAGS_cache_store == "disk") {
    auto status = InitializeGlobalSlabPool();
    if (!status.ok()) {
      LOG(ERROR) << "Fail to initialize global slab pool: "
                 << status.ToString();
      return status;
    }
  }

  auto status = storage_client_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start StorageClient";
    return status;
  }

  status = local_block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start local BlockCache";
    return status;
  }

  status = remote_block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start remote BlockCache";
    return status;
  }

  joiner_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "TierBlockCache is up: local=" << *local_block_cache_
            << ", remote=" << *remote_block_cache_;
  return Status::OK();
}

Status TierBlockCache::Shutdown() {
  if (!running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "TierBlockCache already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "TierBlockCache is shutting down...";

  auto status = local_block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown local BlockCache";
    return status;
  }

  status = remote_block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown remote BlockCache";
    return status;
  }

  status = storage_client_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to shutdown StorageClient";
    return status;
  }

  joiner_->Shutdown();

  LOG(INFO) << "TierBlockCache is down";
  running_.store(false, std::memory_order_relaxed);
  return Status::OK();
}

Status TierBlockCache::Put(BlockHandle handle, IOBuffer block,
                           PutOption option) {
  DCHECK_RUNNING("TierBlockCache");

  option.tier = ResolveTier(handle, option.tier);

  Status status;
  if (option.writeback) {
    option.block_attr = BlockAttr(BlockAttr::kFromWriteback);
    // Don't move `block` here: if writeback fails, we still need it for the
    // storage fallback path below.
    if (UseLocal(option.tier) && EnableLocalStage()) {
      status = local_block_cache_->Put(handle, block, option);
    } else if (UseRemote(option.tier) && EnableRemoteStage()) {
      status = remote_block_cache_->Put(handle, block, option);
    } else {
      status = Status::NotFound("no cache layer found");
    }

    if (status.ok()) {
      return status;
    }
    LOG(WARNING) << "Fail to put block to cache, key=" << handle.Filename()
                 << ", status=" << status.ToString();
  }

  // S3 upload (storage_client → IOBuffer::Fetch1) requires a single
  // contiguous backing block; linearize once and share with the optional
  // fill-group path so downstream consumers see the same flat buffer.
  IOBuffer contiguous = CopyBlock(block);
  if (UseRemote(option.tier) && EnableRemoteCache() && FLAGS_fill_group_cache) {
    FillGroupCache(handle, contiguous);
  }

  status = storage_client_->Put(handle, contiguous);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to storage, key=" << handle.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

Status TierBlockCache::Range(BlockHandle handle, off_t offset, size_t length,
                             IOBuffer* buffer, RangeOption option) {
  DCHECK_RUNNING("TierBlockCache");
  CHECK_NOTNULL(buffer);
  CHECK_EQ(buffer->BackingBlockNum(), 1);

  option.tier = ResolveTier(handle, option.tier);

  Status status = Status::NotFound("no cache layer found");

  // Firstly, try local cache (skipped if caller pinned to remote tier).
  if (UseLocal(option.tier) && (EnableLocalCache() || EnableLocalStage())) {
    status = local_block_cache_->Range(handle, offset, length, buffer,
                                       {.retrieve_storage = false});
    if (status.ok()) {
      return status;
    }

    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR)
          << "Fail to range block from local cache because cache is unhealthy";
    } else if (!status.IsNotFound()) {
      LOG(ERROR) << "Fail to range block from local cache, key="
                 << handle.Filename() << ", status=" << status.ToString();
    }
  }

  // Secondly, try remote cache (skipped if caller pinned to local tier).
  if (UseRemote(option.tier) && EnableRemoteCache()) {
    status = remote_block_cache_->Range(handle, offset, length, buffer, option);
    if (status.ok()) {
      return status;
    }

    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR)
          << "Fail to range block from remote cache because cache is unhealthy";
    } else if (!status.IsNotFound()) {
      LOG(ERROR) << "Fail to range block from remote cache, key="
                 << handle.Filename() << ", status=" << status.ToString();
    }
  }

  // Finally, retrieve from storage if allowed
  if (option.retrieve_storage) {
    status = storage_client_->Range(handle, offset, length, buffer);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to range block from storage, key="
                 << handle.Filename() << ", status=" << status.ToString();
    }
  }
  return status;
}

Status TierBlockCache::Cache(BlockHandle handle, IOBuffer block,
                             CacheOption option) {
  DCHECK_RUNNING("TierBlockCache");

  option.tier = ResolveTier(handle, option.tier);

  Status status;
  if (UseLocal(option.tier) && EnableLocalCache()) {
    status = local_block_cache_->Cache(handle, std::move(block), option);
  } else if (UseRemote(option.tier) && EnableRemoteCache()) {
    status = remote_block_cache_->Cache(handle, std::move(block), option);
  } else {
    status = Status::NotFound("no cache layer found");
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block to cache, key=" << handle.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

Status TierBlockCache::Prefetch(BlockHandle handle, size_t length,
                                PrefetchOption option) {
  DCHECK_RUNNING("TierBlockCache");

  option.tier = ResolveTier(handle, option.tier);

  Status status;
  if (UseLocal(option.tier) && EnableLocalCache()) {
    status = local_block_cache_->Prefetch(handle, length, option);
  } else if (UseRemote(option.tier) && EnableRemoteCache()) {
    status = remote_block_cache_->Prefetch(handle, length, option);
  } else {
    status = Status::NotFound("no cache layer found");
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to prefetch block, key=" << handle.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

void TierBlockCache::AsyncPut(BlockHandle handle, IOBuffer block,
                              AsyncCallback cb, PutOption option) {
  DCHECK_RUNNING("TierBlockCache");

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([self, handle = std::move(handle),
                           block = std::move(block), cb, option]() mutable {
        Status status = self->Put(std::move(handle), std::move(block), option);
        if (cb) {
          cb(status);
        }
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncRange(BlockHandle handle, off_t offset, size_t length,
                                IOBuffer* buffer, AsyncCallback cb,
                                RangeOption option) {
  DCHECK_RUNNING("TierBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, handle = std::move(handle), offset,
                                  length, buffer, cb, option]() mutable {
    Status status =
        self->Range(std::move(handle), offset, length, buffer, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncCache(BlockHandle handle, IOBuffer block,
                                AsyncCallback cb, CacheOption option) {
  DCHECK_RUNNING("TierBlockCache");

  // TODO: maybe filter out duplicate task by up-level is better
  auto tracker = cache_tracker_;
  auto status = tracker->Add(handle.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [tracker, self, handle, block = std::move(block), cb, option]() mutable {
        Status status = self->Cache(handle, std::move(block), option);
        if (cb) {
          cb(status);
        }
        tracker->Remove(handle.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncPrefetch(BlockHandle handle, size_t length,
                                   AsyncCallback cb, PrefetchOption option) {
  DCHECK_RUNNING("TierBlockCache");

  // TODO: maybe filter out duplicate task by up-level is better
  auto tracker = prefetch_tracker_;
  auto status = tracker->Add(handle.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [tracker, self, handle, length, cb, option]() mutable {
        Status status = self->Prefetch(handle, length, option);
        if (cb) {
          cb(status);
        }
        tracker->Remove(handle.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

CacheTier TierBlockCache::ResolveTier(const BlockHandle& handle,
                                      CacheTier hint) const {
  if (hint != CacheTier::kDefault) {
    return hint;  // Respect caller's explicit choice.
  }
  if (FLAGS_small_block_size_kb == 0 || !local_block_cache_->IsEnabled()) {
    return hint;
  }
  if (handle.StoreSize() <
      static_cast<uint64_t>(FLAGS_small_block_size_kb) * 1024) {
    return CacheTier::kLocal;
  }
  return hint;
}

IOBuffer TierBlockCache::CopyBlock(const IOBuffer& block) {
  IOBuffer buffer;
  size_t size = block.Size();
  char* data = new char[size];
  block.CopyTo(data);
  buffer.AppendUserData(data, size, iutil::DeleteBuffer);
  return buffer;
}

void TierBlockCache::FillGroupCache(const BlockHandle& handle,
                                    const IOBuffer& block) {
  // IOBuffer copy is O(1) refcount-incr on underlying butil::IOBuf blocks;
  // the lambda holds its own ref so the data outlives the caller's `block`.
  remote_block_cache_->AsyncCache(handle, block, [handle](Status status) {
    if (!status.ok()) {
      LOG(ERROR) << "Fail to async block, status=" << status.ToString();
    }
  });
}

}  // namespace cache
}  // namespace dingofs
