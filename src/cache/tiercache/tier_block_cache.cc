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

#include "cache/tiercache/tier_block_cache.h"

#include <glog/logging.h>

#include <atomic>
#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/context.h"
#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/iutil/string_util.h"
#include "cache/remotecache/remote_block_cache.h"
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

TierBlockCache::TierBlockCache(StorageClientUPtr storage_client)
    : running_(false),
      storage_client_(std::move(storage_client)),
      cache_tracker_(std::make_shared<iutil::InflightTracker>(1024)),
      prefetch_tracker_(std::make_shared<iutil::InflightTracker>(
          FLAGS_prefetch_max_inflights)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()) {
  if (FLAGS_cache_store == "disk") {
    FLAGS_fix_buffer = false;
    local_block_cache_ =
        std::make_unique<BlockCacheImpl>(storage_client_.get());
  } else {
    local_block_cache_ = std::make_unique<BlockCache>();
  }

  if (!FLAGS_cache_group.empty()) {
    remote_block_cache_ =
        std::make_unique<RemoteBlockCacheImpl>(storage_client_.get());
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

Status TierBlockCache::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block, PutOption option) {
  DCHECK_RUNNING("TierBlockCache");

  Status status;
  if (option.writeback) {
    option.block_attr = BlockAttr(BlockAttr::kFromWriteback);
    if (EnableLocalStage()) {
      status = local_block_cache_->Put(ctx, key, block, option);
    } else if (EnableRemoteStage()) {
      status = remote_block_cache_->Put(ctx, key, block, option);
    } else {
      status = Status::NotFound("no cache layer found");
    }

    if (status.ok()) {
      return status;
    } else {
      LOG(WARNING) << "Fail to put block to cache, key=" << key.Filename()
                   << ", status=" << status.ToString();
    }
  }

  // FIXME: !!!!!! independent copy thread pool !!!!!!
  auto new_block = CopyBlock(block);
  if (EnableRemoteCache() && FLAGS_fill_group_cache) {
    FillGroupCache(ctx, key, new_block);
  }

  status = storage_client_->Put(ctx, key, &new_block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to storage, key=" << key.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

Status TierBlockCache::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption option) {
  DCHECK_RUNNING("TierBlockCache");

  Status status = Status::NotFound("no cache layer found");

  // Firstly, try local cache
  if (EnableLocalCache()) {
    status = local_block_cache_->Range(ctx, key, offset, length, buffer,
                                       {.retrieve_storage = false});
    if (status.ok()) {
      return status;
    }

    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR)
          << "Fail to range block from local cache because cache is unhealthy";
    } else if (!status.IsNotFound()) {
      LOG(ERROR) << "Fail to range block from local cache, key="
                 << key.Filename() << ", status=" << status.ToString();
    }
  }

  // Secondly, try remote cache
  if (EnableRemoteCache()) {
    status =
        remote_block_cache_->Range(ctx, key, offset, length, buffer, option);
    if (status.ok()) {
      return status;
    }

    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR)
          << "Fail to range block from remote cache because cache is unhealthy";
    } else if (!status.IsNotFound()) {
      LOG(ERROR) << "Fail to range block from remote cache, key="
                 << key.Filename() << ", status=" << status.ToString();
    }
  }

  // Finally, retrieve from storage if allowed
  if (option.retrieve_storage) {
    status = storage_client_->Range(ctx, key, offset, length, buffer);
    if (!status.ok()) {
      LOG(ERROR) << "Fail to range block from storage, key=" << key.Filename()
                 << ", status=" << status.ToString();
    }
  }
  return status;
}

Status TierBlockCache::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption option) {
  DCHECK_RUNNING("TierBlockCache");

  Status status;
  if (EnableLocalCache()) {
    status = local_block_cache_->Cache(ctx, key, block, option);
  } else if (EnableRemoteCache()) {
    status = remote_block_cache_->Cache(ctx, key, block, option);
  } else {
    status = Status::NotFound("no cache layer found");
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block to cache, key=" << key.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

Status TierBlockCache::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length, PrefetchOption option) {
  DCHECK_RUNNING("TierBlockCache");

  Status status;
  if (EnableLocalCache()) {
    status = local_block_cache_->Prefetch(ctx, key, length, option);
  } else if (EnableRemoteCache()) {
    status = remote_block_cache_->Prefetch(ctx, key, length, option);
  } else {
    status = Status::NotFound("no cache layer found");
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to prefetch block, key=" << key.Filename()
               << ", status=" << status.ToString();
  }
  return status;
}

void TierBlockCache::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                              const Block& block, AsyncCallback cb,
                              PutOption option) {
  DCHECK_RUNNING("TierBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, ctx, key, block, cb, option]() {
    Status status = self->Put(ctx, key, block, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                off_t offset, size_t length, IOBuffer* buffer,
                                AsyncCallback cb, RangeOption option) {
  DCHECK_RUNNING("TierBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [self, ctx, key, offset, length, buffer, cb, option]() {
        Status status = self->Range(ctx, key, offset, length, buffer, option);
        if (cb) {
          cb(status);
        }
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                const Block& block, AsyncCallback cb,
                                CacheOption option) {
  DCHECK_RUNNING("TierBlockCache");

  // TODO: maybe filter out duplicate task by up-level is better
  auto tracker = cache_tracker_;
  auto status = tracker->Add(key.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([tracker, self, ctx, key, block, cb, option]() {
        Status status = self->Cache(ctx, key, block, option);
        if (cb) {
          cb(status);
        }

        tracker->Remove(key.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                   size_t length, AsyncCallback cb,
                                   PrefetchOption option) {
  DCHECK_RUNNING("TierBlockCache");

  // TODO: maybe filter out duplicate task by up-level is better
  auto tracker = prefetch_tracker_;
  auto status = tracker->Add(key.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid =
      iutil::RunInBthread([tracker, self, ctx, key, length, cb, option]() {
        Status status = self->Prefetch(ctx, key, length, option);
        if (cb) {
          cb(status);
        }

        tracker->Remove(key.Filename());
      });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Block TierBlockCache::CopyBlock(const Block& block) {
  IOBuffer buffer;
  char* data = new char[block.size];
  block.buffer.CopyTo(data);
  buffer.AppendUserData(data, block.size, iutil::DeleteBuffer);
  return Block(std::move(buffer));
}

void TierBlockCache::FillGroupCache(ContextSPtr ctx, const BlockKey& key,
                                    const Block& block) {
  remote_block_cache_->AsyncCache(
      ctx, key, block, [ctx, key, block](Status status) {
        if (!status.ok()) {
          LOG(ERROR) << "Fail to async block, status=" << status.ToString();
        }
      });
}

}  // namespace cache
}  // namespace dingofs
