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

#include <memory>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/block_cache_impl.h"
#include "cache/common/macro.h"
#include "cache/remotecache/remote_block_cache.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_impl.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/inflight_tracker.h"
#include "cache/utils/offload_thread_pool.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_bool(fill_group_cache, true,
            "Whether the data blocks uploaded to the storage are "
            "simultaneously sent to the cache group node.");
DEFINE_validator(fill_group_cache, brpc::PassValidate);

DEFINE_uint32(prefetch_max_inflights, 16,
              "Maximum inflight requests for prefetching blocks");

static const std::string kModule = "tiercache";

TierBlockCache::TierBlockCache(StorageSPtr storage)
    : running_(false),
      storage_(storage),
      local_block_cache_(std::make_unique<BlockCacheImpl>(storage_)),
      remote_block_cache_(std::make_unique<RemoteBlockCacheImpl>(storage_)),
      joiner_(std::make_unique<BthreadJoiner>()),
      inflight_tracker_(
          std::make_unique<InflightTracker>(FLAGS_prefetch_max_inflights)) {}

TierBlockCache::TierBlockCache(blockaccess::BlockAccesser* block_accesser)
    : TierBlockCache(std::make_shared<StorageImpl>(block_accesser)) {}

Status TierBlockCache::Start() {
  CHECK_NOTNULL(storage_);
  CHECK_NOTNULL(local_block_cache_);
  CHECK_NOTNULL(remote_block_cache_);
  CHECK_NOTNULL(joiner_);
  CHECK_NOTNULL(inflight_tracker_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Tier block cache is starting...";

  OffloadThreadPool::GetInstance().Start();

  auto status = storage_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start storage failed: " << status.ToString();
    return status;
  }

  status = local_block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start local block cache failed: " << status.ToString();
    return status;
  }

  status = remote_block_cache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start remote block cache failed: " << status.ToString();
    return status;
  }

  status = joiner_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start bthread joiner failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Tier block cache is up: local_block_cache(enable="
            << local_block_cache_->HasCacheStore()
            << ",stage=" << local_block_cache_->EnableStage()
            << ",cache=" << local_block_cache_->EnableCache()
            << "), remote_block_cache(enable="
            << remote_block_cache_->HasCacheStore()
            << ",stage=" << remote_block_cache_->EnableStage()
            << ",cache=" << remote_block_cache_->EnableCache() << ")";

  CHECK_RUNNING("Tier block cache");
  return Status::OK();
}

Status TierBlockCache::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Tier block cache is shutting down...";

  auto status = local_block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown local block cache failed: " << status.ToString();
    return status;
  }

  status = remote_block_cache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown remote block cache failed: " << status.ToString();
    return status;
  }

  status = storage_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown storage failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Tier block cache is down.";

  CHECK_DOWN("Tier block cache");
  return Status::OK();
}

Status TierBlockCache::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block, PutOption option) {
  CHECK_RUNNING("Tier block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  if (option.writeback) {
    if (EnableLocalStage()) {
      NEXT_STEP("local_put");
      status = local_block_cache_->Put(ctx, key, block, option);
    } else if (EnableRemoteStage()) {
      NEXT_STEP("remote_put");
      status = remote_block_cache_->Put(ctx, key, block, option);
    } else {
      status = Status::NotFound("both local and remote block cache disabled");
    }

    if (status.ok()) {
      return status;
    }
  }

  NEXT_STEP("s3_put");
  Storage::PutOption opt;
  if (EnableRemoteCache() && FLAGS_fill_group_cache) {
    opt.async_cache_fn = NewFillGroupCacheCb(ctx);
  }
  status = storage_->Put(ctx, key, block, opt);

  if (!status.ok()) {
    GENERIC_LOG_UPLOAD_ERROR();
  }
  return status;
}

Status TierBlockCache::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption option) {
  CHECK_RUNNING("Tier block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  // Try local cache first
  if (EnableLocalCache()) {
    NEXT_STEP("local_range");
    auto opt = option;
    opt.retrive = false;
    status = local_block_cache_->Range(ctx, key, offset, length, buffer, opt);
    if (status.ok()) {
      return status;
    }
  }

  // Not found or failed for local cache
  if (EnableRemoteCache()) {  // Remote cache will always retrive storage
    NEXT_STEP("remote_range");
    status =
        remote_block_cache_->Range(ctx, key, offset, length, buffer, option);
  } else if (option.retrive) {  // No remote cache, retrive storage
    NEXT_STEP("s3_range");
    status = storage_->Range(ctx, key, offset, length, buffer);
  } else {
    status = Status::NotFound("no available cache can be tried");
  }

  if (!status.ok()) {
    auto message = absl::StrFormat(
        "[%s] Range block failed: key = %s, offset = %lld, length = %zu, "
        "status = %s",
        ctx->TraceId(), key.Filename(), offset, length, status.ToString());
    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR) << message;
    } else if (!status.IsNotFound()) {
      LOG(ERROR) << message;
    }
  }
  return status;
}

Status TierBlockCache::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption option) {
  CHECK_RUNNING("Tier block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  if (EnableLocalCache()) {
    NEXT_STEP("local_cache");
    status = local_block_cache_->Cache(ctx, key, block, option);
  } else if (EnableRemoteCache()) {
    NEXT_STEP("remote_cache");
    status = remote_block_cache_->Cache(ctx, key, block, option);
  } else {
    status = Status::NotFound("both local and remote block cache disabled");
  }

  if (!status.ok()) {
    // GENERIC_LOG_CACHE_ERROR();
  }
  return status;
}

Status TierBlockCache::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length, PrefetchOption option) {
  CHECK_RUNNING("Tier block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "prefetch(%s,%zu)",
                    key.Filename(), length);
  StepTimerGuard guard(timer);

  if (EnableLocalCache()) {
    NEXT_STEP("local_prefetch");
    status = local_block_cache_->Prefetch(ctx, key, length, option);
  } else if (EnableRemoteCache()) {
    NEXT_STEP("remote_prefetch");
    status = remote_block_cache_->Prefetch(ctx, key, length, option);
  } else {
    status = Status::NotFound("both local and remote block cache disabled");
  }

  if (!status.ok()) {
    LOG_CTX(ERROR) << "Prefetch block failed: key = " << key.Filename()
                   << ", length = " << length
                   << ", status = " << status.ToString();
  }
  return status;
}

void TierBlockCache::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                              const Block& block, AsyncCallback cb,
                              PutOption option) {
  CHECK_RUNNING("Tier block cache");

  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, block, cb, option]() {
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
  CHECK_RUNNING("Tier block cache");

  auto* self = GetSelfPtr();
  auto tid =
      RunInBthread([self, ctx, key, offset, length, buffer, cb, option]() {
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
  CHECK_RUNNING("Tier block cache");

  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, block, cb, option]() {
    Status status = self->Cache(ctx, key, block, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void TierBlockCache::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                   size_t length, AsyncCallback cb,
                                   PrefetchOption option) {
  CHECK_RUNNING("Tier block cache");

  // TODO: maybe filter out duplicate task by up-level is better
  auto status = inflight_tracker_->Add(key.Filename());
  if (status.IsExist()) {
    if (cb) {
      cb(status);
    }
    return;
  }

  auto* self = GetSelfPtr();
  auto tid = RunInBthread([&, self, ctx, key, length, cb, option]() {
    Status status = self->Prefetch(ctx, key, length, option);
    if (cb) {
      cb(status);
    }

    inflight_tracker_->Remove(key.Filename());
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

TierBlockCache::FillGroupCacheCb TierBlockCache::NewFillGroupCacheCb(
    ContextSPtr ctx) {
  return [this, ctx](const BlockKey& key, const Block& block) {
    remote_block_cache_->AsyncCache(
        ctx, key, block, [ctx, key, block](Status status) {
          if (!status.ok()) {
            LOG_CTX(ERROR) << "Fill group cache failed: key = "
                           << key.Filename() << ", length = " << block.size
                           << ", status = " << status.ToString();
          }
        });
  };
}

bool TierBlockCache::EnableLocalStage() const {
  return local_block_cache_->EnableStage();
}

bool TierBlockCache::EnableLocalCache() const {
  return local_block_cache_->EnableCache();
}

bool TierBlockCache::EnableRemoteStage() const {
  return remote_block_cache_->EnableStage();
}

bool TierBlockCache::EnableRemoteCache() const {
  return remote_block_cache_->EnableCache();
}

bool TierBlockCache::HasCacheStore() const {
  return local_block_cache_->HasCacheStore() ||
         remote_block_cache_->HasCacheStore();
}

bool TierBlockCache::EnableStage() const {
  return EnableLocalStage() || EnableRemoteStage();
}

bool TierBlockCache::EnableCache() const {
  return EnableLocalCache() || EnableRemoteCache();
}

bool TierBlockCache::IsCached(const BlockKey& key) const {
  if (EnableLocalCache() && local_block_cache_->IsCached(key)) {
    return true;
  } else if (EnableRemoteCache() && remote_block_cache_->IsCached(key)) {
    return true;
  }
  return false;
}

}  // namespace cache
}  // namespace dingofs
