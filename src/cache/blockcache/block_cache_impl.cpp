/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_impl.h"

#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/blockcache/disk_cache_group.h"
#include "cache/blockcache/mem_cache.h"
#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/storage/storage.h"
#include "cache/storage/storage_pool.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/step_timer.h"
#include "common/io_buffer.h"
#include "common/status.h"
#include "options/cache/blockcache.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_store, "disk",
              "Cache store type, can be none, disk or 3fs");
DEFINE_bool(enable_stage, true, "Whether to enable stage block for writeback");
DEFINE_bool(enable_cache, true, "Whether to enable cache block");
DEFINE_uint32(prefetch_max_inflights, 32,
              "Maximum inflight requests for prefetching blocks");

static const std::string kModule = kBlockCacheMoudule;

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option, StorageSPtr storage)
    : BlockCacheImpl(option, std::make_shared<SingleStorage>(storage)) {}

BlockCacheImpl::BlockCacheImpl(BlockCacheOption option,
                               StoragePoolSPtr storage_pool)
    : running_(false),
      option_(option),
      storage_pool_(storage_pool),
      prefetch_throttle_(
          std::make_shared<InflightThrottle>(FLAGS_prefetch_max_inflights)),
      joiner_(std::make_unique<BthreadJoiner>()) {
  if (HasCacheStore()) {
    store_ = std::make_shared<DiskCacheGroup>(option.disk_cache_options);
  } else {
    store_ = std::make_shared<MemCache>();
  }
  uploader_ = std::make_shared<BlockCacheUploader>(store_, storage_pool_);
}

BlockCacheImpl::~BlockCacheImpl() { Shutdown(); }

Status BlockCacheImpl::Start() {
  CHECK_NOTNULL(storage_pool_);
  CHECK_NOTNULL(store_);
  CHECK_NOTNULL(uploader_);
  CHECK_NOTNULL(prefetch_throttle_);
  CHECK_NOTNULL(joiner_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Block cache is starting...";

  uploader_->Start();

  auto status = store_->Start([this](ContextSPtr ctx, const BlockKey& key,
                                     size_t length, BlockContext block_ctx) {
    uploader_->AddStagingBlock(StagingBlock(ctx, key, length, block_ctx));
  });
  if (!status.ok()) {
    LOG(ERROR) << "Init cache store failed: " << status.ToString();
    return status;
  }

  status = joiner_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start bthread joiner failed: " << status.ToString();
    return status;
  }

  running_ = true;

  LOG(INFO) << "Block cache is up.";

  CHECK_RUNNING("Block cache");
  return Status::OK();
}

Status BlockCacheImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Block cache is shutting down...";

  joiner_->Shutdown();
  uploader_->Shutdown();
  store_->Shutdown();

  LOG(INFO) << "Block cache is down.";

  CHECK_DOWN("Block cache");
  return Status::OK();
}

Status BlockCacheImpl::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block, PutOption option) {
  CHECK_RUNNING("Block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  if (!option.writeback) {
    NEXT_STEP(kS3Put);
    status = StoragePut(ctx, key, block);
    return status;
  }

  // writeback: stage block
  NEXT_STEP(kStageBlock);
  CacheStore::StageOption opt;
  opt.block_ctx = option.block_ctx;
  status = store_->Stage(ctx, key, block, opt);
  if (status.ok()) {
    return status;
  } else if (status.IsCacheFull()) {
    LOG_EVERY_SECOND(WARNING)
        << "Stage block failed: trace id = " << ctx->TraceId()
        << ", key = " << key.Filename() << ", status = " << status.ToString();
  } else {
    LOG(ERROR) << "Stage block failed: trace id = " << ctx->TraceId()
               << ", key = " << key.Filename() << ", length = " << block.size
               << ", status = " << status.ToString();
  }

  // Stage block failed, try to upload it
  NEXT_STEP(kS3Put);
  status = StoragePut(ctx, key, block);
  return status;
}

Status BlockCacheImpl::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption option) {
  CHECK_RUNNING("Block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  NEXT_STEP(kLoadBlock);
  status = store_->Load(ctx, key, offset, length, buffer);
  if (status.ok()) {
    return status;
  } else if (!option.retrive) {
    LOG(ERROR)
        << "Load block failed, and no longer retrive storage: trace id = "
        << ctx->TraceId() << ", key = " << key.Filename()
        << ", offset = " << offset << ", length = " << length
        << ", status = " << status.ToString();
    return status;
  }

  NEXT_STEP(kS3Range);
  status = StorageRange(ctx, key, offset, length, buffer);
  return status;
}

Status BlockCacheImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block, CacheOption /*option*/) {
  CHECK_RUNNING("Block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP(kCacheBlock);
  status = store_->Cache(ctx, key, block);
  if (!status.ok()) {
    LOG(ERROR) << "Cache block failed: trace id = " << ctx->TraceId()
               << ", key = " << key.Filename() << ", length = " << block.size
               << ", status = " << status.ToString();
    return status;
  }

  return status;
}

Status BlockCacheImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length, PrefetchOption /*option*/) {
  CHECK_RUNNING("Block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "prefetch(%s,%lld)",
                    key.Filename(), length);
  StepTimerGuard guard(timer);

  if (IsCached(key)) {
    return Status::OK();
  }

  NEXT_STEP(kS3Range);
  IOBuffer buffer;
  status = StorageRange(ctx, key, 0, length, &buffer);
  if (!status.ok()) {
    return status;
  }

  NEXT_STEP(kCacheBlock);
  status = store_->Cache(ctx, key, Block(buffer));
  if (!status.ok()) {
    LOG(ERROR) << "Cache block failed: trace id = " << ctx->TraceId()
               << ", key = " << key.Filename() << ", length = " << length
               << ", status = " << status.ToString();
    return status;
  }

  return status;
}

void BlockCacheImpl::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                              const Block& block, AsyncCallback cb,
                              PutOption option) {
  CHECK_RUNNING("Block cache");

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

void BlockCacheImpl::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                off_t offset, size_t length, IOBuffer* buffer,
                                AsyncCallback cb, RangeOption option) {
  CHECK_RUNNING("Block cache");

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

void BlockCacheImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                const Block& block, AsyncCallback cb,
                                CacheOption option) {
  CHECK_RUNNING("Block cache");

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

void BlockCacheImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                   size_t length, AsyncCallback cb,
                                   PrefetchOption option) {
  CHECK_RUNNING("Block cache");

  // TODO: acts on sync op
  InflightThrottleGuard guard(prefetch_throttle_, 1);
  auto* self = GetSelfPtr();
  auto tid = RunInBthread([self, ctx, key, length, cb, option]() {
    Status status = self->Prefetch(ctx, key, length, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Status BlockCacheImpl::StoragePut(ContextSPtr ctx, const BlockKey& key,
                                  const Block& block) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    LOG(ERROR) << "Get storage failed: fs_id = " << key.fs_id
               << ", key = " << key.Filename()
               << ", status = " << status.ToString();
    return status;
  }

  status = storage->Put(ctx, key, block);
  if (!status.ok()) {
    LOG(ERROR) << "Storage put failed: key = " << key.Filename()
               << ", status = " << status.ToString();
  }
  return status;
}

Status BlockCacheImpl::StorageRange(ContextSPtr ctx, const BlockKey& key,
                                    off_t offset, size_t length,
                                    IOBuffer* buffer) {
  StorageSPtr storage;
  auto status = storage_pool_->GetStorage(key.fs_id, storage);
  if (!status.ok()) {
    LOG(ERROR) << "Get storage failed: trace id = " << ctx->TraceId()
               << ", fs_id = " << key.fs_id << ", key = " << key.Filename()
               << ", status = " << status.ToString();
    return status;
  }

  status = storage->Range(ctx, key, offset, length, buffer);
  if (!status.ok()) {
    LOG(ERROR) << "Storage range failed: trace id = " << ctx->TraceId()
               << ", key = " << key.Filename() << ", offset = " << offset
               << ", length = " << length << ", status = " << status.ToString();
  }
  return status;
}

bool BlockCacheImpl::HasCacheStore() const {
  return option_.cache_store != "none";
}

bool BlockCacheImpl::EnableStage() const {
  return HasCacheStore() && option_.enable_stage;
}

bool BlockCacheImpl::EnableCache() const {
  return HasCacheStore() && option_.enable_cache;
}

bool BlockCacheImpl::IsCached(const BlockKey& key) const {
  return store_->IsCached(key);
}

}  // namespace cache
}  // namespace dingofs
