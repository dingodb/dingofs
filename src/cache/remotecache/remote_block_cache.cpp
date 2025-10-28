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
 * Created Date: 2025-01-13
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_block_cache.h"

#include <butil/iobuf.h>

#include <memory>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "cache/metric/remote_block_cache_metric.h"
#include "cache/remotecache/mem_cache.h"
#include "cache/remotecache/remote_cache_node_group.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "common/io_buffer.h"
#include "common/options/cache/option.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_string(cache_group, "",
              "Cache group name to use, empty means not use cache group");

DEFINE_bool(enable_remote_prefetch, true,
            "Whether enable remote prefetch, default is true");
DEFINE_validator(enable_remote_prefetch, brpc::PassValidate);

DEFINE_uint32(remote_prefetch_max_buffer_size_mb, 512,
              "Maximum buffer size for remote prefetch");

static const std::string kModule = "remotecache";

RemoteBlockCacheImpl::RemoteBlockCacheImpl(StorageSPtr storage)
    : running_(false),
      storage_(storage),
      joiner_(std::make_unique<BthreadJoiner>()),
      memcache_(std::make_shared<MemCacheImpl>(
          FLAGS_remote_prefetch_max_buffer_size_mb * kMiB)) {
  if (HasCacheStore()) {
    remote_node_ = std::make_shared<RemoteCacheNodeGroup>();
  } else {
    remote_node_ = std::make_shared<NoneRemoteCacheNode>();
  }
  prefetcher_ = std::make_unique<Prefetcher>(memcache_, remote_node_);
}

Status RemoteBlockCacheImpl::Start() {
  CHECK_NOTNULL(remote_node_);
  CHECK_NOTNULL(storage_);
  CHECK_NOTNULL(memcache_);
  CHECK_NOTNULL(prefetcher_);
  CHECK_NOTNULL(joiner_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote block cache is starting...";

  auto status = joiner_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start bthread joiner failed: " << status.ToString();
    return status;
  }

  status = remote_node_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start remote node failed: " << status.ToString();
    return status;
  }

  status = memcache_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start mem cache failed: " << status.ToString();
    return status;
  }

  status = prefetcher_->Start();
  if (!status.ok()) {
    LOG(ERROR) << "Start prefetcher failed: " << status.ToString();
    return status;
  }

  RemoteBlockCacheMetric::Init();

  running_ = true;

  LOG(INFO) << "Remote block cache is up.";

  CHECK_RUNNING("Remote block cache");
  return Status::OK();
}

Status RemoteBlockCacheImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Remote block cache is shutting down...";

  auto status = prefetcher_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown prefetcher failed: " << status.ToString();
    return status;
  }

  status = memcache_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown mem cache failed: " << status.ToString();
    return status;
  }

  status = remote_node_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown remote node failed: " << status.ToString();
    return status;
  }

  status = joiner_->Shutdown();
  if (!status.ok()) {
    LOG(ERROR) << "Shutdown bthread joiner failed: " << status.ToString();
    return status;
  }

  LOG(INFO) << "Remote block cache is down.";

  CHECK_DOWN("Remote block cache");
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(ContextSPtr ctx, const BlockKey& key,
                                 const Block& block, PutOption option) {
  CHECK_RUNNING("Remote block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "put(%s,%zu)", key.Filename(),
                    block.size);
  StepTimerGuard guard(timer);

  if (!option.writeback) {
    NEXT_STEP("s3_put");
    status = storage_->Upload(ctx, key, block);
  } else {
    NEXT_STEP("remote_put");
    status = remote_node_->Put(ctx, key, block);
    if (!status.ok()) {
      NEXT_STEP("s3_put");
      status = storage_->Upload(ctx, key, block);
    }
  }

  if (!status.ok()) {
    LOG_CTX(ERROR) << "Put block failed: key = " << key.Filename()
                   << ", length = " << block.size
                   << ", status = " << status.ToString();
  }
  return status;
}

Status RemoteBlockCacheImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                   off_t offset, size_t length,
                                   IOBuffer* buffer, RangeOption option) {
  CHECK_RUNNING("Remote block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "range(%s,%lld,%zu)",
                    key.Filename(), offset, length);
  StepTimerGuard guard(timer);

  SCOPE_EXIT {
    if (ctx->GetCacheHit()) {
      RemoteBlockCacheMetric::AddCacheHit(1);
    } else {
      RemoteBlockCacheMetric::AddCacheMiss(1);
    }
  };

  if (FLAGS_enable_remote_prefetch && option.block_size > length) {
    prefetcher_->Submit(ctx, key, option.block_size);
  }

  NEXT_STEP("memcache");
  Block block;
  status = memcache_->Get(key, &block);
  if (status.ok()) {
    block.buffer.AppendTo(buffer, length, offset);
    ctx->SetCacheHit(true);
    return status;
  }

  NEXT_STEP("remote_range");
  status = remote_node_->Range(ctx, key, offset, length, buffer, option);
  if (status.ok()) {
    return status;
  } else if (!option.retrive) {
    auto message = absl::StrFormat(
        "[%s] Remote range block failed: key = %s, offset = %lld"
        ", length = %zu, status = %s",
        ctx->TraceId(), key.Filename(), offset, length, status.ToString());
    if (status.IsCacheUnhealthy()) {
      LOG_EVERY_SECOND(ERROR) << message;
    } else {
      LOG(ERROR) << message;
    }
    return status;
  }

  NEXT_STEP("s3_range");
  status = storage_->Download(ctx, key, offset, length, buffer);
  if (!status.ok()) {
    GENERIC_LOG_DOWNLOAD_ERROR();
  }
  return status;
}

Status RemoteBlockCacheImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                                   const Block& block, CacheOption /*option*/) {
  CHECK_RUNNING("Remote block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "cache(%s,%zu)",
                    key.Filename(), block.size);
  StepTimerGuard guard(timer);

  NEXT_STEP("remote_cache");
  status = remote_node_->Cache(ctx, key, block);

  if (!status.ok()) {
    GENERIC_LOG_CACHE_ERROR("remote cache node");
  }
  return status;
}

Status RemoteBlockCacheImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                      size_t length,
                                      PrefetchOption /*option*/) {
  CHECK_RUNNING("Remote block cache");

  Status status;
  StepTimer timer;
  TraceLogGuard log(ctx, status, timer, kModule, "prefetch(%s,%zu)",
                    key.Filename(), length);
  StepTimerGuard guard(timer);

  NEXT_STEP("remote_prefetch");
  status = remote_node_->Prefetch(ctx, key, length);

  if (!status.ok()) {
    GENERIC_LOG_PREFETCH_ERROR("remote cache node");
  }
  return status;
}

void RemoteBlockCacheImpl::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                                    const Block& block, AsyncCallback cb,
                                    PutOption option) {
  CHECK_RUNNING("Remote block cache");

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

void RemoteBlockCacheImpl::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                      off_t offset, size_t length,
                                      IOBuffer* buffer, AsyncCallback cb,
                                      RangeOption option) {
  CHECK_RUNNING("Remote block cache");

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

void RemoteBlockCacheImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                      const Block& block, AsyncCallback cb,
                                      CacheOption option) {
  CHECK_RUNNING("Remote block cache");

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

void RemoteBlockCacheImpl::AsyncPrefetch(ContextSPtr ctx, const BlockKey& key,
                                         size_t length, AsyncCallback cb,
                                         PrefetchOption option) {
  CHECK_RUNNING("Remote block cache");

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

bool RemoteBlockCacheImpl::HasCacheStore() const {
  return !FLAGS_cache_group.empty();
}

// We gurantee that block cache of cache group node is always enable stage
// and cache.
bool RemoteBlockCacheImpl::EnableStage() const { return HasCacheStore(); }
bool RemoteBlockCacheImpl::EnableCache() const { return HasCacheStore(); };
bool RemoteBlockCacheImpl::IsCached(const BlockKey& /*key*/) const {
  return HasCacheStore();  // FIXME: using rpc request to check if the key is
                           // cached
}

}  // namespace cache
}  // namespace dingofs
