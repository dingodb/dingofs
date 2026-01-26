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

#include <brpc/reloadable_flags.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>

#include <atomic>
#include <memory>

#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/remotecache/upstream.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace brpc {
DECLARE_int32(idle_timeout_second);
DECLARE_bool(log_idle_connection_close);
}  // namespace brpc

namespace dingofs {
namespace cache {

DEFINE_int32(brpc_idle_timeout_second, 300, "alias idle_timeout_second");
DEFINE_bool(brpc_log_idle_connection_close, true,
            "alias log_idle_connection_close");

DEFINE_string(cache_group, "",
              "Cache group name to use, empty means not use cache group");

DEFINE_bool(block_prefetch, false,
            "whether prefetching whole block from remote");
DEFINE_validator(block_prefetch, brpc::PassValidate);

RemoteBlockCacheImpl::RemoteBlockCacheImpl(StorageClient* storage_client)
    : running_(false),
      upstream_(std::make_unique<Upstream>()),
      retriever_(
          std::make_unique<CacheRetriever>(upstream_.get(), storage_client)),
      joiner_(std::make_unique<iutil::BthreadJoiner>()),
      vars_(std::make_unique<RemoteBlockCacheVarsCollector>()) {
  brpc::FLAGS_idle_timeout_second = FLAGS_brpc_idle_timeout_second;
  brpc::FLAGS_log_idle_connection_close = FLAGS_brpc_log_idle_connection_close;
}

Status RemoteBlockCacheImpl::Start() {
  CHECK(!FLAGS_cache_group.empty());

  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteBlockCache already started";
    return Status::OK();
  }

  LOG(INFO) << "RemoteBlockCache is starting...";

  joiner_->Start();
  upstream_->Start();
  retriever_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "RemoteBlockCache is up";
  return Status::OK();
}

Status RemoteBlockCacheImpl::Shutdown() {
  if (!running_.exchange(false, std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteBlockCache already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "RemoteBlockCache is shutting down...";

  retriever_->Shutdown();
  upstream_->Shutdown();
  joiner_->Shutdown();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "RemoteBlockCache is down";
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(ContextSPtr /*ctx*/, const BlockKey& key,
                                 const Block& block, PutOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = upstream_->SendPutRequest(key, block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to remote cache";
  }
  return status;
}

Status RemoteBlockCacheImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                   off_t offset, size_t length,
                                   IOBuffer* buffer, RangeOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

  BRPC_SCOPE_EXIT {
    if (ctx->GetCacheHit()) {
      vars_->cache_hit_count << 1;
    } else {
      vars_->cache_miss_count << 1;
    }
  };

  Status status;
  if (FLAGS_block_prefetch) {
    status = retriever_->Range(key, offset, length, option.block_whole_length,
                               buffer);
  } else {
    status = upstream_->SendRangeRequest(key, offset, length, buffer,
                                         option.block_whole_length);
  }

  if (!status.ok()) {
    LOG(ERROR) << "Fail to range block from remote cache";
  }
  return status;
}

Status RemoteBlockCacheImpl::Cache(ContextSPtr /*ctx*/, const BlockKey& key,
                                   const Block& block, CacheOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = upstream_->SendCacheRequest(key, block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block to remote cache";
  }
  return status;
}

Status RemoteBlockCacheImpl::Prefetch(ContextSPtr /*ctx*/, const BlockKey& key,
                                      size_t length,
                                      PrefetchOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = upstream_->SendPrefetchRequest(key, length);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to submit prefetch task to remote cache";
  }
  return status;
}

void RemoteBlockCacheImpl::AsyncPut(ContextSPtr ctx, const BlockKey& key,
                                    const Block& block, AsyncCallback cb,
                                    PutOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

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

void RemoteBlockCacheImpl::AsyncRange(ContextSPtr ctx, const BlockKey& key,
                                      off_t offset, size_t length,
                                      IOBuffer* buffer, AsyncCallback cb,
                                      RangeOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

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

void RemoteBlockCacheImpl::AsyncCache(ContextSPtr ctx, const BlockKey& key,
                                      const Block& block, AsyncCallback cb,
                                      CacheOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, ctx, key, block, cb, option]() {
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
  DCHECK_RUNNING("RemoteBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, ctx, key, length, cb, option]() {
    Status status = self->Prefetch(ctx, key, length, option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

}  // namespace cache
}  // namespace dingofs
