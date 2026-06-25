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

#include "cache/remote/remote_block_cache.h"

#include <brpc/reloadable_flags.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <utility>

#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/remote/remote_cache_cluster.h"
#include "common/io_buffer.h"
#include "common/options/cache.h"
#include "common/status.h"

namespace brpc {
DECLARE_int32(idle_timeout_second);
DECLARE_bool(log_idle_connection_close);
}  // namespace brpc

namespace dingofs {
namespace cache {

DEFINE_int32(brpc_idle_timeout_second, 300,
             "cache-specific alias for brpc::idle_timeout_second");
DEFINE_bool(brpc_log_idle_connection_close, true,
            "cache-specific alias for brpc::log_idle_connection_close");

DEFINE_string(cache_group, "",
              "cache group name used by clients; empty disables the remote "
              "cache tier");

RemoteBlockCache::RemoteBlockCache(StorageClient* /*storage_client*/)
    : running_(false),
      cluster_(std::make_unique<RemoteCacheCluster>()),
      joiner_(std::make_unique<iutil::BthreadJoiner>()),
      vars_(std::make_unique<RemoteBlockCacheMetrics>()) {
  brpc::FLAGS_idle_timeout_second = FLAGS_brpc_idle_timeout_second;
  brpc::FLAGS_log_idle_connection_close = FLAGS_brpc_log_idle_connection_close;
}

Status RemoteBlockCache::Start() {
  CHECK(!FLAGS_cache_group.empty());

  if (running_.load(std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteBlockCache already started";
    return Status::OK();
  }

  LOG(INFO) << "RemoteBlockCache is starting...";

  joiner_->Start();
  cluster_->Start();

  running_.store(true, std::memory_order_relaxed);
  LOG(INFO) << "RemoteBlockCache is up";
  return Status::OK();
}

Status RemoteBlockCache::Shutdown() {
  if (!running_.exchange(false, std::memory_order_relaxed)) {
    LOG(WARNING) << "RemoteBlockCache already shutdown";
    return Status::OK();
  }

  LOG(INFO) << "RemoteBlockCache is shutting down...";

  cluster_->Shutdown();
  joiner_->Shutdown();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "RemoteBlockCache is down";
  return Status::OK();
}

Status RemoteBlockCache::Put(BlockHandle handle, IOBuffer block,
                             PutOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = cluster_->SendPutRequest(handle, block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to remote cache";
  }
  return status;
}

Status RemoteBlockCache::Range(BlockHandle handle, off_t offset, size_t length,
                               IOBuffer* buffer, RangeOption option) {
  DCHECK_RUNNING("RemoteBlockCache");
  CHECK_NOTNULL(buffer);
  CHECK_EQ(buffer->BackingBlockNum(), 1);

  bool cache_hit = false;
  auto status = cluster_->SendRangeRequest(
      handle, offset, length, buffer, option.block_whole_length, &cache_hit);
  if (status.ok()) {
    if (cache_hit) {
      vars_->cache_hit_count << 1;
    } else {
      vars_->cache_miss_count << 1;
    }
  } else {
    LOG(ERROR) << "Fail to range block from remote cache";
  }
  return status;
}

Status RemoteBlockCache::Cache(BlockHandle handle, IOBuffer block,
                               CacheOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = cluster_->SendCacheRequest(handle, block);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block to remote cache";
  }
  return status;
}

Status RemoteBlockCache::Prefetch(BlockHandle handle, size_t length,
                                  PrefetchOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = cluster_->SendPrefetchRequest(handle, length);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to submit prefetch task to remote cache";
  }
  return status;
}

void RemoteBlockCache::AsyncPut(BlockHandle handle, IOBuffer block,
                                AsyncCallback cb, PutOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

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

void RemoteBlockCache::AsyncRange(BlockHandle handle, off_t offset,
                                  size_t length, IOBuffer* buffer,
                                  AsyncCallback cb, RangeOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

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

void RemoteBlockCache::AsyncCache(BlockHandle handle, IOBuffer block,
                                  AsyncCallback cb, CacheOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread([self, handle = std::move(handle),
                                  block = std::move(block), cb,
                                  option]() mutable {
    Status status = self->Cache(std::move(handle), std::move(block), option);
    if (cb) {
      cb(status);
    }
  });

  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

void RemoteBlockCache::AsyncPrefetch(BlockHandle handle, size_t length,
                                     AsyncCallback cb, PrefetchOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto* self = GetSelfPtr();
  auto tid = iutil::RunInBthread(
      [self, handle = std::move(handle), length, cb, option]() mutable {
        Status status = self->Prefetch(std::move(handle), length, option);
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
