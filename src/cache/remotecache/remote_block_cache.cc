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
#include <utility>

#include "cache/common/macro.h"
#include "cache/common/storage_client.h"
#include "cache/remotecache/rdma_buffer_manager.h"
#include "cache/remotecache/rdma_region_registry.h"
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
DEFINE_bool(cache_rdma_put_scatter_read, false,
            "Advertise multi-segment Put sources for server scatter RDMA "
            "reads. When false, multi-segment sources are staged into one "
            "registered buffer before Put.");

namespace {

// A multi-segment source whose every segment is RDMA-registered can be read in
// place by the server (scatter, zero copy); otherwise it must be staged into a
// single registered buffer (one copy).
bool AllSegmentsRegistered(const IOBuffer& block) {
  auto& registry = RdmaRegionRegistry::GetInstance();
  for (const auto& iov : block.Fetch()) {
    if (registry.RkeyFor(iov.iov_base, iov.iov_len) == 0) {
      return false;
    }
  }
  return true;
}

}  // namespace

RemoteBlockCacheImpl::RemoteBlockCacheImpl(StorageClient* /*storage_client*/)
    : running_(false),
      upstream_(std::make_unique<Upstream>()),
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

  upstream_->Shutdown();
  joiner_->Shutdown();

  running_.store(false, std::memory_order_relaxed);
  LOG(INFO) << "RemoteBlockCache is down";
  return Status::OK();
}

Status RemoteBlockCacheImpl::Put(BlockHandle handle, IOBuffer block,
                                 PutOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  // Prepare the upload source so the server can RDMA-read it. A single
  // registered block is zero copy. Multi-segment scatter reads are optional:
  // they avoid one client copy, but a 4 MiB FUSE write-mempool block can carry
  // dozens of regions and force the server to post a long RDMA_READ chain.
  // Stage by default to keep Put as one control region and one server read.
  auto& rdma = RdmaBufferManager::GetInstance();
  IOBuffer source;
  if (!rdma.Enabled()) {
    source = std::move(block);
  } else if (FLAGS_cache_rdma_put_scatter_read &&
             block.ConstIOBuf().backing_block_num() > 1 &&
             AllSegmentsRegistered(block)) {
    source = std::move(block);
  } else {
    source = rdma.EnsureRegistered(block);
  }
  auto status = upstream_->SendPutRequest(handle, source);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to put block to remote cache";
  }
  return status;
}

Status RemoteBlockCacheImpl::Range(BlockHandle handle, off_t offset,
                                   size_t length, IOBuffer* buffer,
                                   RangeOption option) {
  DCHECK_RUNNING("RemoteBlockCache");

  if (buffer == nullptr || !buffer->IsContiguousDest()) {
    return Status::InvalidParam(
        "range requires a caller-allocated contiguous destination");
  }

  Status status;
  bool cache_hit = false;
  auto& rdma = RdmaBufferManager::GetInstance();
  if (rdma.Enabled()) {
    if (buffer->ConstIOBuf().backing_block_num() == 1 &&
        buffer->GetFirstDataMeta() != 0 && buffer->Size() >= length) {
      // The caller's destination is a single registered block (meta carries the
      // rkey, e.g. an LMCache arena slice or a registered FUSE read slot); let
      // the server RDMA-write straight into it -- fully zero copy.
      status =
          upstream_->SendRangeRequest(handle, offset, length, buffer,
                                      option.block_whole_length, &cache_hit);
    } else {
      // Unregistered destination: hand the server a registered pool buffer to
      // RDMA-write into, then copy it into the caller's destination. Only fill
      // on success, so a NotFound leaves `buffer` clean for the storage tier.
      IOBuffer staging = rdma.NewBuffer(length);
      if (staging.Size() == 0) {
        return Status::Internal("alloc rdma range destination failed");
      }
      status =
          upstream_->SendRangeRequest(handle, offset, length, &staging,
                                      option.block_whole_length, &cache_hit);
      if (status.ok()) {
        FillDest(buffer, staging, length);
      }
    }
  } else {
    // brpc: receive the block into a temporary, then copy it into the caller's
    // destination. Only fill on success, so a NotFound leaves `buffer` clean.
    IOBuffer recv;
    status = upstream_->SendRangeRequest(handle, offset, length, &recv,
                                         option.block_whole_length, &cache_hit);
    if (status.ok()) {
      FillDest(buffer, recv, length);
    }
  }
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

Status RemoteBlockCacheImpl::Cache(BlockHandle handle, IOBuffer block,
                                   CacheOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto& rdma = RdmaBufferManager::GetInstance();
  IOBuffer source =
      rdma.Enabled() ? rdma.EnsureRegistered(block) : std::move(block);
  auto status = upstream_->SendCacheRequest(handle, source);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to cache block to remote cache";
  }
  return status;
}

Status RemoteBlockCacheImpl::Prefetch(BlockHandle handle, size_t length,
                                      PrefetchOption /*option*/) {
  DCHECK_RUNNING("RemoteBlockCache");

  auto status = upstream_->SendPrefetchRequest(handle, length);
  if (!status.ok()) {
    LOG(ERROR) << "Fail to submit prefetch task to remote cache";
  }
  return status;
}

void RemoteBlockCacheImpl::AsyncPut(BlockHandle handle, IOBuffer block,
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

void RemoteBlockCacheImpl::AsyncRange(BlockHandle handle, off_t offset,
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

void RemoteBlockCacheImpl::AsyncCache(BlockHandle handle, IOBuffer block,
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

void RemoteBlockCacheImpl::AsyncPrefetch(BlockHandle handle, size_t length,
                                         AsyncCallback cb,
                                         PrefetchOption option) {
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
