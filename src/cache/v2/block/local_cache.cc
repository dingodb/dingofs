/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "cache/v2/block/local_cache.h"

#include <brpc/reloadable_flags.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <memory>
#include <utility>

#include "cache/v2/common/flag_decls.h"
#include "cache/v2/core/runtime/worker_pool.h"
#include "cache/v2/store/disk_cache_group.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_uint32(max_range_size_kb, 128, "whole-block fetch threshold (KB)");
DEFINE_validator(max_range_size_kb, brpc::PassValidate);

LocalCache::LocalCache(ObjectStorage* storage)
    : store_(
          std::make_unique<DiskCacheGroup>(ParseDiskOptions(FLAGS_cache_dir))),
      uploader_(std::make_unique<Uploader>(store_.get(), storage)),
      object_retriever_(std::make_unique<ObjectRetriever>(
          storage, [this](BlockHandle handle, SharedBlock block) {
            return CacheBlock(handle, std::move(block));
          })) {}

LocalCache::~LocalCache() {
  LOG_IF(WARNING, running_) << "LocalCache destroyed without Shutdown()";
}

Future<> LocalCache::Start() {
  CHECK(!running_) << "LocalCache started twice";

  LOG(INFO) << "LocalCache is starting...";

  co_await uploader_->Start();
  co_await object_retriever_->Start();
  co_await store_->Start(
      [this](BlockHandle handle) { uploader_->Enqueue(handle); });

  running_ = true;
  LOG(INFO) << "Successfully start LocalCache{max_range_size_kb="
            << FLAGS_max_range_size_kb << "}";
}

Future<> LocalCache::Shutdown() {
  if (!running_) {
    co_return;
  }

  LOG(INFO) << "LocalCache is shutting down...";

  co_await object_retriever_->Shutdown();
  co_await uploader_->Shutdown();
  co_await store_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown LocalCache";
}

Future<Status> LocalCache::Put(BlockHandle handle, BufferViews block,
                               PutOption option) {
  if (!running_) {
    co_return Status::CacheDown("LocalCache is down");
  }

  if (!option.stage) {
    co_return co_await store_->Cache(handle, block);
  }

  const Status status = co_await store_->Stage(handle, block);
  if (!status.ok()) {
    uploader_->Enqueue(handle);
  }
  co_return status;
}

Future<Status> LocalCache::Get(BlockHandle handle, uint64_t offset,
                               uint32_t length, char* buffer,
                               GetOption option) {
  if (!running_) {
    co_return Status::CacheDown("LocalCache is down");
  }

  Status status = co_await store_->Load(handle, offset, length, buffer);
  if (status.ok()) {
    MarkHit(option.stats);
    co_return status;
  }
  if (!status.IsNotFound() || !option.retrieve_storage) {
    co_return status;
  }

  if (length < FLAGS_max_range_size_kb * 1024 && length < handle.size) {
    co_return co_await GetPart(handle, offset, length, buffer);
  }
  co_return co_await GetWhole(handle, offset, length, buffer);
}

Future<Status> LocalCache::GetPart(BlockHandle handle, uint64_t offset,
                                   uint32_t length, char* buffer) {
  const Status status =
      co_await object_retriever_->GetPart(handle, offset, length, buffer);
  if (status.ok()) {
    (void)Prefetch(handle);
  }
  co_return status;
}

Future<Status> LocalCache::GetWhole(BlockHandle handle, uint64_t offset,
                                    uint32_t length, char* buffer) {
  StatusOr<SharedBlock> block =
      co_await object_retriever_->GetWholeAndCache(handle);
  if (!block.ok()) {
    co_return block.status();
  }
  co_await CopyBlock(std::move(block).value(), offset, length, buffer);
  co_return Status::OK();
}

Future<> LocalCache::CacheBlock(BlockHandle handle, SharedBlock block) {
  const BufferView whole = block.view();
  const Status status = co_await store_->Cache(handle, {&whole, 1});
  LOG_IF(WARNING, !status.ok())
      << "Fail to cache block into the cache store: " << status.ToString();
}

Future<> LocalCache::CopyBlock(SharedBlock block, uint64_t offset,
                               uint32_t length, char* buffer) {
  const char* from = block.data() + offset;
  WorkerPool* workers = GetGlobalWorkers();
  if (workers == nullptr || !WorkerPool::ShouldOffload(length)) {
    std::memcpy(buffer, from, length);
    co_return;
  }
  workers->CountCopy(length);
  (void)co_await workers->Submit(Lane::kCpu,
                                 [from, buffer, length]() -> Status {
                                   std::memcpy(buffer, from, length);
                                   return Status::OK();
                                 });
}

Future<Status> LocalCache::Prefetch(BlockHandle handle,
                                    PrefetchOption /*option*/) {
  if (!running_) {
    co_return Status::CacheDown("LocalCache is down");
  }

  if (co_await store_->Exists(handle)) {
    co_return Status::OK();
  }
  co_return co_await object_retriever_->Prefetch(handle);
}

Future<CacheStats> LocalCache::GetStats() {
  if (!running_) {
    return MakeReadyFuture<CacheStats>(CacheStats{});
  }
  return store_->GetStats();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
