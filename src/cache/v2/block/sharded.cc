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

#include "cache/v2/block/sharded.h"

#include <glog/logging.h>

#include <memory>
#include <utility>

#include "cache/v2/common/route.h"
#include "cache/v2/core/runtime/smp.h"

namespace dingofs {
namespace cache {
namespace v2 {

ShardedLocalCache::ShardedLocalCache(MDSClient* mds_client)
    : storage_client_(std::make_unique<StorageClient>(mds_client)),
      storage_(std::make_unique<ObjectStorage>(storage_client_.get())) {}

ShardedLocalCache::ShardedLocalCache(ObjectStorageUPtr storage)
    : storage_(std::move(CHECK_NOTNULL(storage))) {}

ShardedLocalCache::~ShardedLocalCache() { Shutdown(); }

Status ShardedLocalCache::Start() {
  CHECK(!running_) << "ShardedLocalCache started twice";
  CHECK_GT(ShardCount(), 0u) << "Runtime must be up first";

  LOG(INFO) << "ShardedLocalCache is starting...";

  storage_client_->Start();
  // it will invoke LocalCache::Start
  Status status = block_cache_.StartOnAllShards(
      [this](unsigned) { return new LocalCache(storage_.get()); });
  if (!status.ok()) {
    return status;
  }

  running_ = true;
  LOG(INFO) << "Successfully start ShardedLocalCache{shards=" << ShardCount()
            << "}";
  return Status::OK();
}

void ShardedLocalCache::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "ShardedLocalCache is shutting down...";

  block_cache_.ShutdownOnAllShards();  // it will invoke LocalCache::Shutdown
  storage_client_->Shutdown();

  running_ = false;
  LOG(INFO) << "Successfully shutdown ShardedLocalCache";
}

Future<Status> ShardedLocalCache::Put(BlockHandle handle, BufferViews block,
                                      PutOption option) {
  return InvokeOnOwner(handle, [=](LocalCache& cache) {
    return cache.Put(handle, block, option);
  });
}

Future<Status> ShardedLocalCache::Get(BlockHandle handle, uint64_t offset,
                                      uint32_t length, char* buffer,
                                      GetOption option) {
  return InvokeOnOwner(handle, [=](LocalCache& cache) {
    return cache.Get(handle, offset, length, buffer, option);
  });
}

Future<Status> ShardedLocalCache::Prefetch(BlockHandle handle,
                                           PrefetchOption option) {
  return InvokeOnOwner(handle, [=](LocalCache& cache) {
    return cache.Prefetch(handle, option);
  });
}

Future<Status> ShardedLocalCache::Delete(BlockHandle handle,
                                         DeleteOption option) {
  return InvokeOnOwner(handle, [=](LocalCache& cache) {
    return cache.Delete(handle, option);
  });
}

Future<CacheStats> ShardedLocalCache::GetStats() {
  return block_cache_.MapReduce(
      CacheStats{}, [](LocalCache& cache) { return cache.GetStats(); },
      [](CacheStats& sum, CacheStats part) { sum.Merge(part); });
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
