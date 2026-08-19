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

#include "blockcache/tier/sharded.h"

#include <glog/logging.h>

#include <utility>

#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

ShardedTierCache::ShardedTierCache(MDSClient* mds_client,
                                   ObjectStorageUPtr storage)
    : mds_client_(mds_client), storage_(std::move(storage)) {}

ShardedTierCache::~ShardedTierCache() { Shutdown(); }

Status ShardedTierCache::Start() {
  CHECK(!running_) << "ShardedTierCache started twice";
  CHECK_GT(ShardCount(), 0u) << "Runtime must be up first";

  LOG(INFO) << "ShardedTierCache is starting...";

  Status status = tiers_.StartOnAllShards(
      [this](unsigned) { return new TierCache(storage_.get(), mds_client_); });
  if (!status.ok()) {
    tiers_.ShutdownOnAllShards();
    return status;
  }

  running_ = true;
  LOG(INFO) << "Successfully start ShardedTierCache{shards=" << ShardCount()
            << "}";
  return Status::OK();
}

void ShardedTierCache::Shutdown() {
  if (!running_) {
    return;
  }

  LOG(INFO) << "ShardedTierCache is shutting down...";

  tiers_.ShutdownOnAllShards();

  running_ = false;
  LOG(INFO) << "Successfully shutdown ShardedTierCache";
}

Future<Status> ShardedTierCache::Put(BlockHandle handle, BufferViews block,
                                     PutOption option) {
  return InvokeOnOwner(
      handle, [=](TierCache& tier) { return tier.Put(handle, block, option); });
}

Future<Status> ShardedTierCache::Get(BlockHandle handle, uint64_t offset,
                                     uint32_t length, char* buffer,
                                     GetOption option) {
  return InvokeOnOwner(handle, [=](TierCache& tier) {
    return tier.Get(handle, offset, length, buffer, option);
  });
}

Future<Status> ShardedTierCache::Prefetch(BlockHandle handle,
                                          PrefetchOption option) {
  return InvokeOnOwner(
      handle, [=](TierCache& tier) { return tier.Prefetch(handle, option); });
}

Future<Status> ShardedTierCache::Delete(BlockHandle handle,
                                        DeleteOption option) {
  return InvokeOnOwner(
      handle, [=](TierCache& tier) { return tier.Delete(handle, option); });
}

Future<CacheStats> ShardedTierCache::GetStats() {
  return tiers_.MapReduce(
      CacheStats{}, [](TierCache& tier) { return tier.GetStats(); },
      [](CacheStats& sum, CacheStats part) { sum.Merge(part); });
}

}  // namespace blockcache
}  // namespace dingofs
