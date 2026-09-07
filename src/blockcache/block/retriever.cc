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

#include "blockcache/block/retriever.h"

#include <glog/logging.h>

#include <utility>

#include "blockcache/common/route.h"
#include "blockcache/core/runtime/smp.h"

namespace dingofs {
namespace blockcache {

Future<StatusOr<SharedBlock>> InflightTracker::GetOrCreate(BlockHandle handle,
                                                           bool* created) {
  const auto [it, first] = inflight_.try_emplace(handle);
  *created = first;
  if (!it->second.block.empty()) {  // downloaded, still being cached
    return MakeReadyFuture<StatusOr<SharedBlock>>(it->second.block);
  }
  it->second.waiters.emplace_back();
  return it->second.waiters.back().GetFuture();
}

bool InflightTracker::Create(BlockHandle handle) {
  return inflight_.try_emplace(handle).second;
}

InflightTracker::Waiters InflightTracker::Complete(BlockHandle handle,
                                                   SharedBlock block) {
  const auto it = inflight_.find(handle);
  if (it == inflight_.end()) {
    return {};
  }
  it->second.block = std::move(block);
  return std::move(it->second.waiters);
}

InflightTracker::Waiters InflightTracker::TakeWaiters(BlockHandle handle) {
  const auto it = inflight_.find(handle);
  if (it == inflight_.end()) {
    return {};
  }
  Waiters waiters = std::move(it->second.waiters);
  inflight_.erase(it);
  return waiters;
}

void InflightTracker::Erase(BlockHandle handle) { inflight_.erase(handle); }

ObjectRetriever::ObjectRetriever(ObjectStorage* storage, CacheFunc cache_func)
    : storage_(CHECK_NOTNULL(storage)), cache_func_(std::move(cache_func)) {
  CHECK(cache_func_ != nullptr) << "an object retriever needs a cache function";
}

Future<> ObjectRetriever::Start() {
  LOG(INFO) << "ObjectRetriever is starting...";
  LOG(INFO) << "Successfully start ObjectRetriever";
  return MakeReadyFuture<>();
}

Future<> ObjectRetriever::Shutdown() {
  LOG(INFO) << "ObjectRetriever is shutting down...";
  co_await gate_.Close();
  LOG(INFO) << "Successfully shutdown ObjectRetriever";
}

Future<Status> ObjectRetriever::GetPart(BlockHandle handle, uint64_t offset,
                                        uint32_t length, char* buffer) {
  DCHECK_EQ(OwnerShard(handle), ThisShardId()) << "not the owner shard";
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return Status::CacheDown("ObjectRetriever is down");
  }
  co_return co_await storage_->Get(handle, offset, length, buffer);
}

Future<StatusOr<SharedBlock>> ObjectRetriever::GetWholeAndCache(
    BlockHandle handle) {
  DCHECK_EQ(OwnerShard(handle), ThisShardId()) << "not the owner shard";
  bool created = false;
  Future<StatusOr<SharedBlock>> future =
      inflight_.GetOrCreate(handle, &created);
  if (created) {
    StartRetrieval(handle);
  }
  return future;
}

Future<Status> ObjectRetriever::Prefetch(BlockHandle handle) {
  DCHECK_EQ(OwnerShard(handle), ThisShardId()) << "not the owner shard";
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    co_return Status::CacheDown("ObjectRetriever is down");
  }
  if (inflight_.Create(handle)) {
    StartRetrieval(handle);
  }
  co_return Status::OK();
}

void ObjectRetriever::StartRetrieval(BlockHandle handle) {
  Gate::Holder holder(gate_);
  if (!holder.ok()) {
    for (auto& waiter : inflight_.TakeWaiters(handle)) {
      waiter.SetValue(Status::CacheDown("ObjectRetriever is down"));
    }
    return;
  }
  (void)RunRetrieval(handle, std::move(holder));
}

Future<> ObjectRetriever::RunRetrieval(BlockHandle handle,
                                       Gate::Holder /*holder*/) {
  SharedBlock block = SharedBlock::Alloc(handle.size);
  Status status = Status::OutOfMemory("no memory for block download");
  if (!block.empty()) {
    status = co_await storage_->Get(handle, 0, handle.size, block.data());
  }

  // failed
  if (!status.ok()) {
    for (auto& waiter : inflight_.TakeWaiters(handle)) {
      waiter.SetValue(status);
    }
    co_return;
  }

  // success
  for (auto& waiter : inflight_.Complete(handle, block)) {
    waiter.SetValue(block);
  }
  co_await cache_func_(handle, std::move(block));
  inflight_.Erase(handle);
}

}  // namespace blockcache
}  // namespace dingofs
