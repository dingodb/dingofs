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

#include "cache/v2/api/task.h"

#include <utility>

#include "cache/v2/tier/sharded.h"

namespace dingofs {
namespace cache {
namespace v2 {

PutTask::PutTask(BlockHandle handle, BufferViews block, AsyncCallback cb,
                 PutOption option)
    : handle_(handle),
      views_(block.begin(), block.end()),
      cb_(std::move(cb)),
      option_(option) {}

Future<Status> PutTask::RunOnShard(ShardedTierCache& cache) {
  return cache.Put(handle_, BufferViews(views_.data(), views_.size()), option_);
}

void PutTask::RunOnWorker(Status status) { cb_(std::move(status)); }

GetTask::GetTask(BlockHandle handle, uint64_t offset, uint32_t length,
                 char* buffer, AsyncCallback cb, GetOption option)
    : handle_(handle),
      offset_(offset),
      length_(length),
      buffer_(buffer),
      cb_(std::move(cb)),
      option_(option) {}

Future<Status> GetTask::RunOnShard(ShardedTierCache& cache) {
  return cache.Get(handle_, offset_, length_, buffer_, option_);
}

void GetTask::RunOnWorker(Status status) { cb_(std::move(status)); }

PrefetchTask::PrefetchTask(BlockHandle handle, AsyncCallback cb,
                           PrefetchOption option)
    : handle_(handle), cb_(std::move(cb)), option_(option) {}

Future<Status> PrefetchTask::RunOnShard(ShardedTierCache& cache) {
  return cache.Prefetch(handle_, option_);
}

void PrefetchTask::RunOnWorker(Status status) { cb_(std::move(status)); }

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
