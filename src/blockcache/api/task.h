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

#ifndef DINGOFS_BLOCKCACHE_API_TASK_H_
#define DINGOFS_BLOCKCACHE_API_TASK_H_

#include <cstdint>
#include <functional>
#include <vector>

#include "blockcache/block/block_cache.h"
#include "blockcache/common/block_handle.h"
#include "blockcache/core/memory/buffer_view.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {

class ShardedTierCache;

using AsyncCallback = std::function<void(Status)>;

class AsyncTask {
 public:
  virtual ~AsyncTask() = default;
  AsyncTask() = default;

  AsyncTask(const AsyncTask&) = delete;
  AsyncTask& operator=(const AsyncTask&) = delete;

  virtual Future<Status> RunOnShard(ShardedTierCache& cache) = 0;
  virtual void RunOnWorker(Status status) = 0;
};

class PutTask final : public AsyncTask {
 public:
  PutTask(BlockHandle handle, BufferViews block, AsyncCallback cb,
          PutOption option);
  PutTask(const PutTask&) = delete;
  PutTask& operator=(const PutTask&) = delete;

  Future<Status> RunOnShard(ShardedTierCache& cache) override;
  void RunOnWorker(Status status) override;

 private:
  BlockHandle handle_;
  std::vector<BufferView> views_;
  AsyncCallback cb_;
  PutOption option_;
};

class GetTask final : public AsyncTask {
 public:
  GetTask(BlockHandle handle, uint64_t offset, uint32_t length, char* buffer,
          AsyncCallback cb, GetOption option);

  Future<Status> RunOnShard(ShardedTierCache& cache) override;
  void RunOnWorker(Status status) override;

 private:
  BlockHandle handle_;
  uint64_t offset_;
  uint32_t length_;
  char* buffer_;
  AsyncCallback cb_;
  GetOption option_;
};

class PrefetchTask final : public AsyncTask {
 public:
  PrefetchTask(BlockHandle handle, AsyncCallback cb, PrefetchOption option);

  Future<Status> RunOnShard(ShardedTierCache& cache) override;
  void RunOnWorker(Status status) override;

 private:
  BlockHandle handle_;
  AsyncCallback cb_;
  PrefetchOption option_;
};

class DeleteTask final : public AsyncTask {
 public:
  DeleteTask(BlockHandle handle, AsyncCallback cb, DeleteOption option);

  Future<Status> RunOnShard(ShardedTierCache& cache) override;
  void RunOnWorker(Status status) override;

 private:
  BlockHandle handle_;
  AsyncCallback cb_;
  DeleteOption option_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_API_TASK_H_
