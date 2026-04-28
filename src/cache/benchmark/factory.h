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
 * Created Date: 2025-06-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_FACTORY_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_FACTORY_H_

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "cache/blockcache/block_cache.h"
#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

// block key iterator
class BlockKeyIterator {
 public:
  BlockKeyIterator(uint64_t idx, uint64_t blksize, uint64_t blocks);

  void SeekToFirst();
  bool Valid() const;
  void Next();
  BlockKey Key() const;

 private:
  static constexpr uint64_t kBlocksPerChunk = 16;

  uint64_t idx_;  // worker index, start with 0
  uint64_t blksize_;
  uint64_t blocks_;
  uint64_t id_;        // slice id
  uint64_t index_;     // block index within the slice
  uint64_t allocated_;
};

// block
IOBuffer NewBlock(uint64_t blksize);

// task factory
struct TaskResult {
  Status status;
  uint64_t bytes{0};
};

struct TaskContext {
  Status Init(uint64_t worker_idx);
  IOBuffer* RequestBody() { return &request_body; }
  bool VerifyRange(const IOBuffer& buffer, uint64_t length) const;

  // With --bench_rdma_registered_buffers (and --use_rdma), request_body is a
  // single registered buffer from RdmaBufferManager so Put/Cache are zero-copy
  // on the wire; its deleter returns the buffer to the pool on destruction.
  IOBuffer request_body;
  uint64_t worker_idx{0};
};

using Task = std::function<TaskResult()>;

class TaskFactory {
 public:
  virtual ~TaskFactory() = default;

  virtual Task GenTask(const BlockKey& key, TaskContext* context) = 0;
};

using TaskFactoryUPtr = std::unique_ptr<TaskFactory>;
using TaskFactorySPtr = std::shared_ptr<TaskFactory>;

class PutTaskFactory final : public TaskFactory {
 public:
  PutTaskFactory(BlockCacheSPtr block_cache);

  Task GenTask(const BlockKey& key, TaskContext* context) override;

 private:
  TaskResult Put(const BlockKey& key, TaskContext* context);

  BlockCacheSPtr block_cache_;
};

class CacheTaskFactory final : public TaskFactory {
 public:
  explicit CacheTaskFactory(BlockCacheSPtr block_cache);

  Task GenTask(const BlockKey& key, TaskContext* context) override;

 private:
  TaskResult Cache(const BlockKey& key, TaskContext* context);

  BlockCacheSPtr block_cache_;
};

class RangeTaskFactory final : public TaskFactory {
 public:
  explicit RangeTaskFactory(BlockCacheSPtr block_cache);

  Task GenTask(const BlockKey& key, TaskContext* context) override;

 private:
  TaskResult RangeAll(const BlockKey& key, TaskContext* context);
  Status Range(const BlockKey& key, off_t offset, size_t length,
               IOBuffer* buffer);

  BlockCacheSPtr block_cache_;
};

TaskFactoryUPtr NewFactory(BlockCacheSPtr block_cache, const std::string& op);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_FACTORY_H_
