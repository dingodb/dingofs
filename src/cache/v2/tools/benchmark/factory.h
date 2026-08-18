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

#ifndef DINGOFS_CACHE_V2_TOOLS_BENCHMARK_FACTORY_H_
#define DINGOFS_CACHE_V2_TOOLS_BENCHMARK_FACTORY_H_

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>

#include "cache/v2/api/cache.h"
#include "cache/v2/common/block_handle.h"
#include "cache/v2/core/memory/buffer_view.h"

namespace dingofs {
namespace cache {
namespace v2 {

class BlockKeyIterator {
 public:
  BlockKeyIterator(uint64_t idx, uint64_t fsid, uint64_t blksize,
                   uint64_t blocks);

  void SeekToFirst();
  bool Valid() const;
  void Next();
  BlockHandle Key() const;

 private:
  static constexpr uint64_t kBlocksPerChunk = 16;

  uint64_t idx_;
  uint64_t fsid_;
  uint64_t blksize_;
  uint64_t blocks_;
  uint64_t chunkid_;
  uint64_t blockidx_;
  uint64_t allocated_;
};

struct Slot {
  char* data = nullptr;
  BufferView view;
  BlockHandle key;
  std::chrono::steady_clock::time_point t0;
};

class TaskFactory {
 public:
  virtual ~TaskFactory() = default;

  virtual bool SubmitTask(Slot* slot, AsyncCallback cb) = 0;
  virtual uint64_t BytesPerOp() const = 0;
};

using TaskFactoryUPtr = std::unique_ptr<TaskFactory>;
using TaskFactorySPtr = std::shared_ptr<TaskFactory>;

class PutTaskFactory final : public TaskFactory {
 public:
  explicit PutTaskFactory(BlockCacheImpl* block_cache);

  bool SubmitTask(Slot* slot, AsyncCallback cb) override;
  uint64_t BytesPerOp() const override;

 private:
  BlockCacheImpl* block_cache_;
};

class GetTaskFactory final : public TaskFactory {
 public:
  explicit GetTaskFactory(BlockCacheImpl* block_cache);

  bool SubmitTask(Slot* slot, AsyncCallback cb) override;
  uint64_t BytesPerOp() const override;

 private:
  BlockCacheImpl* block_cache_;
};

TaskFactoryUPtr NewFactory(BlockCacheImpl* block_cache, const std::string& op);

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_TOOLS_BENCHMARK_FACTORY_H_
