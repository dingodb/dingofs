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

#include "cache/blockcache/block_cache.h"

namespace dingofs {
namespace cache {

// block key iterator
class BlockKeyIterator {
 public:
  BlockKeyIterator(uint64_t idx, uint64_t fsid, uint64_t ino, uint64_t blksize,
                   uint64_t blocks);

  void SeekToFirst();
  bool Valid() const;
  void Next();
  BlockKey Key() const;

 private:
  static constexpr uint64_t kBlocksPerChunk = 16;

  uint64_t idx_;  // worker index, start with 0
  uint64_t fsid_;
  uint64_t ino_;
  uint64_t blksize_;
  uint64_t blocks_;
  uint64_t chunkid_;
  uint64_t blockidx_;
  uint64_t allocated_;
};

// block
Block NewBlock(uint64_t blksize);

// task factory
using Task = std::function<void()>;

class TaskFactory {
 public:
  virtual ~TaskFactory() = default;

  virtual Task GenTask(const BlockKey& key) = 0;
};

using TaskFactoryUPtr = std::unique_ptr<TaskFactory>;
using TaskFactorySPtr = std::shared_ptr<TaskFactory>;

class PutTaskFactory final : public TaskFactory {
 public:
  PutTaskFactory(BlockCacheSPtr block_cache);

  Task GenTask(const BlockKey& key) override;

 private:
  void Put(const BlockKey& key);

  BlockCacheSPtr block_cache_;
  Block block_;
};

class RangeTaskFactory final : public TaskFactory {
 public:
  explicit RangeTaskFactory(BlockCacheSPtr block_cache);

  Task GenTask(const BlockKey& key) override;

 private:
  void Range(const BlockKey& key);

  BlockCacheSPtr block_cache_;
};

TaskFactoryUPtr NewFactory(BlockCacheSPtr block_cache, const std::string& op);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_FACTORY_H_
