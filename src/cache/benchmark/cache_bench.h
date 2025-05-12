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
 * Created Date: 2025-05-10
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_CACHE_BENCH_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_CACHE_BENCH_H_

#include <butil/iobuf.h>

#include <memory>

#include "cache/blockcache/cache_store.h"
#include "cache/common/common.h"
#include "cache/remotecache/remote_block_cache.h"

namespace dingofs {
namespace cache {
namespace benchmark {

using dingofs::cache::blockcache::BlockKey;
using dingofs::cache::remotecache::RemoteBlockCache;

class CacheBench {
 public:
  virtual ~CacheBench() = default;

  virtual Status Init() = 0;

  virtual void Run() = 0;
};

class CacheBenchImpl : public CacheBench {
 public:
  explicit CacheBenchImpl(RemoteBlockCacheOption option);

  Status Init() override;

  void Run() override;

 private:
  Status RangeBlock(const BlockKey& block_key, size_t block_size, off_t offset,
                    size_t length);

  void DumpBlock(const std::string& filepath, butil::IOBuf* buffer,
                 size_t length);

 private:
  RemoteBlockCacheOption option_;
  std::unique_ptr<RemoteBlockCache> remote_block_cache_;
};

}  // namespace benchmark
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_CACHE_BENCH_H_
