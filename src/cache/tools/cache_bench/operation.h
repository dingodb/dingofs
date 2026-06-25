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

#ifndef DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPERATION_H_
#define DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPERATION_H_

#include <memory>

#include "cache/api/block_cache.h"
#include "cache/tools/cache_bench/options.h"
#include "common/block/block_key.h"

namespace dingofs {
namespace cache {

class Operation {
 public:
  virtual ~Operation() = default;

  virtual Status Run(const BlockKey& key) = 0;
  virtual uint64_t BytesPerOp() const = 0;
};

using OperationUPtr = std::unique_ptr<Operation>;

OperationUPtr NewOperation(BlockCacheSPtr block_cache, const Options& options);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_TOOLS_CACHE_BENCH_OPERATION_H_
