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
 * Created Date: 2025-06-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_BENCHMARK_HELPER_H_
#define DINGOFS_SRC_CACHE_BENCHMARK_HELPER_H_

#include <optional>

#include "blockaccess/block_accesser.h"
#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace cache {

blockaccess::BlockAccessOptions NewBlockAccessOptions();

Block NewOneBlock();

class BlockKeyFactory {
 public:
  BlockKeyFactory(uint64_t worker_id, uint64_t blocks);

  std::optional<BlockKey> Next();

 private:
  uint64_t fs_id_;
  uint64_t ino_;
  uint64_t chunkid_;
  uint64_t block_index_;
  uint64_t allocated_blocks_;
  uint64_t total_blocks_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_BENCHMARK_HELPER_H_
