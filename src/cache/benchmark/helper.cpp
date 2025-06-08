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
 * Created Date: 2025-06-04
 * Author: Jingli Chen (Wine93)
 */

#include "cache/benchmark/helper.h"

#include <optional>

#include "cache/config/config.h"

namespace dingofs {
namespace cache {

extern BenchmarkOption* g_option;

static constexpr uint64_t kBlocksPerChunk = 16;

BlockKeyGenerator::BlockKeyGenerator(uint64_t worker_id, uint64_t blocks)
    : fs_id_(g_option->start_s),
      ino_(worker_id + 1),
      chunkid_(worker_id * blocks + 1),
      block_index_(0),
      allocated_blocks_(0),
      total_blocks_(blocks) {}

std::optional<BlockKey> BlockKeyGenerator::Next() {
  if (allocated_blocks_ == total_blocks_) {
    return std::nullopt;
  }

  BlockKey key(fs_id_, ino_, chunkid_, block_index_++, 0);
  if (block_index_ == kBlocksPerChunk) {
    block_index_ = 0;
    chunkid_++;
  }

  allocated_blocks_++;
  return key;
}

}  // namespace cache
}  // namespace dingofs
