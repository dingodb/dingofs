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

#include "client/vfs/data/flat/flat_file.h"

#include <cstdint>
#include <memory>
#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

void FlatFile::FillChunk(uint64_t index, std::vector<Slice> chunk_slices) {
  auto chunk = std::make_unique<FlatFileChunk>(
      fs_id_, ino_, index, chunk_size_, block_size_, std::move(chunk_slices));

  auto it = chunk_map_.find(index);
  if (it == chunk_map_.end()) {
    chunk_map_.emplace(index, std::move(chunk));
    return;
  }

  LOG(WARNING) << "Chunk with index " << index
               << " already exists in the chunk map, will be replaced.";
  it->second = std::move(chunk);
}

std::vector<BlockReadReq> FlatFile::GenBlockReadReqs() const {
  std::vector<BlockReadReq> block_reqs;
  for (const auto& [index, chunk] : chunk_map_) {
    auto chunk_block_reqs = chunk->GenBlockReadReqs();
    block_reqs.insert(block_reqs.end(),
                      std::make_move_iterator(chunk_block_reqs.begin()),
                      std::make_move_iterator(chunk_block_reqs.end()));
  }
  return block_reqs;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs