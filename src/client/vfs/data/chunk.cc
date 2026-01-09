/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License";
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

#include "client/vfs/data/chunk.h"

#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string Chunk::UUID() const { return fmt::format("{}-{}", ino, index); }

std::string Chunk::ToString() const {
  return fmt::format(
      "(fs_id={}, ino={}, index={}, chunk_size={}, block_size={}, "
      "chunk_start={}, chunk_end={})",
      fs_id, ino, index, chunk_size, block_size, chunk_start, chunk_end);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs