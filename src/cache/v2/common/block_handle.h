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

#ifndef DINGOFS_CACHE_V2_COMMON_BLOCK_HANDLE_H_
#define DINGOFS_CACHE_V2_COMMON_BLOCK_HANDLE_H_

#include <fmt/format.h>

#include <cstddef>
#include <cstdint>
#include <string>

#include "cache/v2/utils/hash.h"
#include "dingofs/cache.pb.h"

namespace dingofs {
namespace cache {
namespace v2 {

struct BlockHandle {
  bool operator==(const BlockHandle& o) const {
    return id == o.id && index == o.index && size == o.size;
  }

  bool operator!=(const BlockHandle& o) const { return !(*this == o); }

  static BlockHandle FromPb(const pb::cache::v2::BlockHandle& key) {
    return {.fs_id = key.fs_id(),
            .id = key.id(),
            .index = key.index(),
            .size = key.size()};
  }

  void ToPb(pb::cache::v2::BlockHandle* pb) const {
    pb->set_fs_id(fs_id);
    pb->set_id(id);
    pb->set_index(index);
    pb->set_size(size);
  }

  uint64_t Hash() const {
    return Mix64(Mix64(id) ^ ((static_cast<uint64_t>(index) << 32) | size));
  }

  std::string StoreKey() const {
    return fmt::format("blocks/{}/{}/{}_{}_{}", id / 1000 / 1000, id / 1000, id,
                       index, size);
  }

  uint64_t fs_id{0};
  uint64_t id{0};  // slice id
  uint32_t index{0};
  uint32_t size{0};
};

struct BlockHandleHash {
  size_t operator()(const BlockHandle& h) const {
    return static_cast<size_t>(h.Hash());
  }
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_COMMON_BLOCK_HANDLE_H_
