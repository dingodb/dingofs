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

#ifndef DINGOFS_CACHE_COMMON_BLOCK_HANDLE_HELPER_H_
#define DINGOFS_CACHE_COMMON_BLOCK_HANDLE_HELPER_H_

#include <glog/logging.h>

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "common/block/block_handle.h"
#include "common/block/block_key.h"
#include "common/block/tensor_key.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

// Try to parse a file-block filename ({id}_{index}_{size}) into BlockKey.
inline bool ParseFromFilename(std::string_view filename, BlockKey* key) {
  uint64_t id = 0;
  uint32_t index = 0;
  uint32_t size = 0;
  // std::string_view may not be null-terminated, copy to std::string.
  std::string s(filename);
  int n = std::sscanf(s.c_str(), "%lu_%u_%u", &id, &index, &size);
  if (n != 3) {
    return false;
  }
  *key = BlockKey(id, index, size);
  return true;
}

// Try to parse a tensor filename ({model_name}@{world_size}@{worker_id}
// @{chunk_hash}@{dtype}) into TensorKey.
inline bool ParseFromFilename(std::string_view filename, TensorKey* key) {
  std::vector<std::string> parts;
  parts.reserve(5);
  size_t pos = 0;
  while (parts.size() < 5) {
    auto next = filename.find('@', pos);
    if (next == std::string_view::npos) {
      parts.emplace_back(filename.substr(pos));
      break;
    }
    parts.emplace_back(filename.substr(pos, next - pos));
    pos = next + 1;
  }
  if (parts.size() != 5 || filename.find('@', pos) != std::string_view::npos) {
    return false;
  }

  auto parse_u32 = [](const std::string& s, uint32_t* out) -> bool {
    if (s.empty()) return false;
    char* end = nullptr;
    errno = 0;
    unsigned long v = std::strtoul(s.c_str(), &end, 10);
    if (errno != 0 || end != s.c_str() + s.size() || v > UINT32_MAX) {
      return false;
    }
    *out = static_cast<uint32_t>(v);
    return true;
  };

  uint32_t world_size = 0;
  uint32_t worker_id = 0;
  if (!parse_u32(parts[1], &world_size) || !parse_u32(parts[2], &worker_id)) {
    return false;
  }
  *key = TensorKey(parts[0], world_size, worker_id, parts[3], parts[4]);
  return true;
}

inline pb::cache::BlockKey ToPB(const BlockKey& key) {
  pb::cache::BlockKey pb;
  pb.set_id(key.id);
  pb.set_index(key.index);
  pb.set_size(key.size);
  return pb;
}

inline BlockKey FromPB(const pb::cache::BlockKey& pb) {
  return BlockKey(pb.id(), pb.index(), pb.size());
}

inline pb::cache::TensorKey ToPB(const TensorKey& key) {
  pb::cache::TensorKey pb;
  pb.set_model_name(key.model_name);
  pb.set_world_size(key.world_size);
  pb.set_worker_id(key.worker_id);
  pb.set_chunk_hash(key.chunk_hash);
  pb.set_dtype(key.dtype);
  return pb;
}

inline TensorKey FromPB(const pb::cache::TensorKey& pb) {
  return TensorKey(pb.model_name(), pb.world_size(), pb.worker_id(),
                   pb.chunk_hash(), pb.dtype());
}

inline pb::cache::BlockHandle ToHandlePB(const BlockHandle& handle) {
  pb::cache::BlockHandle pb;
  pb.set_fs_id(handle.FsId());
  handle.Visit([&pb](const auto& key) {
    using KeyT = std::decay_t<decltype(key)>;
    if constexpr (std::is_same_v<KeyT, BlockKey>) {
      *pb.mutable_block_key() = ToPB(key);
    } else if constexpr (std::is_same_v<KeyT, TensorKey>) {
      *pb.mutable_tensor_key() = ToPB(key);
    }
  });
  return pb;
}

inline BlockHandle FromHandlePB(const pb::cache::BlockHandle& pb) {
  if (pb.has_block_key()) {
    CHECK(pb.has_fs_id()) << "BlockKey handle must carry fs_id";
    return BlockHandle(pb.fs_id(), FromPB(pb.block_key()));
  }
  return BlockHandle(FromPB(pb.tensor_key()));
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_COMMON_BLOCK_HANDLE_HELPER_H_
