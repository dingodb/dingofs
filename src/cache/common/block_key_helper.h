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

#ifndef DINGOFS_CACHE_COMMON_BLOCK_KEY_HELPER_H_
#define DINGOFS_CACHE_COMMON_BLOCK_KEY_HELPER_H_

#include <cstdio>
#include <string_view>

#include "common/block/block_context.h"
#include "dingofs/blockcache.pb.h"

namespace dingofs {
namespace cache {

inline bool ParseFromFilename(std::string_view filename, BlockKey* key) {
  uint64_t id = 0;
  uint32_t index = 0;
  uint32_t size = 0;
  // std::string_view may not be null-terminated, copy to std::string
  std::string s(filename);
  int n = std::sscanf(s.c_str(), "%lu_%u_%u", &id, &index, &size);
  if (n != 3) {
    return false;
  }
  *key = BlockKey(id, index, size);
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

inline pb::cache::BlockContext ToContextPB(const BlockContext& ctx) {
  pb::cache::BlockContext pb;
  *pb.mutable_key() = ToPB(ctx.key);
  pb.set_fs_id(ctx.fs_id);
  return pb;
}

inline BlockContext FromContextPB(const pb::cache::BlockContext& pb) {
  return BlockContext(FromPB(pb.key()), pb.fs_id());
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_COMMON_BLOCK_KEY_HELPER_H_
