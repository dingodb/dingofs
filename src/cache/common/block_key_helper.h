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
#include <functional>
#include <memory>
#include <string_view>

#include "common/block/block_context.h"
#include "common/block/tensor_key.h"
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

inline pb::cache::BlockContext ToContextPB(const BlockContext& ctx) {
  pb::cache::BlockContext pb;
  pb.set_fs_id(ctx.fs_id);
  if (ctx.tensor_key) {
    // The overlay is currently always a TensorKey; downcast is safe.
    *pb.mutable_tensor_key() =
        ToPB(static_cast<const TensorKey&>(*ctx.tensor_key));
  } else {
    *pb.mutable_block_key() = ToPB(ctx.key);
  }
  return pb;
}

inline BlockContext FromContextPB(const pb::cache::BlockContext& pb) {
  BlockContext ctx;
  ctx.fs_id = pb.fs_id();
  switch (pb.key_case()) {
    case pb::cache::BlockContext::kBlockKey:
      ctx.key = FromPB(pb.block_key());
      break;
    case pb::cache::BlockContext::kTensorKey: {
      auto tkey = std::make_shared<TensorKey>(FromPB(pb.tensor_key()));
      // Fill BlockKey with a deterministic synthetic id so the S3-only call
      // sites that still index by block_ctx.key keep working. Hash by Filename
      // so collisions across tensor chunks are negligible.
      uint64_t synth_id = std::hash<std::string>{}(tkey->Filename());
      ctx.key = BlockKey(synth_id, 0, /*size set by caller from request*/ 0);
      ctx.tensor_key = std::move(tkey);
      break;
    }
    default:
      // No key set — leave defaults; caller is expected to error out.
      break;
  }
  return ctx;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_COMMON_BLOCK_KEY_HELPER_H_
