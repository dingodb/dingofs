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

#ifndef DINGOFS_CLIENT_BLOCK_STORE_V2_UTIL_H_
#define DINGOFS_CLIENT_BLOCK_STORE_V2_UTIL_H_

#include <cstdint>
#include <type_traits>
#include <vector>

#include "blockcache/common/block_handle.h"
#include "blockcache/core/memory/buffer_view.h"
#include "common/block/block_handle.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

inline Status ToV2Handle(const BlockHandle& in, blockcache::BlockHandle* out) {
  return in.Visit([&](const auto& key) -> Status {
    using KeyType = std::decay_t<decltype(key)>;
    if constexpr (std::is_same_v<KeyType, BlockKey>) {
      *out = blockcache::BlockHandle{.fs_id = in.FsId(),
                                    .id = key.id,
                                    .index = key.index,
                                    .size = key.size};
      return Status::OK();
    } else {
      return Status::NotSupport("tensor block is not supported by blockcache");
    }
  });
}

inline Status BuildBufferViews(const IOBuffer& data,
                               std::vector<blockcache::BufferView>* views) {
  views->clear();
  for (const auto& iov : data.Fetch()) {
    char* base = static_cast<char*>(iov.iov_base);
    size_t len = iov.iov_len;
    if (len == 0) {
      continue;
    }

    if (!views->empty()) {
      auto& last = views->back();
      char* last_end = static_cast<char*>(last.data) + last.size;
      if (last_end == base && last.size + len <= UINT32_MAX) {
        last.size += len;
        continue;
      }
    }

    if (views->size() == blockcache::kMaxBufferViews) {
      return Status::InvalidParam("block has too many buffer segments");
    }
    views->emplace_back(base, static_cast<uint32_t>(len));
  }

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_CLIENT_BLOCK_STORE_V2_UTIL_H_
