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

#include "blockcache/net/rdma/rpc/frame_pool.h"

#include <cerrno>
#include <utility>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {

FramePool::~FramePool() {
  for (auto& buffer : buffers_) {
    if (buffer.frame != nullptr) {
      pool_->Free(buffer.frame);
    }
  }
}

Status FramePool::Init(RdmaConnection* conn, SlabPool* pool, uint32_t lkey,
                       uint32_t count, uint32_t frame_bytes) {
  frame_bytes_ = frame_bytes;
  lkey_ = lkey;
  pool_ = pool;
  buffers_.resize(count);

  for (uint32_t i = 0; i < count; ++i) {
    char* frame = pool->Alloc(frame_bytes);
    if (frame == nullptr) {
      return ToStatus(ENOMEM,
                      "carve a frame buffer from the slab pool: the pool "
                      "holds fewer connections than this shard opened, "
                      "raise RdmaOption::max_connections");
    }
    buffers_[i].conn = conn;
    buffers_[i].frame = frame;
  }
  for (uint32_t i = count; i > 0; --i) {
    FrameBuf* buffer = &buffers_[i - 1];
    buffer->next = free_;
    free_ = buffer;
  }
  return Status::OK();
}

}  // namespace blockcache
}  // namespace dingofs
