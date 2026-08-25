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

#include "blockcache/infiniband/connection/send_buffer.h"

#include <cerrno>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

SendBufferPool::~SendBufferPool() {
  for (auto& buffer : buffers_) {
    if (buffer.data != nullptr) {
      pool_->Free(buffer.data);
    }
  }
}

Status SendBufferPool::Init(uint32_t buffer_size, uint32_t buffer_count,
                            void* conn) {
  buffer_size_ = buffer_size;
  buffers_.resize(buffer_count);

  for (uint32_t i = 0; i < buffer_count; ++i) {
    char* data = pool_->Alloc(buffer_size);
    if (data == nullptr) {
      return ToStatus(ENOMEM, "alloc send buffer failed");
    }

    buffers_[i].data = data;
    buffers_[i].length = 0;
    buffers_[i].capacity = buffer_size;
    buffers_[i].conn = conn;
  }

  // add to free list
  for (uint32_t i = buffer_count; i > 0; --i) {
    SendBuffer* buffer = &buffers_[i - 1];
    buffer->next = free_;
    free_ = buffer;
  }

  return Status::OK();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
