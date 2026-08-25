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

#include "blockcache/infiniband/connection/receive_buffer.h"

#include <cerrno>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

ReceiveBufferPool::~ReceiveBufferPool() {
  for (auto& buffer : buffers_) {
    if (buffer.data != nullptr) {
      pool_->Free(buffer.data);
    }
  }
}

Status ReceiveBufferPool::Init(uint32_t buffer_size, uint32_t buffer_count,
                               void* conn) {
  buffers_.resize(buffer_count);
  for (uint32_t i = 0; i < buffer_count; ++i) {
    char* data = pool_->Alloc(buffer_size);
    if (data == nullptr) {
      return ToStatus(ENOMEM, "alloc receive buffer failed");
    }

    buffers_[i].data = data;
    buffers_[i].size = buffer_size;
    buffers_[i].index = i;
    buffers_[i].conn = conn;
  }
  return Status::OK();
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
