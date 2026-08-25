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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_BUFFER_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_BUFFER_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "blockcache/infiniband/base/buffer_pool.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

struct alignas(8) ReceiveBuffer {
  char* data = nullptr;
  uint32_t size = 0;
  uint16_t index = 0;
  void* conn = nullptr;
};

class ReceiveBufferPool {
 public:
  explicit ReceiveBufferPool(BufferPool* pool) : pool_(pool) {}
  ~ReceiveBufferPool();

  ReceiveBufferPool(const ReceiveBufferPool&) = delete;
  ReceiveBufferPool& operator=(const ReceiveBufferPool&) = delete;

  Status Init(uint32_t buffer_size, uint32_t buffer_count, void* conn);

  ReceiveBuffer& Get(uint32_t index) { return buffers_[index]; }

  uint32_t buffer_count() const {
    return static_cast<uint32_t>(buffers_.size());
  }
  uint32_t lkey() const { return pool_->lkey(); }

 private:
  BufferPool* pool_;
  std::vector<ReceiveBuffer> buffers_;
};

using ReceiveBufferPoolUPtr = std::unique_ptr<ReceiveBufferPool>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_RECEIVE_BUFFER_H_
