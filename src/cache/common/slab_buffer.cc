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

/*
 * Project: DingoFS
 * Created Date: 2026-06-23
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/slab_buffer.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <utility>

#include "common/const.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

SlabBufferPool::SlabBufferPool(MemoryPoolUPtr memory_pool)
    : memory_pool_(std::move(memory_pool)) {
  size_t buffer_size = memory_pool_->BufferSize();
  size_t buffer_count = memory_pool_->BufferCount();
  buffers_.reserve(buffer_count);
  for (size_t i = 0; i < buffer_count; ++i) {
    SlabBuffer buffer;
    buffer.data = memory_pool_->BaseAddr() + (i * buffer_size);
    buffer.capacity = static_cast<uint32_t>(buffer_size);
    buffer.length = 0;
    buffer.lkey = 0;
    buffer.rkey = 0;
    buffer.index = static_cast<uint32_t>(i);
    buffers_.emplace_back(buffer);
  }
}

SlabBufferPoolUPtr SlabBufferPool::Create(size_t buffer_size,
                                          size_t buffer_count) {
  auto memory_pool = MemoryPool::Create(buffer_size, buffer_count);
  if (memory_pool == nullptr) {
    return nullptr;
  }

  LOG(INFO) << "Successfully create SlabBufferPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count << "}";

  return SlabBufferPoolUPtr(new SlabBufferPool(std::move(memory_pool)));
}

SlabBuffer* SlabBufferPool::Alloc() {
  char* addr = memory_pool_->Require();
  if (addr == nullptr) {
    return nullptr;
  }
  return &buffers_[memory_pool_->IndexOf(addr)];
}

void SlabBufferPool::Free(SlabBuffer* buffer) {
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer, buffers_.data());
  DCHECK_LT(buffer, buffers_.data() + buffers_.size());
  memory_pool_->Release(buffer->data);
}

int SlabBufferPool::IndexOf(SlabBuffer* buffer) const {
  return memory_pool_->IndexOf(buffer->data);
}

int SlabBufferPool::IndexOf(const char* data) const {
  const char* base = memory_pool_->BaseAddr();
  if (data < base || data >= base + memory_pool_->TotalSize()) {
    return -1;
  }
  return static_cast<int>((data - base) / memory_pool_->BufferSize());
}

std::vector<iovec> SlabBufferPool::Fetch() const {
  std::vector<iovec> iovecs;
  iovecs.reserve(buffers_.size());
  for (const auto& buffer : buffers_) {
    iovecs.push_back(iovec{buffer.data, buffer.capacity});
  }
  return iovecs;
}

void SlabBufferPool::SetRdmaKeys(uint32_t lkey, uint32_t rkey) {
  for (auto& buffer : buffers_) {
    buffer.lkey = lkey;
    buffer.rkey = rkey;
  }
}

static SlabBufferPoolUPtr g_read_slab_pool;
static SlabBufferPoolUPtr g_write_slab_pool;

SlabBufferPool* GetGlobalReadSlabPool() { return g_read_slab_pool.get(); }
SlabBufferPool* GetGlobalWriteSlabPool() { return g_write_slab_pool.get(); }

void SetGlobalSlabPools(SlabBufferPoolUPtr read, SlabBufferPoolUPtr write) {
  g_read_slab_pool = std::move(read);
  g_write_slab_pool = std::move(write);
}

Status InitializeGlobalSlabPool() {
  static std::once_flag once;
  static Status result;
  std::call_once(once, [] {
    constexpr size_t kSlabBufferSize = 4 * kMiB;
    auto read_pool = SlabBufferPool::Create(kSlabBufferSize, FLAGS_iodepth);
    auto write_pool = SlabBufferPool::Create(kSlabBufferSize, FLAGS_iodepth);
    if (read_pool == nullptr || write_pool == nullptr) {
      result = Status::OutOfMemory("create slab pool failed");
      return;
    }
    SetGlobalSlabPools(std::move(read_pool), std::move(write_pool));
  });
  return result;
}

}  // namespace cache
}  // namespace dingofs
