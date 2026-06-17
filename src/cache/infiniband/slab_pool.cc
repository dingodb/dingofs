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
 * Created Date: 2026-04-22
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/slab_pool.h"

#include <glog/logging.h>

#include <cstddef>
#include <mutex>

#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {
namespace infiniband {

constexpr size_t kSlabBufferSize = 4 * 1024 * 1024;

static SlabPool g_read_slab_pool;
static SlabPool g_write_slab_pool;

SlabPool::SlabPool() = default;

SlabPool::~SlabPool() = default;

void SlabPool::Init(size_t buffer_count) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (memory_pool_ != nullptr) {
    return;
  }

  CHECK_GT(buffer_count, 0);
  memory_pool_ = MemoryPool::Create(kSlabBufferSize, buffer_count);
  CHECK_NOTNULL(memory_pool_);

  buffers_.reserve(buffer_count);
  for (size_t i = 0; i < buffer_count; ++i) {
    RDMABuffer buffer;
    buffer.data = memory_pool_->base() + (i * kSlabBufferSize);
    buffer.capacity = kSlabBufferSize;
    buffer.index = static_cast<uint32_t>(i);
    buffer.lkey = meta_;
    buffers_.emplace_back(buffer);
  }
}

void SlabPool::EnsureInitialized() {
  CHECK(memory_pool_ != nullptr)
      << "SlabPool is used before Init(); call InitializeGlobalSlabPool() first";
}

void SlabPool::SetMeta(uint32_t meta) {
  EnsureInitialized();
  meta_ = meta;
  for (auto& buffer : buffers_) {
    buffer.lkey = meta;
  }
}

RDMABuffer* SlabPool::Alloc(size_t size) {
  EnsureInitialized();
  if (size > kSlabBufferSize) {
    return nullptr;
  }

  auto* data = memory_pool_->Require();
  if (data == nullptr) {
    return nullptr;
  }
  return &buffers_[memory_pool_->IndexOf(data)];
}

void SlabPool::Free(RDMABuffer* buffer) {
  DCHECK_NOTNULL(buffer);
  EnsureInitialized();
  memory_pool_->Release(buffer->data);
}

int SlabPool::IndexOf(void* data) {
  EnsureInitialized();
  return static_cast<int>(memory_pool_->IndexOf(static_cast<char*>(data)));
}

int SlabPool::IndexOf(RDMABuffer* buffer) {
  DCHECK_NOTNULL(buffer);
  return static_cast<int>(buffer->index);
}

bool SlabPool::Contains(void* data) {
  EnsureInitialized();
  char* base = memory_pool_->base();
  return data >= base && data < base + ByteSize();
}

std::vector<iovec> SlabPool::Fetch() {
  EnsureInitialized();

  std::vector<iovec> iovecs;
  iovecs.reserve(buffers_.size());
  for (auto& buffer : buffers_) {
    iovecs.emplace_back(iovec{buffer.data, kSlabBufferSize});
  }
  return iovecs;
}

char* SlabPool::Base() {
  EnsureInitialized();
  return memory_pool_->base();
}

size_t SlabPool::BufferSize() {
  EnsureInitialized();
  return memory_pool_->buffer_size();
}

size_t SlabPool::BufferCount() {
  EnsureInitialized();
  return memory_pool_->buffer_count();
}

size_t SlabPool::ByteSize() {
  EnsureInitialized();
  return BufferSize() * BufferCount();
}

void InitializeGlobalSlabPool(size_t buffer_count) {
  g_read_slab_pool.Init(buffer_count);
  g_write_slab_pool.Init(buffer_count);
}

SlabPool& GetGlobalReadSlabPool() { return g_read_slab_pool; }
SlabPool& GetGlobalWriteSlabPool() { return g_write_slab_pool; }

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
