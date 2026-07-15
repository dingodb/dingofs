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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>
#include <utility>

#include "common/const.h"
#include "common/options/cache.h"

namespace dingofs {
namespace cache {

namespace {

constexpr size_t kMinSlabClassSize = 4 * kKiB;

bool IsPowerOfTwo(size_t v) { return v != 0 && (v & (v - 1)) == 0; }

size_t RoundUpPowerOfTwo(size_t v) {
  if (v <= 1) {
    return 1;
  }
  return size_t(1) << (64 - __builtin_clzll(v - 1));
}

std::string ClassLabel(size_t size) {
  if (size >= kMiB) {
    return std::to_string(size / kMiB) + "m";
  }
  return std::to_string(size / kKiB) + "k";
}

}  // namespace

SlabBufferPool::SlabBufferPool(MemoryPoolUPtr memory_pool,
                               const std::string& name)
    : memory_pool_(std::move(memory_pool)),
      buffer_size_(memory_pool_->BufferSize()) {
  size_t buffer_count = memory_pool_->BufferCount();
  buffers_.reserve(buffer_count);
  for (size_t i = 0; i < buffer_count; ++i) {
    SlabBuffer buffer;
    buffer.data = memory_pool_->BaseAddr() + (i * buffer_size_);
    buffer.capacity = static_cast<uint32_t>(buffer_size_);
    buffer.length = 0;
    buffer.lkey = 0;
    buffer.rkey = 0;
    buffer.index = static_cast<uint32_t>(i);
    buffers_.emplace_back(buffer);
  }

  // Size classes need superblocks that split evenly into power-of-two
  // objects; otherwise every allocation takes a whole superblock.
  if (IsPowerOfTwo(buffer_size_) && buffer_size_ > kMinSlabClassSize) {
    size_t num_classes =
        __builtin_ctzll(buffer_size_) - __builtin_ctzll(kMinSlabClassSize);
    classes_.reserve(num_classes);
    for (size_t i = 0; i < num_classes; ++i) {
      classes_.emplace_back(std::make_unique<SizeClass>());
    }
  }

  if (!name.empty()) {
    const std::string prefix = "dingofs_cache_slab_" + name + "_class_";
    superblock_inuse_.expose(prefix + ClassLabel(buffer_size_) + "_inuse");
    for (size_t i = 0; i < classes_.size(); ++i) {
      std::string label = ClassLabel(kMinSlabClassSize << i);
      classes_[i]->inuse.expose(prefix + label + "_inuse");
      classes_[i]->superblocks.expose(prefix + label + "_superblocks");
    }
  }
}

SlabBufferPoolUPtr SlabBufferPool::Create(size_t buffer_size,
                                          size_t buffer_count,
                                          const std::string& name) {
  auto memory_pool = MemoryPool::Create(buffer_size, buffer_count);
  if (memory_pool == nullptr) {
    return nullptr;
  }

  LOG(INFO) << "Successfully create SlabBufferPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count << " name=" << name << "}";

  return SlabBufferPoolUPtr(new SlabBufferPool(std::move(memory_pool), name));
}

SlabBuffer* SlabBufferPool::Alloc() {
  char* addr = memory_pool_->Require();
  if (addr == nullptr) {
    return nullptr;
  }
  superblock_inuse_ << 1;
  return &buffers_[memory_pool_->IndexOf(addr)];
}

SlabBuffer* SlabBufferPool::Alloc(size_t size) {
  if (size > buffer_size_) {
    return nullptr;
  }

  size_t object_size = RoundUpPowerOfTwo(std::max(size, kMinSlabClassSize));
  if (classes_.empty() || object_size >= buffer_size_) {
    return Alloc();
  }
  return AllocFromClass(classes_[ClassIndexOf(object_size)].get(), object_size);
}

SlabBuffer* SlabBufferPool::AllocFromClass(SizeClass* size_class,
                                           size_t object_size) {
  std::lock_guard<bthread::Mutex> lock(size_class->mutex);

  while (!size_class->usable.empty()) {
    uint32_t sb_index = size_class->usable.back();
    auto iter = size_class->slabs.find(sb_index);
    if (iter == size_class->slabs.end() || iter->second->free_objects.empty()) {
      size_class->usable.pop_back();  // reclaimed or drained, drop lazily
      continue;
    }

    auto& slab = *iter->second;
    uint32_t ordinal = slab.free_objects.back();
    slab.free_objects.pop_back();
    ++slab.live;
    size_class->inuse << 1;
    return &slab.objects[ordinal];
  }

  // Carve a fresh superblock for this class.
  char* base = memory_pool_->Require();
  if (base == nullptr) {
    return nullptr;
  }

  auto sb_index = static_cast<uint32_t>(memory_pool_->IndexOf(base));
  size_t object_count = buffer_size_ / object_size;
  auto slab = std::make_unique<ClassSlab>();
  slab->objects.reserve(object_count);
  for (size_t i = 0; i < object_count; ++i) {
    SlabBuffer object;
    object.data = base + (i * object_size);
    object.capacity = static_cast<uint32_t>(object_size);
    object.length = 0;
    object.lkey = lkey_;
    object.rkey = rkey_;
    object.index = sb_index;
    slab->objects.emplace_back(object);
  }
  slab->free_objects.reserve(object_count);
  for (size_t i = object_count; i > 1; --i) {
    slab->free_objects.push_back(static_cast<uint32_t>(i - 1));
  }
  slab->live = 1;

  SlabBuffer* out = slab->objects.data();
  size_class->slabs.emplace(sb_index, std::move(slab));
  size_class->usable.push_back(sb_index);
  size_class->superblocks << 1;
  size_class->inuse << 1;
  return out;
}

void SlabBufferPool::Free(SlabBuffer* buffer) {
  DCHECK(buffer != nullptr);
  if (buffer->capacity == buffer_size_) {  // superblock descriptor
    DCHECK_GE(buffer, buffers_.data());
    DCHECK_LT(buffer, buffers_.data() + buffers_.size());
    memory_pool_->Release(buffer->data);
    superblock_inuse_ << -1;
    return;
  }
  FreeToClass(buffer);
}

void SlabBufferPool::FreeToClass(SlabBuffer* buffer) {
  int class_index = ClassIndexOf(buffer->capacity);
  DCHECK_GE(class_index, 0);
  DCHECK_LT(class_index, static_cast<int>(classes_.size()));
  auto& size_class = *classes_[class_index];

  auto sb_index =
      static_cast<uint32_t>((buffer->data - BaseAddr()) / buffer_size_);

  char* superblock = nullptr;
  {
    std::lock_guard<bthread::Mutex> lock(size_class.mutex);
    auto iter = size_class.slabs.find(sb_index);
    DCHECK(iter != size_class.slabs.end());
    auto& slab = *iter->second;

    auto ordinal = static_cast<uint32_t>(buffer - slab.objects.data());
    DCHECK_LT(ordinal, slab.objects.size());
    DCHECK_GT(slab.live, 0);

    slab.free_objects.push_back(ordinal);
    size_class.inuse << -1;
    if (--slab.live == 0) {
      // Whole superblock free: hand it back so other classes can reuse it.
      superblock = BaseAddr() + (static_cast<size_t>(sb_index) * buffer_size_);
      size_class.slabs.erase(iter);
      size_class.superblocks << -1;
    } else if (slab.free_objects.size() == 1) {
      size_class.usable.push_back(sb_index);  // was full, usable again
    }
  }

  if (superblock != nullptr) {
    memory_pool_->Release(superblock);
  }
}

int SlabBufferPool::ClassIndexOf(size_t object_size) const {
  DCHECK(IsPowerOfTwo(object_size));
  DCHECK_GE(object_size, kMinSlabClassSize);
  DCHECK_LT(object_size, buffer_size_);
  return __builtin_ctzll(object_size) - __builtin_ctzll(kMinSlabClassSize);
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
  lkey_ = lkey;
  rkey_ = rkey;
  for (auto& buffer : buffers_) {
    buffer.lkey = lkey;
    buffer.rkey = rkey;
  }
  // Objects carved before registration must pick up the keys too.
  for (auto& size_class : classes_) {
    std::lock_guard<bthread::Mutex> lock(size_class->mutex);
    for (auto& [sb_index, slab] : size_class->slabs) {
      for (auto& object : slab->objects) {
        object.lkey = lkey;
        object.rkey = rkey;
      }
    }
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
    auto read_pool =
        SlabBufferPool::Create(kSlabBufferSize, FLAGS_iodepth, "read");
    auto write_pool =
        SlabBufferPool::Create(kSlabBufferSize, FLAGS_iodepth, "write");
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
