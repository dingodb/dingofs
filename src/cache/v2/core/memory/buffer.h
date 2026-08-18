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

#ifndef DINGOFS_CACHE_V2_CORE_MEMORY_BUFFER_H_
#define DINGOFS_CACHE_V2_CORE_MEMORY_BUFFER_H_

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <utility>

#include "cache/v2/core/memory/buffer_view.h"
#include "cache/v2/core/memory/slab_pool.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {

// Zero-copy handle: 4KiB-aligned memory pre-registered with HCA and io_uring.
// Alloc > 4MiB or pool exhausted returns empty; deliberately NO heap fallback.
class Buffer {
 public:
  // From THIS shard's pool. Must be freed on the same shard.
  static Buffer Alloc(size_t n);

  Buffer() = default;
  ~Buffer() { Reset(); }

  Buffer(const Buffer&) = delete;
  Buffer& operator=(const Buffer&) = delete;

  Buffer(Buffer&& o) noexcept
      : base_(o.base_), data_(o.data_), size_(o.size_), capacity_(o.capacity_) {
    o.base_ = nullptr;
    o.data_ = nullptr;
    o.size_ = 0;
    o.capacity_ = 0;
  }
  Buffer& operator=(Buffer&& o) noexcept {
    if (this != &o) {
      Reset();
      base_ = o.base_;
      data_ = o.data_;
      size_ = o.size_;
      capacity_ = o.capacity_;
      o.base_ = nullptr;
      o.data_ = nullptr;
      o.size_ = 0;
      o.capacity_ = 0;
    }
    return *this;
  }

  char* data() const { return data_; }
  size_t size() const { return size_; }
  bool empty() const { return data_ == nullptr || size_ == 0; }

  // Bytes writable at data(), which the pool's size classes make no smaller
  // than the requested size rounded up to 4 KiB. A block body can therefore
  // be padded out to what the device takes without being copied first.
  size_t writable_bytes() const {
    return data_ == nullptr ? 0
                            : capacity_ - static_cast<size_t>(data_ - base_);
  }

  // Drop bytes off either end of what the buffer shows. Memory does not move
  // -- the pool still frees the whole allocation -- so an io that had to be
  // widened to reach the device can hand the wire the part that was asked
  // for, with no copy in between.
  void PopFront(size_t n) {
    DCHECK(n <= size_) << "pop past the buffer";
    data_ += n;
    size_ -= n;
  }

  void PopBack(size_t n) {
    DCHECK(n <= size_) << "pop past the buffer";
    size_ -= n;
  }

  // Hands the memory to the wire layer, which borrows and never owns.
  BufferView view() const {
    return BufferView(data_, static_cast<uint32_t>(size_));
  }

 private:
  Buffer(char* data, size_t size)
      : base_(data),
        data_(data),
        size_(size),
        capacity_(SlabPool::SizeOf(size)) {}
  void Reset() noexcept;

  char* base_ = nullptr;  // what the pool handed out; PopFront moves data_
  char* data_ = nullptr;
  size_t size_ = 0;      // what the buffer shows
  size_t capacity_ = 0;  // what the pool actually gave, >= AlignUp(size, 4K)
};

// The per-shard pool behind Buffer, held in a thread_local.
class BufferPool {
 public:
  // Creates one pool on every shard; call once, before anything allocates.
  // External thread, after the shards are up.
  static Status InitOnAllShards(size_t bytes_per_shard);

  // Destroys them again. Every Buffer must already be gone.
  static void ShutdownOnAllShards();

  // This shard's pool, or nullptr when none was created.
  static SlabPool* LocalPool();
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_MEMORY_BUFFER_H_
