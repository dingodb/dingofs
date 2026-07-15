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

#ifndef DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_
#define DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_

#include <bits/types/struct_iovec.h>
#include <bthread/mutex.h>
#include <bvar/bvar.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "common/writemempool/memory_pool.h"

namespace dingofs {
namespace cache {

struct SlabBuffer {
  char* data{nullptr};
  uint32_t capacity{0};
  uint32_t length{0};
  uint32_t lkey{0};
  uint32_t rkey{0};
  uint32_t index{0};
};

class SlabBufferPool;
using SlabBufferPoolUPtr = std::unique_ptr<SlabBufferPool>;

// Size-class slab allocator over one contiguous pre-registered region:
//
//   Superblocks: buffer_count buffers of buffer_size (4 MiB) backed by the
//     lock-free MemoryPool. The region stays a single mmap so RDMA memory
//     registration (one ibv_reg_mr) and io_uring fixed-buffer registration
//     (one iovec per superblock) are untouched.
//
//   Size classes: power-of-two classes from 4 KiB up to buffer_size / 2
//     (4K/8K/.../2M for a 4 MiB superblock). A class carves whole superblocks
//     on demand into equal-sized objects and returns a superblock to the
//     MemoryPool as soon as all of its objects are free, so capacity flows
//     between classes with the workload. Allocations of buffer_size (or from
//     Alloc()) bypass the classes and stay on the lock-free superblock path.
//
// Objects inherit the pool-level lkey/rkey, keep 4 KiB alignment (O_DIRECT
// safe), and report the owning superblock as `index` so io_uring fixed I/O
// keeps working at interior offsets.
class SlabBufferPool {
 public:
  // `name` exposes per-class bvar metrics when non-empty.
  static SlabBufferPoolUPtr Create(size_t buffer_size, size_t buffer_count,
                                   const std::string& name = "");

  // Allocates a whole superblock (lock-free fast path).
  SlabBuffer* Alloc();
  // Size-class allocation: rounds size up to the smallest fitting class and
  // carves superblocks on demand. Returns nullptr when the pool is exhausted
  // or size exceeds buffer_size. Never blocks.
  SlabBuffer* Alloc(size_t size);
  void Free(SlabBuffer* buffer);
  int IndexOf(SlabBuffer* buffer) const;
  int IndexOf(const char* data) const;
  size_t BufferCount() const { return memory_pool_->BufferCount(); }
  std::vector<iovec> Fetch() const;

  char* BaseAddr() const { return memory_pool_->BaseAddr(); }
  size_t TotalSize() const { return memory_pool_->TotalSize(); }
  void SetRdmaKeys(uint32_t lkey, uint32_t rkey);

 private:
  // One superblock carved into equal-sized objects for a class. `objects` is
  // sized once at carve time: outstanding SlabBuffer* stay valid until the
  // slab is reclaimed, which only happens when no object is live.
  struct ClassSlab {
    std::vector<SlabBuffer> objects;
    std::vector<uint32_t> free_objects;  // free ordinals within this slab
    uint32_t live{0};
  };

  struct SizeClass {
    bthread::Mutex mutex;
    // Keyed by superblock index. unique_ptr keeps ClassSlab (and its object
    // descriptors) address-stable across rehash.
    std::unordered_map<uint32_t, std::unique_ptr<ClassSlab>> slabs;
    // Superblocks that may have free objects; entries are validated lazily on
    // Alloc (stale ones from reclaim are simply dropped).
    std::vector<uint32_t> usable;
    bvar::Adder<int64_t> inuse;
    bvar::Adder<int64_t> superblocks;
  };

  SlabBufferPool(MemoryPoolUPtr memory_pool, const std::string& name);

  SlabBuffer* AllocFromClass(SizeClass* size_class, size_t object_size);
  void FreeToClass(SlabBuffer* buffer);
  int ClassIndexOf(size_t object_size) const;

  MemoryPoolUPtr memory_pool_;
  size_t buffer_size_;
  std::vector<SlabBuffer> buffers_;                  // superblock descriptors
  std::vector<std::unique_ptr<SizeClass>> classes_;  // 4K, 8K, ..., 2M
  uint32_t lkey_{0};
  uint32_t rkey_{0};
  bvar::Adder<int64_t> superblock_inuse_;
};

Status InitializeGlobalSlabPool();
SlabBufferPool* GetGlobalReadSlabPool();
SlabBufferPool* GetGlobalWriteSlabPool();
void SetGlobalSlabPools(SlabBufferPoolUPtr read, SlabBufferPoolUPtr write);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_SLAB_BUFFER_H_
