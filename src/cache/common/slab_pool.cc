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
 * Created Date: 2026-07-23
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/slab_pool.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <utility>

#include "common/options/cache.h"

namespace dingofs {
namespace cache {

void SlabLease::Reset() {
  if (data_ != nullptr) {
    pool_->Free(data_);
    data_ = nullptr;
  }
  pool_ = nullptr;
}

uint32_t SlabLease::lkey() const { return pool_ != nullptr ? pool_->Lkey() : 0; }

uint32_t SlabLease::rkey() const { return pool_ != nullptr ? pool_->Rkey() : 0; }

int SlabLease::index() const {
  return pool_ != nullptr ? pool_->IndexOf(data_) : -1;
}

void SlabLease::MoveInto(IOBuffer* buffer, size_t length) {
  DCHECK(data_ != nullptr);
  SlabPool* pool = pool_;
  char* data = data_;
  buffer->AppendUserDataWithMeta(
      data, length, [pool, data](void*) { pool->Free(data); }, pool->Lkey());
  data_ = nullptr;  // ownership handed to the IOBuffer refcount world
  pool_ = nullptr;
}

SlabPoolUPtr SlabPool::Create(size_t superblock_count) {
  auto alloc = SlabAllocator::Create(superblock_count);
  if (alloc == nullptr) {
    return nullptr;
  }

  LOG(INFO) << "Successfully create SlabPool{superblock_count="
            << superblock_count << "}";
  return SlabPoolUPtr(new SlabPool(std::move(alloc)));
}

SlabLease SlabPool::Acquire(size_t size) {
  return SlabLease(this, alloc_->Alloc(size));  // nullptr => invalid lease
}

void SlabPool::Free(char* data) { alloc_->Free(data); }

int SlabPool::IndexOf(const char* data) const { return alloc_->IndexOf(data); }

std::vector<iovec> SlabPool::Fetch() const {
  size_t count = alloc_->SuperblockCount();
  char* base = alloc_->BaseAddr();
  std::vector<iovec> iovecs;
  iovecs.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    iovecs.push_back(iovec{base + (i * SlabAllocator::kSuperblockSize),
                           SlabAllocator::kSuperblockSize});
  }
  return iovecs;
}

static SlabPoolUPtr g_slab_pool;

SlabPool* GetGlobalSlabPool() { return g_slab_pool.get(); }

Status InitializeGlobalSlabPool() {
  static std::once_flag once;
  static Status result;
  std::call_once(once, [] {
    // One unified pool replaces the former read+write pools; keep the same
    // total footprint (2 x iodepth superblocks). Size classes make small IOs
    // stop wasting a whole 4 MiB slab.
    size_t superblock_count = 2 * static_cast<size_t>(FLAGS_iodepth);
    auto pool = SlabPool::Create(superblock_count);
    if (pool == nullptr) {
      result = Status::OutOfMemory("create slab pool failed");
      return;
    }
    g_slab_pool = std::move(pool);
  });
  return result;
}

}  // namespace cache
}  // namespace dingofs
