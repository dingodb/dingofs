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

#include "common/writemempool/write_page_pool.h"

#include <glog/logging.h>
#include <sys/mman.h>

#include <algorithm>
#include <cstdlib>
#include <limits>

#include "common/writemempool/cpu_local.h"

namespace dingofs {
namespace {

constexpr size_t kHugePageSize = 2 * 1024 * 1024;

size_t AlignHugePage(size_t size) {
  return (size + kHugePageSize - 1) & ~(kHugePageSize - 1);
}

}  // namespace

WritePagePool::WritePagePool(char* base, size_t page_size, size_t page_count)
    : base_(base),
      page_size_(page_size),
      page_shift_(
          (page_size & (page_size - 1)) == 0 ? __builtin_ctzll(page_size) : 0),
      page_count_(page_count),
      next_(std::make_unique<uint32_t[]>(page_count)) {
  cache_count_ = CpuLocalCacheCount();
  caches_ = std::make_unique<Cache[]>(cache_count_);
  for (uint32_t i = 0; i < cache_count_; ++i) {
    caches_[i].next_refill_shard = i % kNumShards;
  }

  std::array<uint32_t, kNumShards> tails;
  tails.fill(kNil);
  for (uint32_t index = 0; index < page_count_; ++index) {
    const uint32_t owner = OwnerOfIndex(index);
    Shard& shard = shards_[owner];
    if (shard.head == kNil) {
      shard.head = index;
    } else {
      NextOf(tails[owner]) = index;
    }
    tails[owner] = index;
    ++shard.count;
  }
  for (uint32_t owner = 0; owner < kNumShards; ++owner) {
    if (tails[owner] != kNil) NextOf(tails[owner]) = kNil;
  }
}

WritePagePool::~WritePagePool() {
  if (base_ != nullptr) FreeArena(base_);
}

WritePagePoolUPtr WritePagePool::Create(size_t page_size, size_t page_count) {
  CHECK_GT(page_size, 0);
  CHECK_GT(page_count, 0);
  CHECK_LE(page_count,
           static_cast<size_t>(std::numeric_limits<uint32_t>::max() - 1));
  CHECK_LE(page_count, std::numeric_limits<size_t>::max() / page_size);

  const size_t arena_size = page_size * page_count;
  constexpr size_t kMaxArenaOverhead = (2 * kHugePageSize) - 1;
  CHECK_LE(arena_size, std::numeric_limits<size_t>::max() - kMaxArenaOverhead)
      << "write page pool arena size overflows huge-page alignment";

  void* base = AllocateArena(arena_size);
  if (base == nullptr) return nullptr;
  return WritePagePoolUPtr(
      new WritePagePool(static_cast<char*>(base), page_size, page_count));
}

size_t WritePagePool::TakeFromCache(Cache* cache, char** pages, size_t count) {
  const size_t take = std::min(count, static_cast<size_t>(cache->size));
  for (size_t i = 0; i < take; ++i) {
    pages[i] = PageAt(cache->entries[--cache->size]);
  }
  return take;
}

bool WritePagePool::RefillCache(Cache* cache) {
  CHECK_LT(cache->size, kCacheCapacity);
  const uint32_t start = cache->next_refill_shard;
  for (uint32_t offset = 0; offset < kNumShards; ++offset) {
    const uint32_t owner = (start + offset) % kNumShards;
    Shard& shard = shards_[owner];
    std::lock_guard<std::mutex> lock(shard.mutex);
    const size_t take = std::min(
        static_cast<size_t>(kCacheCapacity - cache->size), shard.count);
    if (take == 0) continue;

    for (size_t i = 0; i < take; ++i) {
      const uint32_t index = shard.head;
      CHECK_NE(index, kNil);
      shard.head = NextOf(index);
      cache->entries[cache->size++] = index;
    }
    shard.count -= take;
    cache->next_refill_shard = (owner + 1) % kNumShards;
    return true;
  }
  return false;
}

size_t WritePagePool::RequireBatch(char** pages, size_t count) {
  return RequireBatchFromCache(pages, count,
                               CurrentCpuLocalCache(cache_count_));
}

size_t WritePagePool::RequireBatchFromCache(char** pages, size_t count,
                                            uint32_t cache_index) {
  if (count == 0) return 0;
  CHECK_NOTNULL(pages);
  CHECK_LT(cache_index, cache_count_);

  size_t acquired = 0;
  {
    Cache& cache = caches_[cache_index];
    std::lock_guard<std::mutex> lock(cache.mutex);
    while (acquired < count) {
      acquired += TakeFromCache(&cache, pages + acquired, count - acquired);
      if (acquired == count || !RefillCache(&cache)) break;
    }
  }

  // Shards can be empty while idle cache slots still hold free pages. Reclaim
  // those pages before reporting a short allocation.
  for (uint32_t offset = 1; offset < cache_count_ && acquired < count;
       ++offset) {
    Cache& cache = caches_[(cache_index + offset) % cache_count_];
    std::lock_guard<std::mutex> lock(cache.mutex);
    acquired += TakeFromCache(&cache, pages + acquired, count - acquired);
  }

  return acquired;
}

size_t WritePagePool::TEST_FreePages() const {
  size_t total = 0;
  for (const auto& shard : shards_) {
    std::lock_guard<std::mutex> lock(shard.mutex);
    total += shard.count;
  }
  for (uint32_t i = 0; i < cache_count_; ++i) {
    std::lock_guard<std::mutex> lock(caches_[i].mutex);
    total += caches_[i].size;
  }
  return total;
}

void WritePagePool::ReleaseBatch(char* const* pages, size_t count) {
  if (count == 0) return;
  CHECK_NOTNULL(pages);

  std::array<Chain, kNumShards> chains;
  for (size_t i = 0; i < count; ++i) {
    char* page = pages[i];
    CHECK_NOTNULL(page);
    CHECK_GE(page, base_);
    CHECK_LT(page, base_ + TotalSize());
    CHECK(PageAligned(page - base_));

    const uint32_t index = IndexOf(page);
    Chain& chain = chains[OwnerOfIndex(index)];
    if (chain.first == kNil) {
      chain.first = index;
    } else {
      NextOf(chain.last) = index;
    }
    chain.last = index;
    ++chain.count;
  }

  for (uint32_t owner = 0; owner < kNumShards; ++owner) {
    Chain& chain = chains[owner];
    if (chain.count == 0) continue;
    Shard& shard = shards_[owner];
    std::lock_guard<std::mutex> lock(shard.mutex);
    NextOf(chain.last) = shard.head;
    shard.head = chain.first;
    shard.count += chain.count;
  }
}

void* WritePagePool::AllocateArena(size_t size) {
  const size_t allocation_size = AlignHugePage(size) + kHugePageSize;
  char* allocation = static_cast<char*>(
      mmap(nullptr, allocation_size, PROT_READ | PROT_WRITE,
           MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB, -1, 0));
  if (allocation == MAP_FAILED) {
    void* aligned = nullptr;
    if (posix_memalign(&aligned, kHugePageSize, allocation_size) != 0) {
      return nullptr;
    }
    allocation = static_cast<char*>(aligned);
    *reinterpret_cast<size_t*>(allocation) = 0;
  } else {
    *reinterpret_cast<size_t*>(allocation) = allocation_size;
  }
  return allocation + kHugePageSize;
}

void WritePagePool::FreeArena(void* ptr) {
  char* allocation = static_cast<char*>(ptr) - kHugePageSize;
  const size_t allocation_size = *reinterpret_cast<size_t*>(allocation);
  if (allocation_size == 0) {
    std::free(allocation);
  } else {
    munmap(allocation, allocation_size);
  }
}

}  // namespace dingofs
