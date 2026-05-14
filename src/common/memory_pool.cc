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

#include "common/memory_pool.h"

#include <glog/logging.h>
#include <sys/mman.h>

#include <atomic>
#include <thread>

namespace dingofs {

#define HUGE_PAGE_SIZE_2MB (2 * 1024 * 1024)

#define ALIGN_TO_PAGE_2MB(x) \
  (((x) + (HUGE_PAGE_SIZE_2MB - 1)) & ~(HUGE_PAGE_SIZE_2MB - 1))

MemoryPool::MemoryPool(char* base, size_t buffer_size, size_t buffer_count)
    : base_(base), buffer_size_(buffer_size), buffer_count_(buffer_count) {
  // Shard k owns indices {idx | idx % kNumShards == k}. Walk indices in
  // reverse so each shard's head ends up at its smallest index.
  uint32_t heads[kNumShards];
  for (auto& h : heads) h = kNil;
  for (size_t i = buffer_count; i > 0; --i) {
    uint32_t idx = static_cast<uint32_t>(i - 1);
    uint32_t k = idx % kNumShards;
    NextOf(idx) = heads[k];
    heads[k] = idx;
  }
  for (uint32_t k = 0; k < kNumShards; ++k) {
    shards_[k].head.store(Pack(heads[k], 0), std::memory_order_relaxed);
  }
}

MemoryPool::~MemoryPool() {
  if (base_ != nullptr) {
    HugePagesFree(base_);
    base_ = nullptr;
  }
}

MemoryPoolUPtr MemoryPool::Create(size_t buffer_size, size_t buffer_count) {
  CHECK_GE(buffer_size, sizeof(uint32_t))
      << "buffer_size must be >= 4 bytes (intrusive freelist next pointer)";
  CHECK_GT(buffer_count, 0u);
  CHECK_LT(buffer_count, static_cast<size_t>(kNil));

  size_t bytes = buffer_size * buffer_count;
  void* base = HugePagesMalloc(bytes);
  if (base == nullptr) {
    LOG(ERROR) << "Fail to allocate memory for MemoryPool: bytes=" << bytes;
    return nullptr;
  }

  LOG(INFO) << "Successfully create MemoryPool{buffer_size=" << buffer_size
            << " buffer_count=" << buffer_count << " shards=" << kNumShards
            << " caches=" << kNumCaches << "}";

  return std::make_unique<MemoryPool>(static_cast<char*>(base), buffer_size,
                                      buffer_count);
}

uint32_t MemoryPool::ThreadSlot() {
  thread_local const uint32_t slot = []() {
    static std::atomic<uint32_t> counter{0};
    return counter.fetch_add(1, std::memory_order_relaxed);
  }();
  return slot;
}

// Pop path: single CAS attempt on this shard. On contention or empty, returns
// nullptr so the caller can immediately try a different shard instead of
// spinning on a contested cache line.
char* MemoryPool::TryRequireFromShard(Shard& shard) {
  uint64_t old_head = shard.head.load(std::memory_order_acquire);
  uint32_t idx = Idx(old_head);
  if (idx == kNil) return nullptr;
  uint32_t next = NextOf(idx);
  uint64_t new_head = Pack(next, Ver(old_head) + 1);
  if (shard.head.compare_exchange_strong(old_head, new_head,
                                         std::memory_order_acquire,
                                         std::memory_order_acquire)) {
    return base_ + (idx * buffer_size_);
  }
  return nullptr;  // contested
}

// Batch-pop up to kRefillBatch buffers from a shard in one CAS by walking
// N-1 next pointers. On CAS failure, skip to next shard (don't re-walk).
void MemoryPool::RefillCacheFromShards(Cache& c, uint32_t start_shard) {
  for (uint32_t i = 0; i < kNumShards; ++i) {
    Shard& s = shards_[(start_shard + i) % kNumShards];
    uint64_t old_head = s.head.load(std::memory_order_acquire);
    uint32_t first = Idx(old_head);
    if (first == kNil) continue;  // shard empty

    // Walk up to kRefillBatch nodes.
    uint32_t last = first;
    uint32_t count = 1;
    uint32_t after = NextOf(first);
    while (after != kNil && count < kRefillBatch) {
      last = after;
      after = NextOf(after);
      ++count;
    }

    uint64_t new_head = Pack(after, Ver(old_head) + 1);
    if (s.head.compare_exchange_strong(old_head, new_head,
                                       std::memory_order_acquire,
                                       std::memory_order_acquire)) {
      // We own chain [first, ..., last]. Pour into cache.
      uint32_t node = first;
      for (uint32_t j = 0; j < count; ++j) {
        c.entries[c.size++] = node;
        node = NextOf(node);
      }
      return;
    }
    // CAS failed: skip to next shard (don't re-walk this one).
  }
}

// Push path: must succeed to avoid leaking buffers, so retry on the home
// shard until the CAS lands.
void MemoryPool::FlushCacheToShards(Cache& c, uint32_t flush_count,
                                    uint32_t home_shard) {
  // Build chain: entries[0] -> entries[1] -> ... -> entries[flush_count-1].
  for (uint32_t i = 0; i + 1 < flush_count; ++i) {
    NextOf(c.entries[i]) = c.entries[i + 1];
  }
  uint32_t chain_first = c.entries[0];
  uint32_t chain_last = c.entries[flush_count - 1];

  Shard& s = shards_[home_shard];
  uint64_t old_head = s.head.load(std::memory_order_relaxed);
  for (;;) {
    NextOf(chain_last) = Idx(old_head);
    uint64_t new_head = Pack(chain_first, Ver(old_head) + 1);
    if (s.head.compare_exchange_weak(old_head, new_head,
                                     std::memory_order_release,
                                     std::memory_order_relaxed)) {
      break;
    }
  }

  for (uint32_t i = 0; i + flush_count < c.size; ++i) {
    c.entries[i] = c.entries[i + flush_count];
  }
  c.size -= flush_count;
}

char* MemoryPool::Require() {
  const uint32_t slot = ThreadSlot();
  const uint32_t my_cache = slot % kNumCaches;
  const uint32_t home_shard = slot % kNumShards;
  Cache& c = caches_[my_cache];

  // Fast path: own cache.
  if (!c.lock.test_and_set(std::memory_order_acquire)) {
    if (c.size == 0) {
      RefillCacheFromShards(c, home_shard);
    }
    if (c.size > 0) {
      uint32_t idx = c.entries[--c.size];
      c.lock.clear(std::memory_order_release);
      return base_ + (idx * buffer_size_);
    }
    c.lock.clear(std::memory_order_release);
    // Cache empty + all shards empty: fall through to steal-from-others.
  } else {
    // Cache contested: try shards directly (one attempt per shard).
    for (uint32_t i = 0; i < kNumShards; ++i) {
      char* buf = TryRequireFromShard(shards_[(home_shard + i) % kNumShards]);
      if (buf != nullptr) return buf;
    }
  }

  // Last resort: steal one buffer from any other cache. Prevents buffers
  // from being stranded in idle threads' caches under near-exhaustion.
  for (uint32_t i = 1; i < kNumCaches; ++i) {
    Cache& other = caches_[(my_cache + i) % kNumCaches];
    if (!other.lock.test_and_set(std::memory_order_acquire)) {
      if (other.size > 0) {
        uint32_t idx = other.entries[--other.size];
        other.lock.clear(std::memory_order_release);
        return base_ + (idx * buffer_size_);
      }
      other.lock.clear(std::memory_order_release);
    }
  }

  return nullptr;
}

void MemoryPool::Release(char* buffer) {
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer, base_);
  DCHECK_LT(buffer, base_ + (buffer_count_ * buffer_size_));
  DCHECK_EQ((buffer - base_) % buffer_size_, 0);

  uint32_t idx = static_cast<uint32_t>((buffer - base_) / buffer_size_);
  const uint32_t slot = ThreadSlot();
  const uint32_t my_cache = slot % kNumCaches;
  const uint32_t home_shard = slot % kNumShards;
  Cache& c = caches_[my_cache];

  if (!c.lock.test_and_set(std::memory_order_acquire)) {
    if (c.size >= kCacheCap) {
      FlushCacheToShards(c, kCacheCap / 2, home_shard);
    }
    c.entries[c.size++] = idx;
    c.lock.clear(std::memory_order_release);
    return;
  }

  // Cache contested: push directly to home shard (must succeed).
  Shard& shard = shards_[home_shard];
  uint64_t old_head = shard.head.load(std::memory_order_relaxed);
  for (;;) {
    NextOf(idx) = Idx(old_head);
    uint64_t new_head = Pack(idx, Ver(old_head) + 1);
    if (shard.head.compare_exchange_weak(old_head, new_head,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
      return;
    }
  }
}

void* MemoryPool::HugePagesMalloc(size_t size) {
  size_t real_size = ALIGN_TO_PAGE_2MB(size) + HUGE_PAGE_SIZE_2MB;
  char* ptr = (char*)mmap(
      nullptr, real_size, PROT_READ | PROT_WRITE,
      MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_HUGETLB, -1, 0);
  if (ptr == MAP_FAILED) {
    ptr = (char*)std::malloc(real_size);
    if (nullptr == ptr) {
      return nullptr;
    }
    real_size = 0;
  }

  *((size_t*)ptr) = real_size;
  return ptr + HUGE_PAGE_SIZE_2MB;
}

void MemoryPool::HugePagesFree(void* ptr) {
  if (nullptr == ptr) {
    return;
  }

  void* real_ptr = (char*)ptr - HUGE_PAGE_SIZE_2MB;
  size_t real_size = *((size_t*)real_ptr);
  CHECK_EQ(real_size % HUGE_PAGE_SIZE_2MB, 0);
  if (real_size != 0) {
    munmap(real_ptr, real_size);
  } else {
    std::free(real_ptr);
  }
}

}  // namespace dingofs
