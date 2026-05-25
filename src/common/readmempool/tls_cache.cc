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

#include "common/readmempool/tls_cache.h"

#include <algorithm>

namespace dingofs {

int TlsCacheSet::CapFor(int order) {
  if (order < 0 || order >= kNumOrders) return 0;
  size_t ob = OrderBytes(order);
  if (ob > kTlsBudget) return 0;  // large class: uncached
  size_t cap = kTlsBudget / ob;
  return static_cast<int>(std::min<size_t>(cap, kMaxLane));
}

uint32_t TlsCacheSet::Slot() {
  static std::atomic<uint32_t> counter{0};
  thread_local const uint32_t slot =
      counter.fetch_add(1, std::memory_order_relaxed);
  return slot;
}

uint64_t TlsCacheSet::TryAlloc(int order) {
  int cap = CapFor(order);
  if (cap == 0) return BuddyAllocator::kInvalid;

  Cache& c = caches_[Slot() % kNumCaches];
  if (c.lock.test_and_set(std::memory_order_acquire)) {
    return BuddyAllocator::kInvalid;  // contention -> go to buddy
  }

  uint64_t off = BuddyAllocator::kInvalid;
  if (c.size[order] > 0) {
    off = c.slots[order][--c.size[order]];
  }
  c.lock.clear(std::memory_order_release);
  if (off == BuddyAllocator::kInvalid) {
    miss_ << 1;
  } else {
    hit_ << 1;
  }
  return off;
}

bool TlsCacheSet::TryFree(int order, uint64_t off) {
  int cap = CapFor(order);
  if (cap == 0) return false;
  Cache& c = caches_[Slot() % kNumCaches];
  if (c.lock.test_and_set(std::memory_order_acquire)) {
    return false;  // contention -> free to buddy
  }
  bool ok = false;
  if (c.size[order] < static_cast<uint32_t>(cap)) {
    c.slots[order][c.size[order]++] = off;
    ok = true;
  }
  c.lock.clear(std::memory_order_release);
  return ok;
}

size_t TlsCacheSet::CachedBytes() const {
  size_t bytes = 0;
  for (const Cache& c : caches_) {
    while (c.lock.test_and_set(std::memory_order_acquire)) {
      // quiescent-ish (scrape frequency), spin is very short
    }
    for (int o = 0; o < kNumOrders; ++o) {
      bytes += static_cast<size_t>(c.size[o]) * OrderBytes(o);
    }
    c.lock.clear(std::memory_order_release);
  }
  return bytes;
}

void TlsCacheSet::DrainAll() {
  for (Cache& c : caches_) {
    while (c.lock.test_and_set(std::memory_order_acquire)) {
      // quiescent context, spin is very short
    }
    for (int o = 0; o < kNumOrders; ++o) {
      while (c.size[o] > 0) {
        buddy_->Free(c.slots[o][--c.size[o]], o);
      }
    }
    c.lock.clear(std::memory_order_release);
  }
}

}  // namespace dingofs
