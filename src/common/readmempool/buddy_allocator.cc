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

#include "common/readmempool/buddy_allocator.h"

#include <glog/logging.h>

#include <algorithm>

namespace dingofs {

int BytesToOrder(size_t len) {
  if (len == 0 || len <= kMinOrderBytes) return 0;
  if (len > kMaxOrderBytes) return -1;
  int o = 0;
  size_t s = kMinOrderBytes;
  while (s < len) {
    s <<= 1;
    ++o;
  }
  return o;
}

BuddyAllocator::BuddyAllocator(uint8_t* base, size_t total)
    : base_(base), total_(total) {
  CHECK(base != nullptr);
  CHECK_GT(total, 0u);
  CHECK_EQ(total % kMaxOrderBytes, 0u)
      << "total must be a multiple of " << kMaxOrderBytes;

  for (unsigned long & o : free_head_) o = kInvalid;
  // Init non-block-start cells to the "allocated" sentinel (0x80) so any stale
  // read never equals any free order, preventing coalesce mis-merges (top-block
  // starts are then set to 14 by PushFree).
  order_tag_.assign(total >> kMinOrderShift, kAllocFlag);

  // Slice the arena into total/64M top blocks of 64M, all chained into the
  // order-14 free list.
  for (uint64_t off = 0; off < total; off += kMaxOrderBytes) {
    PushFreeLocked(kMaxOrder, off);
  }
}

void BuddyAllocator::PushFreeLocked(int order, uint64_t off) {
  NextOf(off) = free_head_[order];
  free_head_[order] = off;
  order_tag_[off >> kMinOrderShift] = static_cast<uint8_t>(order);  // high bit clear = free
}

uint64_t BuddyAllocator::PopFreeLocked(int order) {
  uint64_t off = free_head_[order];
  if (off == kInvalid) return kInvalid;
  free_head_[order] = NextOf(off);
  return off;
}

void BuddyAllocator::RemoveFreeLocked(int order, uint64_t target) {
  uint64_t cur = free_head_[order];
  if (cur == kInvalid) return;
  if (cur == target) {
    free_head_[order] = NextOf(cur);
    return;
  }
  while (cur != kInvalid) {
    uint64_t nxt = NextOf(cur);
    if (nxt == target) {
      NextOf(cur) = NextOf(nxt);
      return;
    }
    cur = nxt;
  }
}

uint64_t BuddyAllocator::Allocate(int order) {
  if (order < 0 || order >= kNumOrders) return kInvalid;
  std::lock_guard<std::mutex> lk(mu_);

  int o = order;
  while (o < kNumOrders && free_head_[o] == kInvalid) ++o;
  if (o == kNumOrders) return kInvalid;  // exhausted

  uint64_t off = PopFreeLocked(o);
  // Top-down split: halve the higher order, push the right half onto the lower
  // free list.
  while (o > order) {
    --o;
    uint64_t right = off + OrderBytes(o);
    PushFreeLocked(o, right);
  }
  order_tag_[off >> kMinOrderShift] =
      static_cast<uint8_t>(kAllocFlag | order);  // allocated + order
  used_ += OrderBytes(order);
  return off;
}

void BuddyAllocator::Free(uint64_t off, int order) {
  DCHECK_LT(off, total_);
  DCHECK_EQ(off % OrderBytes(order), 0u);
  std::lock_guard<std::mutex> lk(mu_);

  used_ -= OrderBytes(order);
  int o = order;
  // Merge upward: the buddy must be "a whole free block of exactly order o"
  // (tag==o, high bit clear).
  while (o < kMaxOrder) {
    uint64_t buddy = off ^ OrderBytes(o);
    if (buddy >= total_) break;  // safety net (should not trigger within the same 64M top block)
    if (order_tag_[buddy >> kMinOrderShift] != static_cast<uint8_t>(o)) break;
    RemoveFreeLocked(o, buddy);
    off = std::min(off, buddy);
    ++o;
  }
  PushFreeLocked(o, off);
}

size_t BuddyAllocator::UsedBytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  return used_;
}

int BuddyAllocator::LargestFreeOrder() const {
  std::lock_guard<std::mutex> lk(mu_);
  for (int o = kMaxOrder; o >= 0; --o) {
    if (free_head_[o] != kInvalid) return o;
  }
  return -1;
}

size_t BuddyAllocator::CountFree(int order) const {
  std::lock_guard<std::mutex> lk(mu_);
  size_t n = 0;
  uint64_t cur = free_head_[order];
  while (cur != kInvalid) {
    ++n;
    cur = *reinterpret_cast<const uint64_t*>(base_ + cur);
  }
  return n;
}

}  // namespace dingofs
