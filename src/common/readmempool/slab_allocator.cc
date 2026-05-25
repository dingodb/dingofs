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

#include "common/readmempool/slab_allocator.h"

#include <glog/logging.h>

namespace dingofs {

int SlabAllocator::BinIndex(size_t len) {
  if (len == 0) return 0;
  if (len > kSlabMax) return -1;
  int i = 0;
  size_t s = kMinOrderBytes;  // 4K = class 0
  while (s < len) {
    s <<= 1;
    ++i;
  }
  return i;  // 0..4
}

SlabAllocator::SlabAllocator(BuddyAllocator* buddy, uint8_t* base, size_t total)
    : buddy_(buddy), base_(base) {
  CHECK(buddy != nullptr && base != nullptr);
  CHECK_EQ(total % kSlabBlockBytes, 0u);
  slab_owner_.assign(total >> kSlabBlockShift, nullptr);
  for (int i = 0; i < kNumSlabClasses; ++i) {
    bins_[i].region_bytes = static_cast<uint32_t>(kMinOrderBytes << i);
  }
}

SlabAllocator::~SlabAllocator() {
  // The arena is freed as a whole on destruction; here we only reclaim Slab
  // metadata to avoid leaks.
  for (Slab* s : slab_owner_) delete s;
}

void SlabAllocator::PushPartial(Bin& bin, Slab* s) {
  s->prev = nullptr;
  s->next = bin.partial;
  if (bin.partial != nullptr) bin.partial->prev = s;
  bin.partial = s;
}

void SlabAllocator::RemovePartial(Bin& bin, Slab* s) {
  if (s->prev != nullptr) {
    s->prev->next = s->next;
  } else {
    bin.partial = s->next;
  }
  if (s->next != nullptr) s->next->prev = s->prev;
  s->prev = s->next = nullptr;
}

uint64_t SlabAllocator::Allocate(size_t len, size_t* out_cap) {
  int bi = BinIndex(len);
  if (bi < 0) return kInvalid;
  std::lock_guard<std::mutex> lk(mu_);
  Bin& bin = bins_[bi];

  bool created = false;
  if (bin.partial == nullptr) {
    // Take one 2MB block and carve it into regions.
    uint64_t block_off = buddy_->Allocate(kSlabBlockOrder);
    if (block_off == BuddyAllocator::kInvalid) return kInvalid;  // buddy full

    Slab* s = new Slab;
    s->block_off = block_off;
    s->region_bytes = bin.region_bytes;
    s->total = static_cast<uint32_t>(kSlabBlockBytes / bin.region_bytes);
    s->free_cnt = s->total;
    for (uint32_t i = 0; i < s->total; ++i) {
      uint64_t roff = block_off + (static_cast<uint64_t>(i) * bin.region_bytes);
      RegionNext(roff) = (i + 1 < s->total) ? (i + 1) : kRegNil;
    }
    s->free_head = 0;
    slab_owner_[block_off >> kSlabBlockShift] = s;
    PushPartial(bin, s);
    created = true;  // a freshly created slab is used immediately, not a "retained empty block"
  }

  Slab* s = bin.partial;
  // Only "reusing an existing retained empty slab" counts as consuming an empty
  // (a freshly created one does not).
  bool was_empty = !created && (s->free_cnt == s->total);
  uint32_t idx = s->free_head;
  uint64_t roff = s->block_off + (static_cast<uint64_t>(idx) * s->region_bytes);
  s->free_head = RegionNext(roff);
  --s->free_cnt;
  ++used_regions_;
  if (was_empty) --bin.empty_count;             // no longer fully empty
  if (s->free_cnt == 0) RemovePartial(bin, s);  // full, remove from partial
  *out_cap = s->region_bytes;
  return roff;
}

size_t SlabAllocator::Free(uint64_t off) {
  uint64_t block_off = off & ~(kSlabBlockBytes - 1);
  std::lock_guard<std::mutex> lk(mu_);
  Slab* s = slab_owner_[block_off >> kSlabBlockShift];
  DCHECK(s != nullptr) << "Free off not in any slab: " << off;
  if (s == nullptr) return 0;

  size_t region_bytes = s->region_bytes;  // capture before potential slab delete
  uint32_t idx = static_cast<uint32_t>((off - block_off) / s->region_bytes);
  bool was_full = (s->free_cnt == 0);
  RegionNext(off) = s->free_head;
  s->free_head = idx;
  ++s->free_cnt;
  --used_regions_;

  Bin& bin = bins_[BinIndex(s->region_bytes)];
  if (was_full) PushPartial(bin, s);  // recovered from full, re-insert into partial

  if (s->free_cnt == s->total) {  // slab became fully empty
    ++bin.empty_count;
    if (bin.empty_count > 1) {
      // Already retain one empty slab -> return this one to buddy
      // (hysteresis: keep at most 1).
      RemovePartial(bin, s);
      uint64_t bo = s->block_off;
      slab_owner_[bo >> kSlabBlockShift] = nullptr;
      delete s;
      buddy_->Free(bo, kSlabBlockOrder);
      --bin.empty_count;
    }
    // else: retain this empty slab (stays in partial, reused by the next
    // Allocate without touching buddy)
  }
  return region_bytes;
}

void SlabAllocator::DrainEmpty() {
  std::lock_guard<std::mutex> lk(mu_);
  for (Bin& bin : bins_) {
    Slab* s = bin.partial;
    while (s != nullptr) {
      Slab* next = s->next;
      if (s->free_cnt == s->total) {  // retained empty slab -> return to buddy
        RemovePartial(bin, s);
        uint64_t bo = s->block_off;
        slab_owner_[bo >> kSlabBlockShift] = nullptr;
        delete s;
        buddy_->Free(bo, kSlabBlockOrder);
      }
      s = next;
    }
    bin.empty_count = 0;
  }
}

bool SlabAllocator::OwnsBlock(uint64_t off) const {
  // Lock-free read is safe for a "currently allocated off": the region is out,
  // so its slab won't be reclaimed/cleared and the owner entry is stable; a
  // concurrent thread can only write a different idx (a different slab).
  uint64_t idx = off >> kSlabBlockShift;
  if (idx >= slab_owner_.size()) return false;
  return slab_owner_[idx] != nullptr;
}

size_t SlabAllocator::UsedRegions() const {
  std::lock_guard<std::mutex> lk(mu_);
  return used_regions_;
}

size_t SlabAllocator::OccupiedBytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  size_t blocks = 0;
  for (Slab* s : slab_owner_) {
    if (s != nullptr) ++blocks;  // one Slab == one 2MB block
  }
  return blocks * kSlabBlockBytes;
}

size_t SlabAllocator::UsedBytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  size_t bytes = 0;
  for (Slab* s : slab_owner_) {
    if (s != nullptr) {
      bytes += static_cast<size_t>(s->total - s->free_cnt) * s->region_bytes;
    }
  }
  return bytes;
}

size_t SlabAllocator::RetainedEmptyBytes() const {
  std::lock_guard<std::mutex> lk(mu_);
  size_t bytes = 0;
  for (const Bin& bin : bins_) {
    bytes += static_cast<size_t>(bin.empty_count) * kSlabBlockBytes;
  }
  return bytes;
}

}  // namespace dingofs
