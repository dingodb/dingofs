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

#include "common/readmempool/read_mem_pool.h"

#include <glog/logging.h>

namespace dingofs {

// Note: ReadBuf::Reset() is defined in read_buf.cc.

ReadMemPool::ReadMemPool(size_t budget_bytes) {
  arena_ = Arena::Create(budget_bytes, kMaxOrderBytes);
  if (arena_ == nullptr) {
    LOG(ERROR) << "ReadMemPool: arena create failed, budget=" << budget_bytes;
    return;
  }
  buddy_ = std::make_unique<BuddyAllocator>(arena_->Base(), arena_->Total());
  slab_ = std::make_unique<SlabAllocator>(buddy_.get(), arena_->Base(),
                                          arena_->Total());
  tls_ = std::make_unique<TlsCacheSet>(buddy_.get());
  LOG(INFO) << "ReadMemPool ready: total=" << arena_->Total()
            << " huge=" << arena_->HasHugePage();
}

ReadBuf ReadMemPool::Allocate(size_t len) {
  if (!Valid() || len == 0) return {};
  alloc_num_ << 1;

  // Small class: slab (fixed regions, compresses buddy's 2^k internal frag).
  if (len <= kSlabMax) {
    size_t cap = 0;
    uint64_t off = slab_->Allocate(len, &cap);
    if (off == SlabAllocator::kInvalid) {
      // tier-2: buddy can't hand slab a fresh 2MB block. Reclaim idle-but-carved
      // memory (TLS cache + retained-empty slab blocks), coalesce, retry once.
      DrainIdle();
      off = slab_->Allocate(len, &cap);
    }
    if (off == SlabAllocator::kInvalid) {  // genuinely exhausted
      alloc_fail_num_ << 1;
      return {};
    }
    OnAllocated(len, cap);
    return ReadBuf(this, arena_->Base() + off, off, cap, MakeMeta(kSlab, 0));
  }

  // Large/mid class: buddy (try the TLS fast path first).
  int order = BytesToOrder(len);
  if (order < 0) {  // > 64M, oversize (the caller should split by chunk)
    alloc_fail_num_ << 1;
    return {};
  }

  uint64_t off = tls_->TryAlloc(order);
  if (off == BuddyAllocator::kInvalid) off = buddy_->Allocate(order);
  if (off == BuddyAllocator::kInvalid) {
    // tier-2: idle slots cached in TLS / retained-empty slab blocks fragment the
    // arena so buddy can't find a contiguous block. Reclaim them, coalesce,
    // retry once. (DrainIdle empties TLS, so go straight to buddy on retry.)
    DrainIdle();
    off = buddy_->Allocate(order);
  }
  if (off == BuddyAllocator::kInvalid) {  // genuinely exhausted
    alloc_fail_num_ << 1;
    return {};
  }

  OnAllocated(len, OrderBytes(order));

  return ReadBuf(this, arena_->Base() + off, off, OrderBytes(order),
                   MakeMeta(kBuddy, order));
}

void ReadMemPool::FreeBuddy(uint64_t off, int order) {
  if (!tls_->TryFree(order, off)) buddy_->Free(off, order);
}

void ReadMemPool::ReturnSlot(uint64_t off, uint32_t meta) {
  uint32_t source = meta >> 7;
  int order = static_cast<int>(meta & 0x7F);
  if (source == kSlab) {
    OnFreed(slab_->Free(off));  // slab Free returns the region_bytes
  } else {
    FreeBuddy(off, order);
    OnFreed(OrderBytes(order));
  }
}

void ReadMemPool::ReleaseExternal(void* p) {
  if (!Valid() || p == nullptr) return;
  uint64_t off =
      static_cast<uint64_t>(static_cast<uint8_t*>(p) - arena_->Base());
  DCHECK_LT(off, arena_->Total());
  // Reverse-lookup source by address: inside a slab block -> slab; else buddy.
  if (slab_->OwnsBlock(off)) {
    OnFreed(slab_->Free(off));
  } else {
    int order = buddy_->OrderAt(off);  // capture before FreeBuddy mutates tags
    FreeBuddy(off, order);
    OnFreed(OrderBytes(order));
  }
}

}  // namespace dingofs
