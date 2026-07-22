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
 * Created Date: 2026-07-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/slab_allocator.h"

#include <glog/logging.h>

#include <mutex>
#include <sstream>
#include <utility>

namespace dingofs {
namespace cache {

SlabAllocator::SlabAllocator(MemoryPoolUPtr pool)
    : pool_(std::move(pool)),
      base_(pool_->BaseAddr()),
      total_size_(pool_->TotalSize()),
      meta_(pool_->BufferCount()) {}

SlabAllocator::~SlabAllocator() {
  for (size_t i = 0; i < meta_.size(); ++i) {
    int tag = meta_[i].shift.load(std::memory_order_relaxed);
    DCHECK_EQ(tag, kFreeTag)
        << "superblock " << i << " still allocated at destruction";
  }
}

SlabAllocatorUPtr SlabAllocator::Create(size_t superblock_count) {
  if (superblock_count == 0 || superblock_count >= kNilIndex) {
    LOG(ERROR) << "Invalid superblock count: " << superblock_count;
    return nullptr;
  }

  auto pool = MemoryPool::Create(kSuperblockSize, superblock_count);
  if (pool == nullptr) {
    return nullptr;
  }

  LOG(INFO) << "Successfully create SlabAllocator{superblock_count="
            << superblock_count << "}";

  return SlabAllocatorUPtr(new SlabAllocator(std::move(pool)));
}

// 4KB => 12, 4MB => 22
int SlabAllocator::ShiftFor(size_t size) {
  if (size <= kMinObjectSize) {
    return kMinShift;
  }
  return CeilLog2(size);  // round up to the next power-of-two class
}

char* SlabAllocator::Alloc(size_t size) {
  if (size == 0 || size > kSuperblockSize) {
    return nullptr;
  }

  int shift = ShiftFor(size);
  char* buffer =
      shift == kSuperShift ? AllocSuperblock() : AllocFromClass(shift);
  if (buffer == nullptr) {
    LogExhausted(ObjectSizeFor(shift));  // outside any class mutex
  }
  return buffer;
}

char* SlabAllocator::AllocSuperblock() {
  char* block = pool_->Require();
  if (block == nullptr) {
    return nullptr;
  }

  auto& meta = meta_[pool_->IndexOf(block)];
  DCHECK_EQ(meta.shift.load(std::memory_order_relaxed), kFreeTag)
      << "superblock handed out twice";
  meta.shift.store(kSuperShift, std::memory_order_relaxed);
  return block;
}

char* SlabAllocator::AllocFromClass(int shift) {
  auto& size_class = classes_[shift - kMinShift];
  std::lock_guard<bthread::Mutex> lock(size_class.mutex);

  uint32_t sb = size_class.partial;
  if (sb == kNilIndex) {
    char* block = pool_->Require();  // lock-free, safe under the class mutex
    if (block == nullptr) {
      return nullptr;
    }
    sb = static_cast<uint32_t>(pool_->IndexOf(block));
    AttachToClass(size_class, sb, shift);
  }

  auto& meta = meta_[sb];
  DCHECK_EQ(meta.shift.load(std::memory_order_relaxed), shift);
  DCHECK_GT(meta.free_count, 0);
  uint32_t ordinal = PopFreeObject(meta, BitmapWordsFor(shift));
  if (--meta.free_count == 0) {
    ListRemove(size_class, meta);
  }
  return ObjectAddress(sb, ordinal, shift);
}

// FREE superblock -> CLASS(shift): every object free, on the partial list.
void SlabAllocator::AttachToClass(SizeClass& size_class, uint32_t sb,
                                  int shift) {
  auto& meta = meta_[sb];
  DCHECK_EQ(meta.shift.load(std::memory_order_relaxed), kFreeTag);
  DCHECK_EQ(meta.free_count, 0);

  uint32_t count = ObjectCountFor(shift);
  meta.shift.store(static_cast<uint8_t>(shift), std::memory_order_relaxed);
  meta.free_count = static_cast<uint16_t>(count);
  // FREE superblocks keep an all-zero bitmap, only the first count bits flip.
  SetFirstBits(meta.bitmap, count);
  ListPushFront(size_class, sb);
}

// Inverse of AttachToClass: off the partial list, back to FREE with an
// all-zero bitmap, ready for any class. Caller holds the class mutex.
void SlabAllocator::DetachFromClass(SizeClass& size_class, SlabMeta& meta,
                                    int shift) {
  ListRemove(size_class, meta);
  meta.shift.store(kFreeTag, std::memory_order_relaxed);
  meta.free_count = 0;
  for (uint32_t w = 0; w < BitmapWordsFor(shift); ++w) {
    meta.bitmap[w] = 0;
  }
}

uint32_t SlabAllocator::PopFreeObject(SlabMeta& meta, uint32_t words) {
  for (uint32_t w = 0; w < words; ++w) {
    if (meta.bitmap[w] != 0) {
      uint32_t ordinal = (w * 64) + FirstSetBit(meta.bitmap[w]);
      ClearBit(meta.bitmap, ordinal);
      return ordinal;
    }
  }
  LOG(FATAL) << "free_count > 0 but bitmap empty";
  return 0;
}

void SlabAllocator::Free(char* ptr) {
  if (ptr == nullptr) {
    return;
  }
  auto offset = static_cast<size_t>(ptr - base_);
  CHECK_LT(offset, total_size_) << "pointer not from this pool";
  uint32_t sb = SuperblockOf(offset);

  // Unlocked read is stable: a live object pins its superblock's state (I2).
  int shift = meta_[sb].shift.load(std::memory_order_relaxed);
  CHECK_NE(shift, kFreeTag)
      << "double free: superblock " << sb << " is already free";
  if (shift == kSuperShift) {
    CHECK(IsAligned(offset, kSuperShift)) << "misaligned pointer";
    meta_[sb].shift.store(kFreeTag, std::memory_order_relaxed);
    pool_->Release(ptr);
    return;
  }
  FreeToClass(sb, offset, shift);
}

void SlabAllocator::FreeToClass(uint32_t sb, size_t offset, int shift) {
  auto& size_class = classes_[shift - kMinShift];
  uint32_t ordinal = OrdinalOf(offset, shift);
  CHECK(IsAligned(offset, shift)) << "misaligned pointer";

  char* reclaim = nullptr;
  {
    std::lock_guard<bthread::Mutex> lock(size_class.mutex);
    auto& meta = meta_[sb];
    DCHECK_EQ(meta.shift.load(std::memory_order_relaxed), shift);

    CHECK(!TestBit(meta.bitmap, ordinal)) << "double free";
    SetBit(meta.bitmap, ordinal);

    if (++meta.free_count == ObjectCountFor(shift)) {
      DetachFromClass(size_class, meta, shift);
      reclaim = SuperblockAddress(sb);
    } else if (meta.free_count == 1) {
      ListPushFront(size_class, sb);
    }
  }
  if (reclaim != nullptr) {
    // Off-list and tag reset, safe outside the lock. Until Release lands a
    // concurrent Alloc can see an empty pool and fail spuriously -- callers
    // already treat nullptr as exhaustion.
    pool_->Release(reclaim);
  }
}

int SlabAllocator::IndexOf(const char* ptr) const {
  if (ptr < base_ || ptr >= base_ + total_size_) {
    return -1;
  }
  return static_cast<int>(SuperblockOf(ptr - base_));
}

void SlabAllocator::ListPushFront(SizeClass& size_class, uint32_t sb) {
  auto& meta = meta_[sb];
  meta.prev = kNilIndex;
  meta.next = size_class.partial;
  if (size_class.partial != kNilIndex) {
    meta_[size_class.partial].prev = static_cast<uint16_t>(sb);
  }
  size_class.partial = static_cast<uint16_t>(sb);
}

void SlabAllocator::ListRemove(SizeClass& size_class, SlabMeta& meta) {
  if (meta.prev != kNilIndex) {
    meta_[meta.prev].next = meta.next;
  } else {
    size_class.partial = meta.next;
  }
  if (meta.next != kNilIndex) {
    meta_[meta.next].prev = meta.prev;
  }
  meta.prev = meta.next = kNilIndex;
}

void SlabAllocator::LogExhausted(size_t size) const {
  uint64_t count = exhausted_count_.fetch_add(1, std::memory_order_relaxed);
  if (count % 100 != 0) {  // the scan below is not free, sample it too
    return;
  }

  uint32_t in_class[kNumSubClasses] = {};
  uint32_t whole = 0;
  uint32_t free = 0;
  for (const auto& meta : meta_) {
    int tag = meta.shift.load(std::memory_order_relaxed);
    if (tag == kFreeTag) {
      ++free;
    } else if (tag == kSuperShift) {
      ++whole;
    } else {
      ++in_class[tag - kMinShift];
    }
  }

  std::ostringstream os;
  for (int i = 0; i < kNumSubClasses; ++i) {
    if (in_class[i] != 0) {
      size_t class_size = ObjectSizeFor(kMinShift + i);
      os << (class_size >= kMiB ? class_size / kMiB : class_size / kKiB)
         << (class_size >= kMiB ? "m" : "k") << "=" << in_class[i] << " ";
    }
  }
  os << "whole=" << whole << " free=" << free;

  LOG(WARNING) << "Slab allocator exhausted for size " << size << " ("
               << count + 1 << " times so far), superblocks: " << os.str();
}

}  // namespace cache
}  // namespace dingofs
