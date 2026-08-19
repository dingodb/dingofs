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

#include "blockcache/core/memory/slab_pool.h"

#include <glog/logging.h>
#include <sys/mman.h>

#include <sstream>

#include "blockcache/utils/cpu.h"

namespace dingofs {
namespace blockcache {

[[gnu::noinline, gnu::cold, noreturn]] static void FatalNotFromPool(
    const void* p, size_t offset, size_t total) {
  LOG(FATAL) << "pointer not from this pool: " << p << " offset=" << offset
             << " total=" << total;
}

[[gnu::noinline, gnu::cold, noreturn]] static void FatalDoubleFreeSuperblock(
    uint32_t sb) {
  LOG(FATAL) << "double free: superblock " << sb << " is already free";
}

[[gnu::noinline, gnu::cold, noreturn]] static void FatalMisaligned(
    size_t offset, size_t unit) {
  LOG(FATAL) << "misaligned pointer: offset=" << offset << " unit=" << unit;
}

[[gnu::noinline, gnu::cold, noreturn]] static void FatalDoubleFree(
    uint32_t sb, uint32_t ordinal) {
  LOG(FATAL) << "double free: superblock " << sb << " object " << ordinal;
}

SlabPool::SlabPool(const Option& option) : metas_(option.superblock_count) {
  CHECK_GT(option.superblock_count, 0u);
  CHECK_LT(option.superblock_count, size_t{kNilIndex});

  total_ = option.superblock_count * kSuperblockSize;
  void* base = ::mmap(nullptr, total_, PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (base == MAP_FAILED) {
    PLOG(FATAL) << "Fail to mmap the slab pool";
  }

  base_ = static_cast<char*>(base);
  BindPages(base_, total_, option.numa_node);

  for (size_t sb = option.superblock_count; sb > 0; --sb) {
    PushFreeSuperblock(static_cast<uint32_t>(sb - 1));
  }
}

SlabPool::~SlabPool() {
  for (size_t sb = 0; sb < metas_.size(); ++sb) {
    const auto& meta = metas_[sb];
    DCHECK(meta.shift == kFreeTag ||
           (meta.shift != kSuperShift &&
            meta.free_count == ObjectCountFor(meta.shift)))
        << "superblock " << sb << " still allocated at destruction";
  }

  if (base_ != nullptr) {
    ::munmap(base_, total_);
  }
}

char* SlabPool::Alloc(size_t n) {
  stats_.allocs++;
  if (n == 0 || n > kSuperblockSize) {
    stats_.failed_allocs++;
    return nullptr;
  }

  int shift = ShiftFor(n);
  char* p = shift == kSuperShift ? AllocSuperblock() : AllocFromClass(shift);
  if (p == nullptr) {
    stats_.failed_allocs++;
    LogExhausted(ObjectSizeFor(shift));
  }
  return p;
}

void SlabPool::Free(char* p) {
  if (p == nullptr) {
    return;
  }

  stats_.frees++;
  auto offset = static_cast<size_t>(p - base_);  // wraps when p < base_
  if (offset >= total_) [[unlikely]] {
    FatalNotFromPool(p, offset, total_);
  }
  auto sb = static_cast<uint32_t>(offset >> kSuperShift);

  int shift = metas_[sb].shift;
  if (shift == kFreeTag) [[unlikely]] {
    FatalDoubleFreeSuperblock(sb);
  }
  if (shift == kSuperShift) {
    if (offset % kSuperblockSize != 0) [[unlikely]] {
      FatalMisaligned(offset, kSuperblockSize);
    }
    metas_[sb].shift = kFreeTag;
    PushFreeSuperblock(sb);
    return;
  }
  FreeToClass(sb, offset, shift);
}

uint32_t SlabPool::PopFreeSuperblock() {
  uint32_t sb = free_head_;
  if (sb != kNilIndex) {
    free_head_ = metas_[sb].next;
    metas_[sb].next = kNilIndex;
    return sb;
  }
  return ReclaimCachedEmpty();
}

void SlabPool::PushFreeSuperblock(uint32_t sb) {
  metas_[sb].next = free_head_;
  free_head_ = static_cast<uint16_t>(sb);
}

uint32_t SlabPool::ReclaimCachedEmpty() {
  for (int c = 0; c < kNumSubClasses; ++c) {
    auto& size_class = classes_[c];
    if (size_class.empty_count == 0) {
      continue;
    }

    int shift = kMinShift + c;
    for (uint32_t sb = size_class.partial; sb != kNilIndex;
         sb = metas_[sb].next) {
      if (metas_[sb].free_count == ObjectCountFor(shift)) {
        DetachFromClass(size_class, metas_[sb], shift);
        return sb;
      }
    }
  }

  return kNilIndex;
}

void SlabPool::DetachFromClass(SizeClass& size_class, SlabMeta& meta,
                               int shift) {
  ListRemove(size_class, meta);
  --size_class.empty_count;
  meta.shift = kFreeTag;
  meta.free_count = 0;
  for (uint32_t w = 0; w < BitmapWordsFor(shift); ++w) {
    meta.bitmap[w] = 0;
  }
}

char* SlabPool::AllocSuperblock() {
  uint32_t sb = PopFreeSuperblock();
  if (sb == kNilIndex) {
    return nullptr;
  }

  DCHECK_EQ(metas_[sb].shift, kFreeTag) << "superblock handed out twice";
  metas_[sb].shift = kSuperShift;
  return SuperblockAddress(sb);
}

char* SlabPool::AllocFromClass(int shift) {
  auto& size_class = classes_[shift - kMinShift];
  uint32_t sb = size_class.partial;
  if (sb == kNilIndex) {
    sb = PopFreeSuperblock();
    if (sb == kNilIndex) {
      return nullptr;
    }

    auto& meta = metas_[sb];
    uint32_t count = ObjectCountFor(shift);
    meta.shift = static_cast<uint8_t>(shift);
    meta.cursor = 0;
    meta.free_count = static_cast<uint16_t>(count);
    SetFirstBits(meta.bitmap, count);
    ListPushFront(size_class, sb);
    ++size_class.empty_count;  // fully free until the pop below
  }

  auto& meta = metas_[sb];
  DCHECK_EQ(meta.shift, shift);
  DCHECK_GT(meta.free_count, 0);
  if (meta.free_count == ObjectCountFor(shift)) {
    --size_class.empty_count;  // about to lose its first object
  }

  uint32_t w = meta.cursor;
  for (;; ++w) {
    DCHECK_LT(w, BitmapWordsFor(shift)) << "free_count > 0 but bitmap empty";
    if (meta.bitmap[w] != 0) {
      break;
    }
  }

  meta.cursor = static_cast<uint8_t>(w);
  uint32_t ordinal = (w * 64) + __builtin_ctzll(meta.bitmap[w]);
  ClearBit(meta.bitmap, ordinal);
  if (--meta.free_count == 0) {
    ListRemove(size_class, meta);
  }
  return SuperblockAddress(sb) + (size_t{ordinal} << shift);
}

void SlabPool::FreeToClass(uint32_t sb, size_t offset, int shift) {
  auto& size_class = classes_[shift - kMinShift];
  auto ordinal = static_cast<uint32_t>((offset % kSuperblockSize) >> shift);
  if (offset % ObjectSizeFor(shift) != 0) [[unlikely]] {
    FatalMisaligned(offset, ObjectSizeFor(shift));
  }

  auto& meta = metas_[sb];
  if (TestBit(meta.bitmap, ordinal)) [[unlikely]] {
    FatalDoubleFree(sb, ordinal);
  }
  SetBit(meta.bitmap, ordinal);
  if (ordinal / 64 < meta.cursor) {
    meta.cursor = static_cast<uint8_t>(ordinal / 64);
  }

  if (++meta.free_count == ObjectCountFor(shift)) {
    if (++size_class.empty_count > 1) {
      DetachFromClass(size_class, meta, shift);
      PushFreeSuperblock(sb);
    }
  } else if (meta.free_count == 1) {
    ListPushFront(size_class, sb);
  }
}

void SlabPool::ListPushFront(SizeClass& size_class, uint32_t sb) {
  auto& meta = metas_[sb];
  meta.prev = kNilIndex;
  meta.next = size_class.partial;
  if (size_class.partial != kNilIndex) {
    metas_[size_class.partial].prev = static_cast<uint16_t>(sb);
  }
  size_class.partial = static_cast<uint16_t>(sb);
}

void SlabPool::ListRemove(SizeClass& size_class, SlabMeta& meta) {
  if (meta.prev != kNilIndex) {
    metas_[meta.prev].next = meta.next;
  } else {
    size_class.partial = meta.next;
  }
  if (meta.next != kNilIndex) {
    metas_[meta.next].prev = meta.prev;
  }
  meta.prev = meta.next = kNilIndex;
}

void SlabPool::LogExhausted(size_t size) {
  if (stats_.failed_allocs % 100 != 1) {  // the scan below is not free
    return;
  }

  uint32_t in_class[kNumSubClasses] = {};
  uint32_t whole = 0;
  uint32_t free = 0;
  for (const auto& meta : metas_) {
    if (meta.shift == kFreeTag) {
      ++free;
    } else if (meta.shift == kSuperShift) {
      ++whole;
    } else {
      ++in_class[meta.shift - kMinShift];
    }
  }

  std::ostringstream os;
  for (int i = 0; i < kNumSubClasses; ++i) {
    if (in_class[i] != 0) {
      size_t class_size = ObjectSizeFor(kMinShift + i);
      os << (class_size >> (class_size >= (1 << 20) ? 20 : 10))
         << (class_size >= (1 << 20) ? "m" : "k") << "=" << in_class[i] << " ";
    }
  }
  os << "whole=" << whole << " free=" << free;

  LOG(WARNING) << "Slab pool exhausted for size " << size << " ("
               << stats_.failed_allocs
               << " times so far), superblocks: " << os.str();
}

}  // namespace blockcache
}  // namespace dingofs
