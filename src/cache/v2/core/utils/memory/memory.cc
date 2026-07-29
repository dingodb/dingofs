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

#include "cache/v1/core/utils/memory/memory.h"

#include <glog/logging.h>
#include <numaif.h>
#include <sys/mman.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>

namespace dingofs {
namespace cache {
namespace memory {

constexpr uint32_t kClassSizes[kNumClasses] = {16,   32,   48,   64,  96,  128,
                                               192,  256,  384,  512, 768, 1024,
                                               1536, 2048, 3072, 4096};

static constexpr auto kSizeClass = [] {
  std::array<uint8_t, (kMaxSlabSize >> 4) + 1> table{};
  uint8_t c = 0;
  for (size_t i = 0; i < table.size(); ++i) {
    while (kClassSizes[c] < i * 16) {
      ++c;
    }
    table[i] = c;
  }
  return table;
}();

static int SizeToClass(size_t size) {
  if (size > kMaxSlabSize) [[unlikely]] {
    return -1;
  }
  return kSizeClass[(size + 15) >> 4];
}

struct FreeObject {
  FreeObject* next;
};

struct CrossFreeItem {
  CrossFreeItem* next;
};

// Span header lives in the first 64 bytes of each 64 KiB span; objects
// start right after. Free() recovers the class via AlignDown(p, kSpanSize).
struct SpanHeader {
  uint32_t magic;
  int32_t cls;
};
constexpr uint32_t kSpanMagic = 0xdcac4e5f;
constexpr size_t kSpanHeaderSize = kCacheLineSize;

struct alignas(kCacheLineSize) CrossFreeStack {
  std::atomic<CrossFreeItem*> head{nullptr};
};

struct ShardMem {
  unsigned shard_id = 0;
  int numa_node = -1;
  char* base = nullptr;  // this shard's arena base
  size_t committed = 0;  // bytes committed (RW) from base
  size_t bump = 0;       // bytes handed out from base
  FreeObject* free_lists[kNumClasses] = {};
};

static char* g_base = nullptr;
static size_t g_reserved = 0;
static unsigned g_nshards = 0;
static CrossFreeStack* g_cross = nullptr;  // one per shard
static ShardMem** g_shard_mem = nullptr;   // one slot per shard

static thread_local ShardMem* tls_mem = nullptr;
static thread_local uintptr_t tls_arena_base = 0;

void GlobalInit(unsigned nshards) {
  if (g_base != nullptr) {
    if (nshards != g_nshards) {
      LOG(FATAL)
          << "Fail to init memory: GlobalInit called twice with different "
             "nshards";
    }
    return;
  }

  size_t reserve = static_cast<size_t>(nshards) << kShardShift;
  constexpr size_t kAlign = size_t{1} << kShardShift;
  size_t padded = reserve + kAlign;
  void* raw = ::mmap(nullptr, padded, PROT_NONE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (raw == MAP_FAILED) {
    std::perror("cache/core: mmap(arena reserve)");
    std::abort();
  }

  uintptr_t aligned = AlignUp(reinterpret_cast<uintptr_t>(raw), kAlign);
  size_t head = aligned - reinterpret_cast<uintptr_t>(raw);
  if (head != 0) {
    (void)::munmap(raw, head);
  }

  size_t tail = padded - reserve - head;
  if (tail != 0) {
    (void)::munmap(reinterpret_cast<void*>(aligned + reserve), tail);
  }

  void* base = reinterpret_cast<void*>(aligned);
  (void)::madvise(base, reserve, MADV_DONTDUMP);
  g_base = static_cast<char*>(base);
  g_reserved = reserve;
  g_nshards = nshards;
  g_cross = new CrossFreeStack[nshards];
  g_shard_mem = new ShardMem*[nshards]();
}

void ShardInit(unsigned shard_id, int numa_node) {
  CHECK(g_base != nullptr) << "memory::GlobalInit first";
  CHECK_LT(shard_id, g_nshards);
  if (tls_mem != nullptr) {
    CHECK_EQ(tls_mem->shard_id, shard_id) << "thread rebound to other "
                                             "shard";
    return;
  }

  ShardMem* m = g_shard_mem[shard_id];
  if (m == nullptr) {
    m = new ShardMem();
    m->shard_id = shard_id;
    m->numa_node = numa_node;
    m->base = g_base + (static_cast<size_t>(shard_id) << kShardShift);
    g_shard_mem[shard_id] = m;
  }
  tls_mem = m;
  tls_arena_base = reinterpret_cast<uintptr_t>(m->base);
}

int LocalNumaNode() { return tls_mem != nullptr ? tls_mem->numa_node : -1; }

bool InArena(const void* p) {
  return static_cast<size_t>(reinterpret_cast<uintptr_t>(p) -
                             reinterpret_cast<uintptr_t>(g_base)) < g_reserved;
}

unsigned ShardOf(const void* p) {
  return static_cast<unsigned>(
      (reinterpret_cast<uintptr_t>(p) - reinterpret_cast<uintptr_t>(g_base)) >>
      kShardShift);
}

bool IsLocal(const void* p) {
  return (reinterpret_cast<uintptr_t>(p) >> kShardShift) ==
         (tls_arena_base >> kShardShift);
}

static void CommitUpTo(ShardMem* m, size_t needed) {
  if (needed <= m->committed) {
    return;
  }

  size_t new_committed = AlignUp(needed, kCommitStep);
  char* start = m->base + m->committed;
  size_t len = new_committed - m->committed;
  if (::mprotect(start, len, PROT_READ | PROT_WRITE) != 0) {
    std::perror("cache/core: mprotect(commit)");
    std::abort();
  }

  if (m->numa_node >= 0) {
    unsigned long nodemask = 1ul << m->numa_node;
    // Best effort: fall back silently if NUMA is unavailable.
    (void)::mbind(start, len, MPOL_BIND, &nodemask, (sizeof(nodemask) * 8) + 1,
                  0);
  }

  (void)::madvise(start, len, MADV_HUGEPAGE);
  m->committed = new_committed;
}

static FreeObject* NewSpan(ShardMem* m, int cls) {
  size_t span_off = AlignUp(m->bump, kSpanSize);
  CommitUpTo(m, span_off + kSpanSize);
  m->bump = span_off + kSpanSize;

  char* span = m->base + span_off;
  auto* hdr = reinterpret_cast<SpanHeader*>(span);
  hdr->magic = kSpanMagic;
  hdr->cls = cls;

  uint32_t objsz = kClassSizes[cls];
  char* first = span + kSpanHeaderSize;
  char* end = span + kSpanSize;
  FreeObject* head = nullptr;
  for (char* p = end - objsz; p >= first + objsz; p -= objsz) {
    auto* obj = reinterpret_cast<FreeObject*>(p);
    obj->next = head;
    head = obj;
  }
  m->free_lists[cls] = head;
  return reinterpret_cast<FreeObject*>(first);
}

void* Alloc(size_t size) {
  ShardMem* m = tls_mem;
  if (m != nullptr) [[likely]] {
    int cls = SizeToClass(size);
    if (cls >= 0) [[likely]] {
      FreeObject* obj = m->free_lists[cls];
      if (obj != nullptr) [[likely]] {
        m->free_lists[cls] = obj->next;
        return obj;
      }
      return NewSpan(m, cls);
    }
  }
  return std::malloc(size);
}

static void FreeLocal(ShardMem* m, void* p) {
  auto* hdr = reinterpret_cast<SpanHeader*>(
      AlignDown(reinterpret_cast<uintptr_t>(p), uintptr_t{kSpanSize}));
  CHECK(hdr->magic == kSpanMagic) << "not a slab pointer: " << p;
  int cls = hdr->cls;
  auto* obj = reinterpret_cast<FreeObject*>(p);
  obj->next = m->free_lists[cls];
  m->free_lists[cls] = obj;
}

void Free(void* p) {
  if (p == nullptr) {
    return;
  }
  if (g_base != nullptr && InArena(p)) [[likely]] {
    ShardMem* m = tls_mem;
    if (m != nullptr && IsLocal(p)) [[likely]] {
      FreeLocal(m, p);
      return;
    }

    unsigned owner = ShardOf(p);
    auto* item = reinterpret_cast<CrossFreeItem*>(p);
    CrossFreeStack& stack = g_cross[owner];
    CrossFreeItem* head = stack.head.load(std::memory_order_relaxed);
    do {
      item->next = head;
    } while (!stack.head.compare_exchange_weak(
        head, item, std::memory_order_release, std::memory_order_relaxed));
    return;
  }
  std::free(p);
}

unsigned DrainCrossShardFree() {
  ShardMem* m = tls_mem;
  if (m == nullptr) {
    return 0;
  }

  // Peek before the exchange: this runs every reactor iteration and the
  // stack is almost always empty, so the common case must stay a plain
  // L1 load -- a full-barrier RMW here costs whole percents of the core.
  CrossFreeStack& stack = g_cross[m->shard_id];
  if (stack.head.load(std::memory_order_relaxed) == nullptr) {
    return 0;
  }

  CrossFreeItem* item = stack.head.exchange(nullptr, std::memory_order_acquire);
  unsigned n = 0;
  while (item != nullptr) {
    CrossFreeItem* next = item->next;
    FreeLocal(m, item);
    item = next;
    ++n;
  }
  return n;
}

}  // namespace memory
}  // namespace cache
}  // namespace dingofs
