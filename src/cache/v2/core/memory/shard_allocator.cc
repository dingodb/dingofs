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

#include "cache/v2/core/memory/shard_allocator.h"

#include <glog/logging.h>
#include <sys/mman.h>

#include <array>
#include <cstdlib>
#include <cstring>
#include <new>

#include "cache/v2/utils/cpu.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace memory {

constexpr uint32_t kClassSizes[kNumClasses] = {16,   32,   48,   64,  96,  128,
                                               192,  256,  384,  512, 768, 1024,
                                               1536, 2048, 3072, 4096};

static constexpr auto kSizeClass = [] {
  std::array<uint8_t, (kMaxSmallSize >> 4) + 1> table{};
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
  if (size > kMaxSmallSize) [[unlikely]] {
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

// First 64B of each span; Free() recovers it via AlignDown(p, kSpanSize).
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
static unsigned g_shard_count = 0;
static CrossFreeStack* g_cross = nullptr;  // one per shard
static ShardMem** g_shard_mem = nullptr;   // one slot per shard

static thread_local ShardMem* tls_mem = nullptr;
static thread_local uintptr_t tls_arena_base = 0;

void GlobalInit(unsigned shard_count) {
  if (g_base != nullptr) {
    if (shard_count != g_shard_count) {
      LOG(FATAL)
          << "Fail to init memory: GlobalInit called twice with different "
             "shard_count";
    }
    return;
  }

  size_t reserve = static_cast<size_t>(shard_count) << kShardShift;
  constexpr size_t kAlign = size_t{1} << kShardShift;
  size_t padded = reserve + kAlign;
  void* raw = ::mmap(nullptr, padded, PROT_NONE,
                     MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
  if (raw == MAP_FAILED) {
    PLOG(FATAL) << "Fail to mmap the arena reserve";
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
  g_shard_count = shard_count;
  g_cross = new CrossFreeStack[shard_count];
  g_shard_mem = new ShardMem*[shard_count]();
}

void ShardInit(unsigned shard_id, int numa_node) {
  CHECK(g_base != nullptr) << "memory::GlobalInit first";
  CHECK_LT(shard_id, g_shard_count);
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

bool IsShardMemory(const void* p) {
  return static_cast<size_t>(reinterpret_cast<uintptr_t>(p) -
                             reinterpret_cast<uintptr_t>(g_base)) < g_reserved;
}

unsigned ShardOfPointer(const void* p) {
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
    PLOG(FATAL) << "Fail to mprotect the arena commit";
  }

  BindPages(start, len, m->numa_node);
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
  if (g_base != nullptr && IsShardMemory(p)) [[likely]] {
    ShardMem* m = tls_mem;
    if (m != nullptr && IsLocal(p)) [[likely]] {
      FreeLocal(m, p);
      return;
    }

    unsigned owner = ShardOfPointer(p);
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

  // Peek before exchange: almost always empty; an unconditional RMW is costly.
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
}  // namespace v2
}  // namespace cache
}  // namespace dingofs
