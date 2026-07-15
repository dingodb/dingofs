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
 * Created Date: 2026-07-15
 * Author: AI
 */

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <random>
#include <thread>
#include <vector>

#include "cache/common/slab_buffer.h"
#include "common/const.h"

namespace dingofs {
namespace cache {

namespace {

// Small superblocks keep tests fast: 64 KiB superblocks carve into
// 4K/8K/16K/32K classes.
constexpr size_t kSuperblockSize = 64 * kKiB;
constexpr size_t kSuperblockCount = 4;

SlabBufferPoolUPtr MakePool(size_t count = kSuperblockCount) {
  return SlabBufferPool::Create(kSuperblockSize, count);
}

}  // namespace

TEST(SlabBufferPoolTest, Create) {
  {  // power-of-two superblock
    auto pool = MakePool();
    ASSERT_NE(pool, nullptr);
    EXPECT_EQ(pool->BufferCount(), kSuperblockCount);
    EXPECT_EQ(pool->TotalSize(), kSuperblockSize * kSuperblockCount);
  }

  {  // non power-of-two superblock: classes disabled, still usable
    auto pool = SlabBufferPool::Create(24 * kKiB, 2);
    ASSERT_NE(pool, nullptr);
    auto* buffer = pool->Alloc(4 * kKiB);
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(buffer->capacity, 24 * kKiB);  // whole superblock
    pool->Free(buffer);
  }
}

TEST(SlabBufferPoolTest, AllocWholeSuperblock) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  std::vector<SlabBuffer*> buffers;
  for (size_t i = 0; i < kSuperblockCount; ++i) {
    auto* buffer = pool->Alloc();
    ASSERT_NE(buffer, nullptr);
    EXPECT_EQ(buffer->capacity, kSuperblockSize);
    buffers.push_back(buffer);
  }

  EXPECT_EQ(pool->Alloc(), nullptr);  // exhausted

  for (auto* buffer : buffers) {
    pool->Free(buffer);
  }
  EXPECT_NE(pool->Alloc(), nullptr);
}

TEST(SlabBufferPoolTest, AllocRoundsUpToClass) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  struct {
    size_t size;
    uint32_t expected_capacity;
  } cases[] = {
      {.size = 0, .expected_capacity = 4 * kKiB},
      {.size = 1, .expected_capacity = 4 * kKiB},
      {.size = 4 * kKiB, .expected_capacity = 4 * kKiB},
      {.size = (4 * kKiB) + 1, .expected_capacity = 8 * kKiB},
      {.size = 12 * kKiB, .expected_capacity = 16 * kKiB},
      {.size = 32 * kKiB, .expected_capacity = 32 * kKiB},
      {.size = (32 * kKiB) + 1, .expected_capacity = kSuperblockSize},
      {.size = kSuperblockSize, .expected_capacity = kSuperblockSize},
  };
  for (const auto& c : cases) {
    auto* buffer = pool->Alloc(c.size);
    ASSERT_NE(buffer, nullptr) << "size=" << c.size;
    EXPECT_EQ(buffer->capacity, c.expected_capacity) << "size=" << c.size;
    pool->Free(buffer);
  }

  EXPECT_EQ(pool->Alloc(kSuperblockSize + 1), nullptr);  // can never fit
}

TEST(SlabBufferPoolTest, ClassObjectsShareOneSuperblock) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  // 64K / 4K = 16 objects from a single superblock.
  std::vector<SlabBuffer*> objects;
  for (int i = 0; i < 16; ++i) {
    auto* object = pool->Alloc(4 * kKiB);
    ASSERT_NE(object, nullptr);
    EXPECT_EQ(object->index,
              objects.empty() ? object->index : objects.front()->index);
    objects.push_back(object);
  }

  {  // one superblock consumed so far: 3 whole superblocks left
    std::vector<SlabBuffer*> superblocks;
    for (size_t i = 0; i + 1 < kSuperblockCount; ++i) {
      auto* superblock = pool->Alloc();
      ASSERT_NE(superblock, nullptr);
      superblocks.push_back(superblock);
    }
    EXPECT_EQ(pool->Alloc(), nullptr);

    // 17th 4K object needs a fresh superblock but none is left.
    EXPECT_EQ(pool->Alloc(4 * kKiB), nullptr);
    for (auto* superblock : superblocks) {
      pool->Free(superblock);
    }
  }

  {  // objects are laid out back to back, 4 KiB aligned
    for (size_t i = 1; i < objects.size(); ++i) {
      EXPECT_EQ(objects[i]->data - objects[i - 1]->data, 4 * kKiB);
    }
    for (auto* object : objects) {
      EXPECT_EQ(reinterpret_cast<uintptr_t>(object->data) % (4 * kKiB), 0u);
    }
  }

  for (auto* object : objects) {
    pool->Free(object);
  }
}

TEST(SlabBufferPoolTest, FreeReclaimsSuperblock) {
  auto pool = MakePool(2);
  ASSERT_NE(pool, nullptr);

  {  // capacity flows back: class -> whole superblock
    auto* object = pool->Alloc(4 * kKiB);  // carves superblock 1
    ASSERT_NE(object, nullptr);
    auto* superblock = pool->Alloc();  // takes superblock 2
    ASSERT_NE(superblock, nullptr);
    EXPECT_EQ(pool->Alloc(), nullptr);

    pool->Free(object);  // slab fully free -> superblock reclaimed
    EXPECT_NE((object = pool->Alloc()), nullptr);
    pool->Free(object);
    pool->Free(superblock);
  }

  {  // capacity flows between classes through reclaim
    auto* small = pool->Alloc(4 * kKiB);   // carves one superblock
    auto* half1 = pool->Alloc(32 * kKiB);  // carves the other
    auto* half2 = pool->Alloc(32 * kKiB);  // same superblock as half1
    ASSERT_NE(small, nullptr);
    ASSERT_NE(half1, nullptr);
    ASSERT_NE(half2, nullptr);
    EXPECT_EQ(half1->index, half2->index);
    EXPECT_EQ(pool->Alloc(32 * kKiB), nullptr);  // both superblocks busy

    pool->Free(small);
    auto* half3 = pool->Alloc(32 * kKiB);  // re-carves the reclaimed one
    ASSERT_NE(half3, nullptr);

    pool->Free(half1);
    pool->Free(half2);
    pool->Free(half3);
  }

  {  // partially free slab is reused before carving a new superblock
    auto* a = pool->Alloc(16 * kKiB);
    auto* b = pool->Alloc(16 * kKiB);
    auto* c = pool->Alloc(16 * kKiB);
    auto* d = pool->Alloc(16 * kKiB);  // slab now fully allocated
    ASSERT_NE(d, nullptr);
    pool->Free(b);
    auto* e = pool->Alloc(16 * kKiB);  // must reuse b's slot
    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->data, b->data);
    for (auto* object : {a, c, d, e}) {
      pool->Free(object);
    }
  }
}

TEST(SlabBufferPoolTest, IndexOf) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  auto* superblock = pool->Alloc();
  ASSERT_NE(superblock, nullptr);
  EXPECT_EQ(pool->IndexOf(superblock), static_cast<int>(superblock->index));

  auto* object = pool->Alloc(4 * kKiB);
  ASSERT_NE(object, nullptr);
  {  // interior address maps back to the owning superblock
    EXPECT_EQ(pool->IndexOf(object), static_cast<int>(object->index));
    EXPECT_EQ(pool->IndexOf(object->data), static_cast<int>(object->index));
    EXPECT_NE(object->index, superblock->index);
  }

  {  // out-of-range addresses
    char local;
    EXPECT_EQ(pool->IndexOf(&local), -1);
    EXPECT_EQ(pool->IndexOf(pool->BaseAddr() + pool->TotalSize()), -1);
  }

  pool->Free(object);
  pool->Free(superblock);
}

TEST(SlabBufferPoolTest, FetchReturnsSuperblockIovecs) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  auto iovecs = pool->Fetch();
  ASSERT_EQ(iovecs.size(), kSuperblockCount);
  for (size_t i = 0; i < iovecs.size(); ++i) {
    EXPECT_EQ(iovecs[i].iov_base, pool->BaseAddr() + (i * kSuperblockSize));
    EXPECT_EQ(iovecs[i].iov_len, kSuperblockSize);
  }
}

TEST(SlabBufferPoolTest, SetRdmaKeys) {
  auto pool = MakePool();
  ASSERT_NE(pool, nullptr);

  auto* carved_before = pool->Alloc(8 * kKiB);
  ASSERT_NE(carved_before, nullptr);
  EXPECT_EQ(carved_before->lkey, 0u);

  pool->SetRdmaKeys(0x1234, 0x5678);

  {  // keys reach superblocks, already-carved and newly-carved objects
    auto* superblock = pool->Alloc();
    ASSERT_NE(superblock, nullptr);
    EXPECT_EQ(superblock->lkey, 0x1234u);
    EXPECT_EQ(superblock->rkey, 0x5678u);
    EXPECT_EQ(carved_before->lkey, 0x1234u);
    EXPECT_EQ(carved_before->rkey, 0x5678u);

    auto* carved_after = pool->Alloc(4 * kKiB);
    ASSERT_NE(carved_after, nullptr);
    EXPECT_EQ(carved_after->lkey, 0x1234u);
    EXPECT_EQ(carved_after->rkey, 0x5678u);

    pool->Free(carved_after);
    pool->Free(superblock);
  }

  pool->Free(carved_before);
}

TEST(SlabBufferPoolTest, ConcurrentAllocFree) {
  auto pool = SlabBufferPool::Create(kSuperblockSize, 16);
  ASSERT_NE(pool, nullptr);

  constexpr int kThreads = 8;
  constexpr int kRounds = 2000;
  std::atomic<int> failures{0};

  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&pool, &failures, t]() {
      std::mt19937 rng(t);
      const size_t sizes[] = {1,         4 * kKiB,  7 * kKiB,       16 * kKiB,
                              32 * kKiB, 48 * kKiB, kSuperblockSize};
      std::vector<SlabBuffer*> held;
      for (int i = 0; i < kRounds; ++i) {
        size_t size = sizes[rng() % std::size(sizes)];
        auto* buffer = pool->Alloc(size);
        if (buffer == nullptr) {
          ++failures;  // pool pressure is expected, just count it
        } else {
          EXPECT_GE(buffer->capacity, size);
          held.push_back(buffer);
        }
        if (held.size() > 8 || (buffer == nullptr && !held.empty())) {
          pool->Free(held.back());
          held.pop_back();
        }
      }
      for (auto* buffer : held) {
        pool->Free(buffer);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }

  {  // everything went back: all 16 whole superblocks are allocatable again
    std::vector<SlabBuffer*> superblocks;
    for (int i = 0; i < 16; ++i) {
      auto* superblock = pool->Alloc();
      ASSERT_NE(superblock, nullptr) << "leaked superblock, got " << i;
      superblocks.push_back(superblock);
    }
    for (auto* superblock : superblocks) {
      pool->Free(superblock);
    }
  }
}

TEST(SlabBufferPoolTest, InitializeGlobalSlabPool) {
  ASSERT_TRUE(InitializeGlobalSlabPool().ok());
  ASSERT_TRUE(InitializeGlobalSlabPool().ok());  // idempotent
  EXPECT_NE(GetGlobalReadSlabPool(), nullptr);
  EXPECT_NE(GetGlobalWriteSlabPool(), nullptr);

  // Global pools carve 4 MiB superblocks into 4K..2M classes.
  auto* pool = GetGlobalReadSlabPool();
  auto* object = pool->Alloc(256 * kKiB);
  ASSERT_NE(object, nullptr);
  EXPECT_EQ(object->capacity, 256 * kKiB);
  pool->Free(object);
}

}  // namespace cache
}  // namespace dingofs
