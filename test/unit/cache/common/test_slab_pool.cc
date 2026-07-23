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
 * Created Date: 2026-07-23
 * Author: AI
 */

#include "cache/common/slab_pool.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <utility>
#include <vector>

#include "cache/common/slab_allocator.h"
#include "common/io_buffer.h"

namespace dingofs {
namespace cache {

namespace {
constexpr size_t kSuperblockSize = SlabAllocator::kSuperblockSize;  // 4 MiB
}  // namespace

TEST(SlabPoolTest, AcquireExposesKeysAndIndex) {
  auto pool = SlabPool::Create(4);
  ASSERT_NE(pool, nullptr);
  pool->SetRdmaKeys(0x1111, 0x2222);

  auto lease = pool->Acquire(8 * 1024);
  ASSERT_TRUE(lease.ok());
  EXPECT_NE(lease.data(), nullptr);
  EXPECT_EQ(lease.lkey(), 0x1111u);
  EXPECT_EQ(lease.rkey(), 0x2222u);
  EXPECT_GE(lease.index(), 0);
  EXPECT_LT(lease.index(), 4);
  EXPECT_EQ(lease.index(), pool->IndexOf(lease.data()));
}

TEST(SlabPoolTest, OversizeAndZeroReturnInvalidLease) {
  auto pool = SlabPool::Create(2);
  ASSERT_NE(pool, nullptr);

  EXPECT_FALSE(pool->Acquire(kSuperblockSize + 1).ok());
  EXPECT_FALSE(pool->Acquire(0).ok());
}

TEST(SlabPoolTest, ExhaustionReturnsInvalidLeaseThenRecovers) {
  auto pool = SlabPool::Create(1);  // exactly one 4 MiB superblock
  ASSERT_NE(pool, nullptr);

  {
    auto held = pool->Acquire(kSuperblockSize);
    ASSERT_TRUE(held.ok());
    EXPECT_FALSE(pool->Acquire(kSuperblockSize).ok());  // pool drained
  }
  // The lease above was destroyed, so the superblock is back in the pool.
  EXPECT_TRUE(pool->Acquire(kSuperblockSize).ok());
}

TEST(SlabPoolTest, DestructionReturnsSlab) {
  auto pool = SlabPool::Create(1);
  ASSERT_NE(pool, nullptr);

  const char* first = nullptr;
  {
    auto lease = pool->Acquire(kSuperblockSize);
    ASSERT_TRUE(lease.ok());
    first = lease.data();
  }
  auto again = pool->Acquire(kSuperblockSize);
  ASSERT_TRUE(again.ok());
  EXPECT_EQ(again.data(), first);  // same slot reused after return
}

TEST(SlabPoolTest, MoveIntoTransfersOwnershipWithoutDoubleFree) {
  auto pool = SlabPool::Create(1);
  ASSERT_NE(pool, nullptr);
  pool->SetRdmaKeys(0x42, 0);

  {
    IOBuffer buffer;
    {
      auto lease = pool->Acquire(kSuperblockSize);
      ASSERT_TRUE(lease.ok());
      lease.MoveInto(&buffer, 1024);
      EXPECT_FALSE(lease.ok());  // ownership handed to the IOBuffer
    }
    // Lease destroyed above must NOT free; the slab is still held by buffer.
    EXPECT_FALSE(pool->Acquire(kSuperblockSize).ok());
    EXPECT_EQ(buffer.Size(), 1024u);
    EXPECT_EQ(buffer.GetFirstDataMeta(), 0x42u);  // pool lkey carried through
  }
  // buffer destroyed -> deleter returns the slab exactly once.
  EXPECT_TRUE(pool->Acquire(kSuperblockSize).ok());
}

TEST(SlabPoolTest, MoveConstructAndAssignTransferOwnership) {
  auto pool = SlabPool::Create(1);
  ASSERT_NE(pool, nullptr);

  auto src = pool->Acquire(kSuperblockSize);
  ASSERT_TRUE(src.ok());
  char* data = src.data();

  SlabLease moved(std::move(src));
  EXPECT_FALSE(src.ok());
  EXPECT_EQ(moved.data(), data);

  SlabLease sink;
  sink = std::move(moved);
  EXPECT_FALSE(moved.ok());
  EXPECT_EQ(sink.data(), data);

  // Still only one slab outstanding: the pool stays drained.
  EXPECT_FALSE(pool->Acquire(kSuperblockSize).ok());
}

TEST(SlabPoolTest, FetchReturnsContiguousSuperblockIovecs) {
  auto pool = SlabPool::Create(3);
  ASSERT_NE(pool, nullptr);

  auto iovecs = pool->Fetch();
  ASSERT_EQ(iovecs.size(), 3u);
  for (size_t i = 0; i < iovecs.size(); ++i) {
    EXPECT_EQ(iovecs[i].iov_len, kSuperblockSize);
    EXPECT_EQ(static_cast<char*>(iovecs[i].iov_base),
              pool->BaseAddr() + (i * kSuperblockSize));
  }
}

TEST(SlabPoolTest, IndexOfOutsidePoolIsNegative) {
  auto pool = SlabPool::Create(2);
  ASSERT_NE(pool, nullptr);

  char outside = 0;
  EXPECT_EQ(pool->IndexOf(&outside), -1);
}

TEST(SlabPoolTest, GlobalPoolInitializesOnceAndIsStable) {
  ASSERT_TRUE(InitializeGlobalSlabPool().ok());
  EXPECT_NE(GetGlobalSlabPool(), nullptr);
  EXPECT_EQ(GetGlobalSlabPool(), GetGlobalSlabPool());
}

}  // namespace cache
}  // namespace dingofs
