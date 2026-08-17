/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "client/vfs/data/slice/block_data.h"
#include "client/vfs/data/slice/common.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::AnyNumber;
using ::testing::Return;

namespace {

constexpr int64_t kPageSize = 4096;
constexpr int64_t kBlockSize = 4 * kPageSize;
constexpr uint64_t kChunkSize = 64 * 1024 * 1024;
constexpr uint64_t kFsId = 1;
constexpr uint64_t kIno = 100;
constexpr uint64_t kChunkIndex = 0;

class BlockDataTest : public test::VFSTestBase {
 protected:
  void SetUp() override {
    trace_manager_ = std::make_unique<TraceManager>();
    ON_CALL(*mock_hub_, GetTraceManager())
        .WillByDefault(Return(trace_manager_.get()));
    EXPECT_CALL(*mock_hub_, GetTraceManager()).Times(AnyNumber());
    context_ = std::make_unique<SliceDataContext>(
        kFsId, kIno, kChunkIndex, kChunkSize, kBlockSize, kPageSize);
  }

  std::unique_ptr<WriteMemPool> MakePool(int pages) {
    return std::make_unique<WriteMemPool>(pages * kPageSize, kPageSize);
  }

  std::unique_ptr<BlockData> MakeBlock(WriteMemPool* pool,
                                       int32_t block_offset = 0) {
    return std::make_unique<BlockData>(*context_, mock_hub_, pool, 0,
                                       block_offset);
  }

  WritePageLease Acquire(WriteMemPool* pool, size_t pages) {
    WritePageLease lease;
    EXPECT_TRUE(pool->Acquire(pages, &lease).ok());
    return lease;
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<SliceDataContext> context_;
};

TEST_F(BlockDataTest, ReservePagesConsumesLeaseWithoutChangingLength) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());
  auto lease = Acquire(pool.get(), 3);

  block->ReservePages(3 * kPageSize, 0, &lease);

  EXPECT_TRUE(lease.Empty());
  EXPECT_EQ(block->Len(), 0);
  EXPECT_EQ(pool->GetUsedBytes(), 3 * kPageSize);
}

TEST_F(BlockDataTest, ExistingPageConsumesOnlyMissingLeasePage) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());
  auto first = Acquire(pool.get(), 1);
  block->ReservePages(kPageSize, 0, &first);
  ASSERT_TRUE(first.Empty());

  auto second = Acquire(pool.get(), 1);
  block->ReservePages(2 * kPageSize, 0, &second);

  EXPECT_TRUE(second.Empty());
  EXPECT_EQ(pool->GetUsedBytes(), 2 * kPageSize);
}

TEST_F(BlockDataTest, UnusedLeasePagesReturnAutomatically) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());
  {
    auto lease = Acquire(pool.get(), 3);
    block->ReservePages(kPageSize, 0, &lease);
    EXPECT_EQ(lease.Size(), 2);
    EXPECT_EQ(pool->GetUsedBytes(), 3 * kPageSize);
  }
  EXPECT_EQ(pool->GetUsedBytes(), kPageSize);
}

TEST_F(BlockDataTest, ApplyWriteUsesReservedPagesAndPreservesBytes) {
  auto pool = MakePool(4);
  constexpr int32_t kStart = 137;
  constexpr int32_t kSize = 2 * kPageSize + 777;
  auto block = MakeBlock(pool.get(), kStart);
  auto lease = Acquire(pool.get(), 3);
  block->ReservePages(kSize, kStart, &lease);
  ASSERT_TRUE(lease.Empty());

  std::vector<char> input(kSize);
  for (size_t i = 0; i < input.size(); ++i) {
    input[i] = static_cast<char>((i * 37 + 11) % 251);
  }
  block->ApplyWrite(ctx_, input.data(), input.size(), kStart);

  EXPECT_EQ(block->Len(), kSize);
  EXPECT_EQ(pool->GetUsedBytes(), 3 * kPageSize);
  IOBuffer output = block->ToIOBuffer();
  ASSERT_EQ(output.Size(), input.size());
  ASSERT_EQ(output.BackingBlockNum(), 3);
  std::vector<char> copied(input.size());
  output.CopyTo(copied.data(), copied.size());
  EXPECT_EQ(copied, input);
}

TEST_F(BlockDataTest, DestructorReturnsOwnedPages) {
  auto pool = MakePool(4);
  {
    auto block = MakeBlock(pool.get());
    auto lease = Acquire(pool.get(), 3);
    block->ReservePages(3 * kPageSize, 0, &lease);
    ASSERT_EQ(pool->GetUsedBytes(), 3 * kPageSize);
  }
  EXPECT_EQ(pool->GetUsedBytes(), 0);
}

}  // namespace
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
