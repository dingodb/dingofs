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
#include "common/status.h"
#include "common/trace/trace_manager.h"
#include "common/writemempool/write_mem_pool.h"
#include "test/unit/client/vfs/test_base.h"

namespace dingofs {
namespace client {
namespace vfs {

using ::testing::AnyNumber;
using ::testing::Return;

// Small geometry so tests stay cheap: 16 KiB block / 4 KiB page = 4 pages.
static constexpr int64_t kPageSize = 4096;
static constexpr int64_t kBlockSize = 4 * kPageSize;  // 4 pages
static constexpr uint64_t kChunkSize = 64 * 1024 * 1024;
static constexpr uint64_t kFsId = 1;
static constexpr uint64_t kIno = 100;
static constexpr uint64_t kChunkIndex = 0;

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

  // Build a pool with `slots` pages of kPageSize.
  std::unique_ptr<WriteMemPool> MakePool(int slots) {
    return std::make_unique<WriteMemPool>(
        static_cast<int64_t>(slots) * kPageSize, kPageSize);
  }

  std::unique_ptr<BlockData> MakeBlock(WriteMemPool* pool,
                                       int32_t block_offset = 0) {
    return std::make_unique<BlockData>(*context_, mock_hub_, pool,
                                       /*block_index=*/0, block_offset);
  }

  std::unique_ptr<TraceManager> trace_manager_;
  std::unique_ptr<SliceDataContext> context_;
};

// ReservePages allocates the pages a write needs, records the new ones, and
// leaves len_ untouched (reserve != apply).
TEST_F(BlockDataTest, ReservePages_Success_RecordsNewPages_NoLenBump) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());

  std::vector<uint32_t> created;
  Status s =
      block->ReservePages(/*size=*/3 * kPageSize, /*block_offset=*/0, &created);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(created.size(), 3u);
  EXPECT_EQ(block->Len(), 0);  // reserve does not bump len_
  EXPECT_EQ(pool->GetUsedBytes(), 3 * kPageSize);
}

// A reserve that outgrows the pool returns NoSpace; it does NOT self-rollback
// (the SliceWriter transaction owns cross-block undo), so the pages it managed
// to take stay recorded for the caller to roll back.
TEST_F(BlockDataTest, ReservePages_Exhausted_ReturnsNoSpace_NoSelfRollback) {
  auto pool = MakePool(2);
  auto block = MakeBlock(pool.get());

  std::vector<uint32_t> created;
  Status s =
      block->ReservePages(/*size=*/3 * kPageSize, /*block_offset=*/0, &created);
  EXPECT_TRUE(s.IsNoSpace()) << s.ToString();
  EXPECT_EQ(created.size(), 2u);  // took 2 before the 3rd missed
  EXPECT_EQ(pool->GetUsedBytes(), 2 * kPageSize);  // not self-rolled-back
}

// Pages already present (e.g. a partially-filled straddle page) are not
// re-allocated and not recorded, so a later RollbackPages can't free them.
TEST_F(BlockDataTest, ReservePages_SkipsExistingPages) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());

  std::vector<uint32_t> first;
  ASSERT_TRUE(block->ReservePages(kPageSize, 0, &first).ok());
  ASSERT_EQ(first.size(), 1u);

  // Reserve [0, 2 pages): page0 already exists -> only page1 is new.
  std::vector<uint32_t> second;
  ASSERT_TRUE(block->ReservePages(2 * kPageSize, 0, &second).ok());
  EXPECT_EQ(second.size(), 1u);
  EXPECT_EQ(second[0], 1u);
  EXPECT_EQ(pool->GetUsedBytes(), 2 * kPageSize);
}

// RollbackPages frees exactly the named pages and nothing else.
TEST_F(BlockDataTest, RollbackPages_FreesOnlyNamedPages) {
  auto pool = MakePool(4);
  auto block = MakeBlock(pool.get());

  std::vector<uint32_t> created;
  ASSERT_TRUE(block->ReservePages(3 * kPageSize, 0, &created).ok());
  ASSERT_EQ(pool->GetUsedBytes(), 3 * kPageSize);

  // Roll back only page index 2 -> 2 pages remain.
  block->RollbackPages({created.back()});
  EXPECT_EQ(pool->GetUsedBytes(), 2 * kPageSize);

  // Roll back the rest -> back to empty.
  block->RollbackPages({created[0], created[1]});
  EXPECT_EQ(pool->GetUsedBytes(), 0);
}

// ApplyWrite memcpys into already-reserved pages and bumps len_, and must NOT
// allocate -- the pool is drained dry before ApplyWrite and it still succeeds.
TEST_F(BlockDataTest, ApplyWrite_NoAllocation_AfterReserve) {
  auto pool = MakePool(2);
  auto block = MakeBlock(pool.get());

  const int32_t size = 2 * kPageSize;
  std::vector<uint32_t> created;
  ASSERT_TRUE(block->ReservePages(size, 0, &created).ok());
  ASSERT_EQ(pool->GetUsedBytes(), size);  // pool now fully drained

  std::vector<char> buf(size, 'Z');
  block->ApplyWrite(ctx_, buf.data(), size, 0);

  EXPECT_EQ(block->Len(), size);
  EXPECT_EQ(pool->GetUsedBytes(), size);  // ApplyWrite did not allocate
  // Data is durable in the pages.
  EXPECT_EQ(block->ToIOBuffer().Size(), static_cast<size_t>(size));
}

// The reserve -> rollback -> reserve+apply sequence (what SliceWriter drives
// per block) is atomic: a reserve larger than the pool leaves len_==0,
// RollbackPages returns every page, and a same-offset reserve+apply afterwards
// is clean (no contiguity CHECK trip).
TEST_F(BlockDataTest, ReserveRollback_ThenReserveApply_SameOffsetClean) {
  auto pool = MakePool(2);
  auto block = MakeBlock(pool.get());

  // Reserve more than the pool holds -> NoSpace, len untouched.
  std::vector<uint32_t> created;
  Status s = block->ReservePages(3 * kPageSize, 0, &created);
  EXPECT_TRUE(s.IsNoSpace()) << s.ToString();
  EXPECT_EQ(block->Len(), 0);

  // Roll back the partial reservation -> pool fully returned.
  block->RollbackPages(created);
  EXPECT_EQ(pool->GetUsedBytes(), 0);
  EXPECT_EQ(block->Len(), 0);

  // Same offset now reserves + applies cleanly (2 pages fit).
  std::vector<uint32_t> created2;
  ASSERT_TRUE(block->ReservePages(2 * kPageSize, 0, &created2).ok());
  std::vector<char> buf(2 * kPageSize, 'y');
  block->ApplyWrite(ctx_, buf.data(), 2 * kPageSize, 0);
  EXPECT_EQ(block->Len(), 2 * kPageSize);
  EXPECT_EQ(pool->GetUsedBytes(), 2 * kPageSize);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
