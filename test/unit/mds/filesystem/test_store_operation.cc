// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "dingofs/mds.pb.h"
#include "gtest/gtest.h"
#include "mds/common/codec.h"
#include "mds/common/tracing.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/dummy_storage.h"

namespace dingofs {
namespace mds {
namespace unit_test {

namespace {

constexpr uint32_t kChunkSize = 64 * 1024 * 1024;  // 64MB

SliceEntry MakeSlice(uint64_t id, uint64_t off, uint64_t len, uint64_t pos,
                     uint64_t size = 0) {
  SliceEntry s;
  s.set_id(id);
  s.set_off(off);
  s.set_len(len);
  s.set_pos(pos);
  s.set_size(size == 0 ? len : size);
  return s;
}

ChunkEntry MakeChunk(uint32_t index, std::vector<SliceEntry> slices) {
  ChunkEntry c;
  c.set_index(index);
  for (auto& s : slices) {
    *c.add_slices() = std::move(s);
  }
  return c;
}

CopyFileRangeOperation MakeOp(Trace& trace) {
  FsInfoEntry fs_info;
  fs_info.set_fs_id(1);
  CopyFileRangeOperation::Param param{};
  return CopyFileRangeOperation(trace, fs_info, param);
}

// Sort slices for stable comparisons (CloneSlice does not guarantee order
// within a chunk vector).
void SortByPos(std::vector<SliceEntry>& slices) {
  std::sort(slices.begin(), slices.end(),
            [](const SliceEntry& a, const SliceEntry& b) {
              return a.pos() < b.pos();
            });
}

}  // namespace

class CopyFileRangeCloneSliceTest : public ::testing::Test {
 protected:
  Trace trace_;
};

// Single slice fully inside one chunk, copied to a same-chunk dst with a
// different in-chunk position. No chunk-boundary crossing.
TEST_F(CopyFileRangeCloneSliceTest, SingleSliceSameChunkShifted) {
  auto op = MakeOp(trace_);

  // chunk 0: slice id=10, in-chunk pos=0, len=1024, backing off=4096
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {MakeSlice(10, /*off=*/4096, /*len=*/1024, /*pos=*/0)});

  // copy [0, 1024) -> [2048, 3072) within chunk 0
  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/2048,
                               /*len=*/1024, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_TRUE(out.contains(0));
  ASSERT_EQ(out[0].size(), 1u);

  const auto& s = out[0][0];
  EXPECT_EQ(s.id(), 10u);
  EXPECT_EQ(s.off(), 4096u);
  EXPECT_EQ(s.len(), 1024u);
  EXPECT_EQ(s.pos(), 2048u);
  EXPECT_EQ(s.size(), 1024u);
}

// Slices with id == 0 must be ignored (they represent holes / unreferenced).
TEST_F(CopyFileRangeCloneSliceTest, SkipsZeroIdSlices) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {
                            MakeSlice(0, 0, 1024, 0),    // hole, must skip
                            MakeSlice(7, 0, 1024, 1024),  // real
                        });

  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/4096, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 1u);
  EXPECT_EQ(out[0][0].id(), 7u);
  EXPECT_EQ(out[0][0].pos(), 1024u);
  EXPECT_EQ(out[0][0].len(), 1024u);
}

// A slice fully covered by another slice in the same chunk should be skipped.
TEST_F(CopyFileRangeCloneSliceTest, SkipsCoveredSlices) {
  auto op = MakeOp(trace_);

  // big slice (id=1) covers [0, 4096); inner slice (id=2) covers [1024, 2048).
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {
                            MakeSlice(1, 0, 4096, 0),
                            MakeSlice(2, 0, 1024, 1024),
                        });

  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/4096, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 1u);
  EXPECT_EQ(out[0][0].id(), 1u);
  EXPECT_EQ(out[0][0].len(), 4096u);
}

// Source range only partially intersects a slice; verify off / len / pos
// are clipped correctly.
TEST_F(CopyFileRangeCloneSliceTest, PartialIntersectionClipping) {
  auto op = MakeOp(trace_);

  // slice covers in-chunk [1024, 1024+4096)=[1024,5120), backing off=8192
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(
      0, {MakeSlice(/*id=*/5, /*off=*/8192, /*len=*/4096, /*pos=*/1024)});

  // copy [2048, 4096) -> [0, 2048) of dst. intersection w/ slice = [2048,4096)
  // off_in_slice = 2048-1024 = 1024 -> backing off 8192+1024=9216, len=2048.
  auto out = op.TestCloneSlice(src, /*src_off=*/2048, /*dst_off=*/0,
                               /*len=*/2048, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 1u);
  const auto& s = out[0][0];
  EXPECT_EQ(s.id(), 5u);
  EXPECT_EQ(s.off(), 9216u);
  EXPECT_EQ(s.len(), 2048u);
  EXPECT_EQ(s.pos(), 0u);
}

// dst_off mis-aligned vs src_off causes a single source slice to be split
// across two destination chunks.
TEST_F(CopyFileRangeCloneSliceTest, SplitsAcrossDstChunkBoundary) {
  auto op = MakeOp(trace_);

  // Source chunk 0 contains a single slice covering all of chunk 0.
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {MakeSlice(/*id=*/100, /*off=*/0,
                                   /*len=*/kChunkSize, /*pos=*/0)});

  // Copy entire chunk 0 to a destination starting half-chunk into dst chunk 0.
  // Half lands in dst chunk 0, the other half in dst chunk 1.
  const uint64_t half = kChunkSize / 2;
  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/half,
                               /*len=*/kChunkSize, kChunkSize);

  ASSERT_EQ(out.size(), 2u);
  ASSERT_EQ(out[0].size(), 1u);
  ASSERT_EQ(out[1].size(), 1u);

  const auto& a = out[0][0];
  EXPECT_EQ(a.id(), 100u);
  EXPECT_EQ(a.off(), 0u);
  EXPECT_EQ(a.len(), half);
  EXPECT_EQ(a.pos(), half);

  const auto& b = out[1][0];
  EXPECT_EQ(b.id(), 100u);
  EXPECT_EQ(b.off(), half);
  EXPECT_EQ(b.len(), half);
  EXPECT_EQ(b.pos(), 0u);
}

// Source range spans multiple source chunks; missing chunks are skipped.
TEST_F(CopyFileRangeCloneSliceTest, MultipleSourceChunksWithGap) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {MakeSlice(11, 0, 1024, 0)});
  // chunk 1 intentionally absent (sparse)
  src[2] = MakeChunk(2, {MakeSlice(13, 0, 1024, 0)});

  // copy from src_off=0, length spanning into chunk 2.
  const uint64_t len = 2 * kChunkSize + 1024;
  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0, len,
                               kChunkSize);

  ASSERT_EQ(out.size(), 2u);
  ASSERT_TRUE(out.contains(0));
  ASSERT_TRUE(out.contains(2));
  EXPECT_EQ(out[0].size(), 1u);
  EXPECT_EQ(out[0][0].id(), 11u);
  EXPECT_EQ(out[2].size(), 1u);
  EXPECT_EQ(out[2][0].id(), 13u);
}

// Empty source map => empty result.
TEST_F(CopyFileRangeCloneSliceTest, EmptySource) {
  auto op = MakeOp(trace_);
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  auto out =
      op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0, /*len=*/kChunkSize,
                        kChunkSize);
  EXPECT_TRUE(out.empty());
}

// Source range that does not intersect any slice produces no output.
TEST_F(CopyFileRangeCloneSliceTest, NoIntersection) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  // slice lives in [0, 1024) of chunk 0.
  src[0] = MakeChunk(0, {MakeSlice(9, 0, 1024, 0)});

  // copy from src_off=4096, len=1024 -> no overlap with slice.
  auto out = op.TestCloneSlice(src, /*src_off=*/4096, /*dst_off=*/0,
                               /*len=*/1024, kChunkSize);
  EXPECT_TRUE(out.empty());
}

// Multiple non-covered slices in one chunk all get cloned.
TEST_F(CopyFileRangeCloneSliceTest, MultipleSlicesPreserved) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {
                            MakeSlice(1, 0, 1024, 0),
                            MakeSlice(2, 0, 1024, 1024),
                            MakeSlice(3, 0, 1024, 2048),
                        });

  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/3 * 1024, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 3u);
  SortByPos(out[0]);
  EXPECT_EQ(out[0][0].id(), 1u);
  EXPECT_EQ(out[0][0].pos(), 0u);
  EXPECT_EQ(out[0][1].id(), 2u);
  EXPECT_EQ(out[0][1].pos(), 1024u);
  EXPECT_EQ(out[0][2].id(), 3u);
  EXPECT_EQ(out[0][2].pos(), 2048u);
}

// ---------------------------------------------------------------------------
// CopyFileRangeOperation::Run against DummyStorage. These cover the same-file
// (src_ino == dst_ino) path, where the source and destination resolve to one
// inode and one set of chunks.
// ---------------------------------------------------------------------------

namespace {

constexpr uint32_t kFsId = 1;
constexpr uint64_t kRunChunkSize = 8192;
constexpr uint64_t kRunBlockSize = 4096;

FsInfoEntry MakeRunFsInfo() {
  FsInfoEntry fs_info;
  fs_info.set_fs_id(kFsId);
  fs_info.set_chunk_size(kRunChunkSize);
  fs_info.set_block_size(kRunBlockSize);
  return fs_info;
}

AttrEntry MakeFileInode(Ino ino, uint64_t length) {
  AttrEntry attr;
  attr.set_ino(ino);
  attr.set_type(pb::mds::FileType::FILE);
  attr.set_length(length);
  attr.set_version(1);
  return attr;
}

}  // namespace

class CopyFileRangeRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = DummyStorage::New();
    ASSERT_TRUE(storage_->Init(""));
  }

  void Seed(const std::string& key, const std::string& value) {
    ASSERT_TRUE(storage_->Put(KVStorage::WriteOption(), key, value).ok());
  }

  AttrEntry GetInode(Ino ino) {
    std::string value;
    EXPECT_TRUE(storage_->Get(MetaCodec::EncodeInodeKey(kFsId, ino), value).ok());
    return MetaCodec::DecodeInodeValue(value);
  }

  ChunkEntry GetChunk(Ino ino, uint64_t index) {
    std::string value;
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeChunkKey(kFsId, ino, index), value).ok());
    return MetaCodec::DecodeChunkValue(value);
  }

  Trace trace_;
  KVStorageSPtr storage_;
};

// Copy a range within the same file and the same chunk. The destination chunk
// must keep its original slice AND gain the cloned one (regression: the dst
// chunk used to be recreated empty, dropping the original slices).
TEST_F(CopyFileRangeRunTest, SameFileNonOverlappingSameChunk) {
  const Ino ino = 100;
  Seed(MetaCodec::EncodeInodeKey(kFsId, ino),
       MetaCodec::EncodeInodeValue(MakeFileInode(ino, 2048)));

  ChunkEntry chunk = MakeChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                             /*len=*/2048, /*pos=*/0)});
  chunk.set_chunk_size(kRunChunkSize);
  chunk.set_block_size(kRunBlockSize);
  Seed(MetaCodec::EncodeChunkKey(kFsId, ino, 0),
       MetaCodec::EncodeChunkValue(chunk));

  CopyFileRangeOperation::Param param{};
  param.src_ino = ino;
  param.dst_ino = ino;
  param.src_off = 0;
  param.dst_off = 4096;
  param.len = 2048;
  CopyFileRangeOperation op(trace_, MakeRunFsInfo(), param);

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.Run(txn).ok());
  ASSERT_TRUE(txn->Commit().ok());

  // Destination chunk keeps the original slice and gains the cloned one.
  ChunkEntry out_chunk = GetChunk(ino, 0);
  std::vector<SliceEntry> slices(out_chunk.slices().begin(),
                                 out_chunk.slices().end());
  SortByPos(slices);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 1000u);
  EXPECT_EQ(slices[0].pos(), 0u);
  EXPECT_EQ(slices[1].id(), 1000u);
  EXPECT_EQ(slices[1].pos(), 4096u);
  EXPECT_EQ(slices[1].len(), 2048u);
  EXPECT_EQ(slices[1].off(), 0u);

  // The single inode grew once and is now shared.
  AttrEntry out_attr = GetInode(ino);
  EXPECT_EQ(out_attr.length(), 6144u);
  EXPECT_TRUE(out_attr.shared_slice());
  EXPECT_EQ(out_attr.version(), 2u);

  // Both references are tracked against the same inode.
  std::string ref_value;
  ASSERT_TRUE(
      storage_->Get(MetaCodec::EncodeSliceRefKey(1000), ref_value).ok());
  SliceRefEntry ref = MetaCodec::DecodeSliceRefValue(ref_value);
  EXPECT_EQ(ref.ref_count(), 2);
  ASSERT_EQ(ref.inos_size(), 2);
  EXPECT_EQ(ref.inos(0), ino);
  EXPECT_EQ(ref.inos(1), ino);

  EXPECT_EQ(op.GetResult().bytes_copied, 2048u);
  EXPECT_EQ(op.GetResult().length_delta, 4096);
}

// Copy within the same file but into a different (new) chunk. The source chunk
// must stay untouched while the destination chunk is created with the clone.
TEST_F(CopyFileRangeRunTest, SameFileCopyToDifferentChunk) {
  const Ino ino = 200;
  Seed(MetaCodec::EncodeInodeKey(kFsId, ino),
       MetaCodec::EncodeInodeValue(MakeFileInode(ino, 4096)));

  ChunkEntry chunk0 = MakeChunk(0, {MakeSlice(/*id=*/2000, /*off=*/0,
                                              /*len=*/4096, /*pos=*/0)});
  chunk0.set_chunk_size(kRunChunkSize);
  chunk0.set_block_size(kRunBlockSize);
  Seed(MetaCodec::EncodeChunkKey(kFsId, ino, 0),
       MetaCodec::EncodeChunkValue(chunk0));

  CopyFileRangeOperation::Param param{};
  param.src_ino = ino;
  param.dst_ino = ino;
  param.src_off = 0;
  param.dst_off = kRunChunkSize;  // start of chunk 1
  param.len = 4096;
  CopyFileRangeOperation op(trace_, MakeRunFsInfo(), param);

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.Run(txn).ok());
  ASSERT_TRUE(txn->Commit().ok());

  // Source chunk is left exactly as it was.
  ChunkEntry out0 = GetChunk(ino, 0);
  ASSERT_EQ(out0.slices_size(), 1);
  EXPECT_EQ(out0.slices(0).id(), 2000u);
  EXPECT_EQ(out0.slices(0).pos(), 0u);

  // Destination chunk is freshly created holding only the cloned slice.
  ChunkEntry out1 = GetChunk(ino, 1);
  ASSERT_EQ(out1.slices_size(), 1);
  EXPECT_EQ(out1.slices(0).id(), 2000u);
  EXPECT_EQ(out1.slices(0).pos(), 0u);
  EXPECT_EQ(out1.slices(0).len(), 4096u);

  AttrEntry out_attr = GetInode(ino);
  EXPECT_EQ(out_attr.length(), kRunChunkSize + 4096);
  EXPECT_TRUE(out_attr.shared_slice());
  EXPECT_EQ(op.GetResult().bytes_copied, 4096u);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs