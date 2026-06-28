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
#include <fcntl.h>

#include <algorithm>
#include <cstdint>
#include <vector>

#include "common/meta.h"
#include "dingofs/error.pb.h"
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
  s.set_size(size == 0 ? (id == 0 ? 0 : len) : size);
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

// Slices with id == 0 represent visible zero ranges and must be copied so they
// overwrite existing destination data.
TEST_F(CopyFileRangeCloneSliceTest, CopiesZeroIdSlices) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {
                            MakeSlice(0, 0, 1024, 0),     // zero range
                            MakeSlice(7, 0, 1024, 1024),  // real
                        });

  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/4096, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 3u);
  SortByPos(out[0]);
  EXPECT_EQ(out[0][0].id(), 0u);
  EXPECT_EQ(out[0][0].pos(), 0u);
  EXPECT_EQ(out[0][0].len(), 1024u);
  EXPECT_EQ(out[0][0].size(), 0u);
  EXPECT_EQ(out[0][1].id(), 7u);
  EXPECT_EQ(out[0][1].pos(), 1024u);
  EXPECT_EQ(out[0][1].len(), 1024u);
  EXPECT_EQ(out[0][2].id(), 0u);  // sparse tail in the copied source range
  EXPECT_EQ(out[0][2].pos(), 2048u);
  EXPECT_EQ(out[0][2].len(), 2048u);
}

// Newer overlapping slices win, but the still-visible portions of older slices
// must be preserved.
TEST_F(CopyFileRangeCloneSliceTest, NewerSliceOverridesOlderSlice) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {
                            MakeSlice(1, 0, 4096, 0),     // older
                            MakeSlice(2, 0, 1024, 1024),  // newer
                        });

  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/4096, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 3u);
  SortByPos(out[0]);
  EXPECT_EQ(out[0][0].id(), 1u);
  EXPECT_EQ(out[0][0].pos(), 0u);
  EXPECT_EQ(out[0][0].len(), 1024u);
  EXPECT_EQ(out[0][0].off(), 0u);
  EXPECT_EQ(out[0][1].id(), 2u);
  EXPECT_EQ(out[0][1].pos(), 1024u);
  EXPECT_EQ(out[0][1].len(), 1024u);
  EXPECT_EQ(out[0][2].id(), 1u);
  EXPECT_EQ(out[0][2].pos(), 2048u);
  EXPECT_EQ(out[0][2].len(), 2048u);
  EXPECT_EQ(out[0][2].off(), 2048u);
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

// Source range spans multiple source chunks; sparse gaps become zero slices.
TEST_F(CopyFileRangeCloneSliceTest, MultipleSourceChunksWithGap) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  src[0] = MakeChunk(0, {MakeSlice(11, 0, 1024, 0)});
  // chunk 1 intentionally absent (sparse)
  src[2] = MakeChunk(2, {MakeSlice(13, 0, 1024, 0)});

  // copy from src_off=0, length spanning into chunk 2.
  const uint64_t len = 2 * kChunkSize + 1024;
  auto out =
      op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0, len, kChunkSize);

  ASSERT_EQ(out.size(), 3u);
  ASSERT_TRUE(out.contains(0));
  ASSERT_TRUE(out.contains(1));
  ASSERT_TRUE(out.contains(2));
  SortByPos(out[0]);
  EXPECT_EQ(out[0].size(), 2u);
  EXPECT_EQ(out[0][0].id(), 11u);
  EXPECT_EQ(out[0][1].id(), 0u);
  EXPECT_EQ(out[0][1].pos(), 1024u);
  EXPECT_EQ(out[0][1].len(), kChunkSize - 1024);
  ASSERT_EQ(out[1].size(), 1u);
  EXPECT_EQ(out[1][0].id(), 0u);
  EXPECT_EQ(out[1][0].pos(), 0u);
  EXPECT_EQ(out[1][0].len(), kChunkSize);
  EXPECT_EQ(out[2].size(), 1u);
  EXPECT_EQ(out[2][0].id(), 13u);
}

// Empty source map means the copied source range is sparse.
TEST_F(CopyFileRangeCloneSliceTest, EmptySource) {
  auto op = MakeOp(trace_);
  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  auto out = op.TestCloneSlice(src, /*src_off=*/0, /*dst_off=*/0,
                               /*len=*/kChunkSize, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_TRUE(out.contains(0));
  ASSERT_EQ(out[0].size(), 1u);
  EXPECT_EQ(out[0][0].id(), 0u);
  EXPECT_EQ(out[0][0].pos(), 0u);
  EXPECT_EQ(out[0][0].len(), kChunkSize);
}

// Source range that does not intersect any slice copies a sparse zero range.
TEST_F(CopyFileRangeCloneSliceTest, NoIntersection) {
  auto op = MakeOp(trace_);

  absl::flat_hash_map<uint64_t, ChunkEntry> src;
  // slice lives in [0, 1024) of chunk 0.
  src[0] = MakeChunk(0, {MakeSlice(9, 0, 1024, 0)});

  // copy from src_off=4096, len=1024 -> no overlap with slice.
  auto out = op.TestCloneSlice(src, /*src_off=*/4096, /*dst_off=*/0,
                               /*len=*/1024, kChunkSize);
  ASSERT_EQ(out.size(), 1u);
  ASSERT_EQ(out[0].size(), 1u);
  EXPECT_EQ(out[0][0].id(), 0u);
  EXPECT_EQ(out[0][0].pos(), 0u);
  EXPECT_EQ(out[0][0].len(), 1024u);
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

AttrEntry MakeDirInode(Ino ino) {
  AttrEntry attr;
  attr.set_ino(ino);
  attr.set_type(pb::mds::FileType::DIRECTORY);
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
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeInodeKey(kFsId, ino), value).ok());
    return MetaCodec::DecodeInodeValue(value);
  }

  ChunkEntry GetChunk(Ino ino, uint64_t index) {
    std::string value;
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeChunkKey(kFsId, ino, index), value)
            .ok());
    return MetaCodec::DecodeChunkValue(value);
  }

  void SeedInode(const AttrEntry& attr) {
    Seed(MetaCodec::EncodeInodeKey(kFsId, attr.ino()),
         MetaCodec::EncodeInodeValue(attr));
  }

  // Seeds a chunk for `ino`, stamping the run chunk/block sizes.
  void SeedChunkFor(Ino ino, ChunkEntry chunk) {
    chunk.set_chunk_size(kRunChunkSize);
    chunk.set_block_size(kRunBlockSize);
    Seed(MetaCodec::EncodeChunkKey(kFsId, ino, chunk.index()),
         MetaCodec::EncodeChunkValue(chunk));
  }

  bool ChunkExists(Ino ino, uint64_t index) {
    std::string value;
    return storage_->Get(MetaCodec::EncodeChunkKey(kFsId, ino, index), value)
        .ok();
  }

  std::vector<SliceEntry> SortedSlicesOf(Ino ino, uint64_t index) {
    ChunkEntry chunk = GetChunk(ino, index);
    std::vector<SliceEntry> slices(chunk.slices().begin(),
                                   chunk.slices().end());
    SortByPos(slices);
    return slices;
  }

  bool SliceRefExists(uint64_t slice_id) {
    std::string value;
    return storage_->Get(MetaCodec::EncodeSliceRefKey(slice_id), value).ok();
  }

  SliceRefEntry GetSliceRef(uint64_t slice_id) {
    std::string value;
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeSliceRefKey(slice_id), value).ok());
    return MetaCodec::DecodeSliceRefValue(value);
  }

  Status RunCopy(const CopyFileRangeOperation::Param& param,
                 CopyFileRangeOperation::Result* result = nullptr) {
    CopyFileRangeOperation op(trace_, MakeRunFsInfo(), param);
    auto txn = storage_->NewTxn();
    auto status = op.Run(txn);
    if (status.ok()) {
      EXPECT_TRUE(txn->Commit().ok());
      if (result != nullptr) *result = op.GetResult();
    }
    return status;
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

TEST_F(CopyFileRangeRunTest, SameFileZeroSourceOverwritesDstWithZeroSlice) {
  const Ino ino = 250;
  SeedInode(MakeFileInode(ino, kRunChunkSize));
  SeedChunkFor(ino, MakeChunk(0, {
                                     MakeSlice(/*id=*/2500, 0, 2048, 0),
                                     MakeSlice(/*id=*/0, 0, 1024, 4096),
                                 }));

  CopyFileRangeOperation::Param param{};
  param.src_ino = ino;
  param.dst_ino = ino;
  param.src_off = 4096;
  param.dst_off = 1024;
  param.len = 1024;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  auto slices = SortedSlicesOf(ino, 0);
  ASSERT_EQ(slices.size(), 3u);
  EXPECT_EQ(slices[0].id(), 2500u);
  EXPECT_EQ(slices[0].pos(), 0u);
  EXPECT_EQ(slices[1].id(), 0u);
  EXPECT_EQ(slices[1].pos(), 1024u);
  EXPECT_EQ(slices[1].len(), 1024u);
  EXPECT_EQ(slices[1].size(), 0u);
  EXPECT_EQ(slices[2].id(), 0u);
  EXPECT_EQ(slices[2].pos(), 4096u);
  EXPECT_FALSE(SliceRefExists(0));
  EXPECT_EQ(result.bytes_copied, 1024u);
  EXPECT_EQ(result.length_delta, 0);
}

// --- cross-file copies (src_ino != dst_ino): the non-same_file path ----------

// Cross-file copy into an empty destination: a dst chunk is created with the
// cloned slice, both inodes are marked shared and version-bumped, and the slice
// ref tracks the two DISTINCT inodes. The source side is left intact.
TEST_F(CopyFileRangeRunTest, CrossFileCopyToEmptyDst) {
  const Ino src = 300;
  const Ino dst = 301;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, 0));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3000, /*off=*/0,
                                            /*len=*/2048, /*pos=*/0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 2048;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  // Destination chunk created holding the cloned slice.
  ChunkEntry dst_chunk = GetChunk(dst, 0);
  ASSERT_EQ(dst_chunk.slices_size(), 1);
  EXPECT_EQ(dst_chunk.slices(0).id(), 3000u);
  EXPECT_EQ(dst_chunk.slices(0).pos(), 0u);
  EXPECT_EQ(dst_chunk.slices(0).len(), 2048u);

  // Destination inode grew and is shared.
  AttrEntry dst_attr = GetInode(dst);
  EXPECT_EQ(dst_attr.length(), 2048u);
  EXPECT_TRUE(dst_attr.shared_slice());
  EXPECT_EQ(dst_attr.version(), 2u);
  EXPECT_GT(dst_attr.mtime(), 0u);
  EXPECT_GT(dst_attr.ctime(), 0u);

  // Source inode is marked shared and bumped, but its data is untouched.
  AttrEntry src_attr = GetInode(src);
  EXPECT_EQ(src_attr.length(), 2048u);
  EXPECT_TRUE(src_attr.shared_slice());
  EXPECT_EQ(src_attr.version(), 2u);
  ChunkEntry src_chunk = GetChunk(src, 0);
  ASSERT_EQ(src_chunk.slices_size(), 1);
  EXPECT_EQ(src_chunk.slices(0).id(), 3000u);

  // Slice ref records both distinct inodes.
  SliceRefEntry ref = GetSliceRef(3000);
  EXPECT_EQ(ref.ref_count(), 2);
  ASSERT_EQ(ref.inos_size(), 2);
  EXPECT_EQ(ref.inos(0), src);
  EXPECT_EQ(ref.inos(1), dst);

  EXPECT_EQ(result.bytes_copied, 2048u);
  EXPECT_EQ(result.length_delta, 2048);
}

// Cross-file copy into a destination chunk that already holds a slice: the
// existing slice is preserved and the clone is appended.
TEST_F(CopyFileRangeRunTest, CrossFileDstKeepsExistingSlices) {
  const Ino src = 310;
  const Ino dst = 311;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, 2048));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3100, 0, 2048, 0)}));
  SeedChunkFor(dst, MakeChunk(0, {MakeSlice(/*id=*/3111, 0, 2048, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 4096;  // same chunk, later position
  param.len = 2048;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  auto slices = SortedSlicesOf(dst, 0);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 3111u);  // original preserved
  EXPECT_EQ(slices[0].pos(), 0u);
  EXPECT_EQ(slices[1].id(), 3100u);  // clone appended
  EXPECT_EQ(slices[1].pos(), 4096u);
  EXPECT_EQ(slices[1].len(), 2048u);

  // Grew from 2048 to dst_off+len = 6144.
  EXPECT_EQ(GetInode(dst).length(), 6144u);
  EXPECT_EQ(result.length_delta, 4096);
}

TEST_F(CopyFileRangeRunTest, CrossFileZeroSourceOverwritesDstWithZeroSlice) {
  const Ino src = 312;
  const Ino dst = 313;
  SeedInode(MakeFileInode(src, 4096));
  SeedInode(MakeFileInode(dst, 4096));
  SeedChunkFor(src, MakeChunk(0, {
                                     MakeSlice(/*id=*/3120, 0, 4096, 0),
                                     MakeSlice(/*id=*/0, 0, 1024, 1024),
                                 }));
  SeedChunkFor(dst, MakeChunk(0, {MakeSlice(/*id=*/3130, 0, 4096, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 1024;
  param.dst_off = 2048;
  param.len = 1024;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  auto slices = SortedSlicesOf(dst, 0);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 3130u);
  EXPECT_EQ(slices[0].pos(), 0u);
  EXPECT_EQ(slices[1].id(), 0u);
  EXPECT_EQ(slices[1].pos(), 2048u);
  EXPECT_EQ(slices[1].len(), 1024u);
  EXPECT_EQ(slices[1].size(), 0u);
  EXPECT_FALSE(SliceRefExists(0));
  EXPECT_EQ(result.bytes_copied, 1024u);
  EXPECT_EQ(result.length_delta, 0);
}

// len that runs past the source EOF is clamped to the bytes actually available
// (src_attr.length - src_off).
TEST_F(CopyFileRangeRunTest, CrossFileLengthClampedToSrcEof) {
  const Ino src = 334;
  const Ino dst = 335;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, 0));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3200, 0, 2048, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 8192;  // far more than the 2048 available

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  EXPECT_EQ(result.bytes_copied, 2048u);  // clamped, not 8192
  EXPECT_EQ(result.length_delta, 2048);
  EXPECT_EQ(GetInode(dst).length(), 2048u);
  EXPECT_EQ(GetChunk(dst, 0).slices(0).len(), 2048u);
}

// Copying into a region fully inside the destination's current length must not
// change the size, but still refreshes mtime/ctime and the shared flag.
TEST_F(CopyFileRangeRunTest, CrossFileNoGrowthWhenWithinDstLength) {
  const Ino src = 314;
  const Ino dst = 315;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, kRunChunkSize));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3300, 0, 2048, 0)}));
  SeedChunkFor(dst,
               MakeChunk(0, {MakeSlice(/*id=*/3311, 0, kRunChunkSize, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 2048;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  EXPECT_EQ(result.length_delta, 0);
  AttrEntry dst_attr = GetInode(dst);
  EXPECT_EQ(dst_attr.length(), kRunChunkSize);  // unchanged
  EXPECT_TRUE(dst_attr.shared_slice());
  EXPECT_GT(dst_attr.mtime(), 0u);
  EXPECT_GT(dst_attr.ctime(), 0u);
}

// A sparse source hole (a chunk index with no backing chunk) inside the copy
// range yields a destination zero slice so any old destination data would be
// overwritten.
TEST_F(CopyFileRangeRunTest, CrossFileSparseSourceHoleCreatesZeroDstChunk) {
  const Ino src = 316;
  const Ino dst = 317;
  const uint64_t length = 2 * kRunChunkSize;
  SeedInode(MakeFileInode(src, length));  // sparse: chunk 1 has no data
  SeedInode(MakeFileInode(dst, 0));
  SeedChunkFor(src,
               MakeChunk(0, {MakeSlice(/*id=*/3400, 0, kRunChunkSize, 0)}));
  // chunk 1 intentionally absent.

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = length;

  CopyFileRangeOperation::Result result;
  ASSERT_TRUE(RunCopy(param, &result).ok());

  // Chunk 0 got the clone.
  ASSERT_EQ(GetChunk(dst, 0).slices_size(), 1);
  EXPECT_EQ(GetChunk(dst, 0).slices(0).id(), 3400u);

  // Chunk 1 was created from the hole: present with a full-chunk zero slice.
  ASSERT_TRUE(ChunkExists(dst, 1));
  ChunkEntry zero_chunk = GetChunk(dst, 1);
  ASSERT_EQ(zero_chunk.slices_size(), 1);
  EXPECT_EQ(zero_chunk.slices(0).id(), 0u);
  EXPECT_EQ(zero_chunk.slices(0).pos(), 0u);
  EXPECT_EQ(zero_chunk.slices(0).len(), kRunChunkSize);
  EXPECT_EQ(zero_chunk.version(), 1u);

  EXPECT_EQ(result.bytes_copied, length);
}

// An existing slice ref in storage is incremented (and the dst ino appended)
// rather than recreated — exercises the "ref already exists" branch of
// UpsertSliceRef.
TEST_F(CopyFileRangeRunTest, CrossFileIncrementsExistingSliceRef) {
  const Ino src = 318;
  const Ino dst = 319;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, 0));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3500, 0, 2048, 0)}));

  // Pre-existing ref for slice 3500 with some prior sharers.
  SliceRefEntry seed_ref;
  seed_ref.set_id(3500);
  seed_ref.set_ref_count(5);
  seed_ref.add_inos(700);
  Seed(MetaCodec::EncodeSliceRefKey(3500),
       MetaCodec::EncodeSliceRefValue(seed_ref));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 2048;

  ASSERT_TRUE(RunCopy(param).ok());

  SliceRefEntry ref = GetSliceRef(3500);
  EXPECT_EQ(ref.ref_count(), 6);  // 5 -> 6, not reset to 2
  ASSERT_EQ(ref.inos_size(), 2);
  EXPECT_EQ(ref.inos(0), 700u);
  EXPECT_EQ(ref.inos(1), dst);  // only dst appended on the existing-ref path
}

// --- validation errors (all returned before any mutation) -------------------

TEST_F(CopyFileRangeRunTest, SrcInodeNotFoundReturnsError) {
  const Ino src = 320;
  const Ino dst = 321;
  SeedInode(MakeFileInode(dst, 0));  // src intentionally missing

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 1024;

  auto status = RunCopy(param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
}

TEST_F(CopyFileRangeRunTest, DstInodeNotFoundReturnsError) {
  const Ino src = 322;
  const Ino dst = 323;
  SeedInode(MakeFileInode(src, 2048));  // dst intentionally missing
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3600, 0, 2048, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 1024;

  auto status = RunCopy(param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FOUND);
}

TEST_F(CopyFileRangeRunTest, SrcNotRegularFileReturnsError) {
  const Ino src = 324;
  const Ino dst = 325;
  SeedInode(MakeDirInode(src));  // src is a directory
  SeedInode(MakeFileInode(dst, 0));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 1024;

  auto status = RunCopy(param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FILE);
}

TEST_F(CopyFileRangeRunTest, DstNotRegularFileReturnsError) {
  const Ino src = 326;
  const Ino dst = 327;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeDirInode(dst));  // dst is a directory
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3700, 0, 2048, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 0;
  param.dst_off = 0;
  param.len = 1024;

  auto status = RunCopy(param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_FILE);
}

TEST_F(CopyFileRangeRunTest, SrcOffsetBeyondEofReturnsError) {
  const Ino src = 328;
  const Ino dst = 329;
  SeedInode(MakeFileInode(src, 2048));
  SeedInode(MakeFileInode(dst, 0));
  SeedChunkFor(src, MakeChunk(0, {MakeSlice(/*id=*/3800, 0, 2048, 0)}));

  CopyFileRangeOperation::Param param{};
  param.src_ino = src;
  param.dst_ino = dst;
  param.src_off = 2048;  // == length, i.e. at/after EOF
  param.dst_off = 0;
  param.len = 1024;

  auto status = RunCopy(param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EOUT_OF_RANGE);
}

// ---------------------------------------------------------------------------
// FallocateOperation::RunInBatch against DummyStorage. Fallocate runs through
// the batch path: the framework loads the inode into shared_param.attr and the
// operation mutates that attr in place while reading/writing chunks via the
// txn. We drive RunInBatch directly with a hand-built shared_param.
//
// Four mode families are covered:
//   - mode == 0                 -> PreAlloc (extend size, append zero slices)
//   - FALLOC_FL_PUNCH_HOLE      -> SetZero, keep_size = true
//   - FALLOC_FL_ZERO_RANGE      -> SetZero, keep_size = (mode & KEEP_SIZE)
//   - FALLOC_FL_COLLAPSE_RANGE  -> ENOT_SUPPORT
// ---------------------------------------------------------------------------

namespace {

constexpr uint32_t kFallocFsId = 1;
constexpr uint64_t kFallocChunkSize = 8192;
constexpr uint64_t kFallocBlockSize = 4096;

// FALLOC_FL_* come from <fcntl.h>: KEEP_SIZE=1, PUNCH_HOLE=2, COLLAPSE=8,
// ZERO_RANGE=16.

AttrEntry MakeFallocInode(Ino ino, uint64_t length) {
  AttrEntry attr;
  attr.set_fs_id(kFallocFsId);
  attr.set_ino(ino);
  attr.set_type(pb::mds::FileType::FILE);
  attr.set_length(length);
  attr.set_version(1);
  return attr;
}

ChunkEntry MakeSizedChunk(uint32_t index, std::vector<SliceEntry> slices) {
  ChunkEntry chunk = MakeChunk(index, std::move(slices));
  chunk.set_chunk_size(kFallocChunkSize);
  chunk.set_block_size(kFallocBlockSize);
  return chunk;
}

}  // namespace

class FallocateRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = DummyStorage::New();
    ASSERT_TRUE(storage_->Init(""));
  }

  void SeedChunk(Ino ino, const ChunkEntry& chunk) {
    ASSERT_TRUE(
        storage_
            ->Put(KVStorage::WriteOption(),
                  MetaCodec::EncodeChunkKey(kFallocFsId, ino, chunk.index()),
                  MetaCodec::EncodeChunkValue(chunk))
            .ok());
  }

  bool ChunkExists(Ino ino, uint64_t index) {
    std::string value;
    return storage_
        ->Get(MetaCodec::EncodeChunkKey(kFallocFsId, ino, index), value)
        .ok();
  }

  ChunkEntry GetChunk(Ino ino, uint64_t index) {
    std::string value;
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeChunkKey(kFallocFsId, ino, index), value)
            .ok());
    return MetaCodec::DecodeChunkValue(value);
  }

  std::vector<SliceEntry> SortedSlices(const ChunkEntry& chunk) {
    std::vector<SliceEntry> slices(chunk.slices().begin(),
                                   chunk.slices().end());
    SortByPos(slices);
    return slices;
  }

  FallocateOperation::Param MakeParam(Ino ino, int32_t mode, uint64_t offset,
                                      uint64_t len, uint32_t slice_num) {
    FallocateOperation::Param param{};
    param.fs_id = kFallocFsId;
    param.ino = ino;
    param.mode = mode;
    param.offset = offset;
    param.len = len;
    param.slice_num = slice_num;
    param.chunk_size = kFallocChunkSize;
    param.block_size = kFallocBlockSize;
    return param;
  }

  Trace trace_;
  KVStorageSPtr storage_;
};

// mode == 0 on an empty (0-byte) file: a brand new chunk is created holding a
// single zero slice (id=0) covering the requested range, and the file grows.
TEST_F(FallocateRunTest, PreAllocExtendsEmptyFile) {
  const Ino ino = 100;
  AttrEntry attr = MakeFallocInode(ino, 0);

  // slice_num is computed by the caller as ((new_len - len)/chunk)+1.
  FallocateOperation op(trace_, MakeParam(ino, /*mode=*/0, /*offset=*/0,
                                          /*len=*/4096, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  // File length grew and the in-place attr reflects it. SetResultAttr copies
  // the mutated attr into the result for the caller.
  EXPECT_EQ(shared_param.attr.length(), 4096u);
  op.SetResultAttr(shared_param);
  EXPECT_EQ(op.GetResult().attr.length(), 4096u);
  EXPECT_EQ(op.GetResult().delta_bytes, 4096);
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 1u);

  ChunkEntry chunk = GetChunk(ino, 0);
  EXPECT_EQ(chunk.chunk_size(), kFallocChunkSize);
  EXPECT_EQ(chunk.block_size(), kFallocBlockSize);
  ASSERT_EQ(chunk.slices_size(), 1);
  const auto& s = chunk.slices(0);
  EXPECT_EQ(s.id(), 0u);
  EXPECT_EQ(s.pos(), 0u);
  EXPECT_EQ(s.off(), 0u);
  EXPECT_EQ(s.len(), 4096u);
  EXPECT_EQ(s.size(), 0u);
}

// mode == 0 where offset+len <= current length is a no-op: no chunk is written
// and the length is unchanged.
TEST_F(FallocateRunTest, PreAllocNoOpWhenWithinLength) {
  const Ino ino = 101;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(trace_, MakeParam(ino, /*mode=*/0, /*offset=*/0,
                                          /*len=*/4096, /*slice_num=*/0));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), kFallocChunkSize);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  EXPECT_TRUE(op.GetResult().effected_chunks.empty());

  // The seeded chunk is untouched (still a single real slice).
  ChunkEntry chunk = GetChunk(ino, 0);
  ASSERT_EQ(chunk.slices_size(), 1);
  EXPECT_EQ(chunk.slices(0).id(), 1000u);
}

// mode == 0 extending within an existing chunk appends the zero slice to that
// chunk and bumps its version (rather than recreating it).
TEST_F(FallocateRunTest, PreAllocAppendsToExistingChunk) {
  const Ino ino = 102;
  AttrEntry attr = MakeFallocInode(ino, 4096);
  SeedChunk(ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                              /*len=*/4096, /*pos=*/0)}));

  FallocateOperation op(trace_, MakeParam(ino, /*mode=*/0, /*offset=*/4096,
                                          /*len=*/2048, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), 6144u);
  EXPECT_EQ(op.GetResult().delta_bytes, 2048);

  ChunkEntry chunk = GetChunk(ino, 0);
  EXPECT_EQ(chunk.version(), 1u);  // bumped so ChunkCache::PutIf accepts it
  auto slices = SortedSlices(chunk);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 1000u);
  EXPECT_EQ(slices[0].pos(), 0u);
  EXPECT_EQ(slices[1].id(), 0u);  // appended zero slice
  EXPECT_EQ(slices[1].pos(), 4096u);
  EXPECT_EQ(slices[1].off(), 0u);
  EXPECT_EQ(slices[1].len(), 2048u);
  EXPECT_EQ(slices[1].size(), 0u);
}

// mode == 0 extending a multi-chunk range from an empty file creates one new
// chunk per touched chunk, each with a full-chunk zero slice.
TEST_F(FallocateRunTest, PreAllocAcrossMultipleNewChunks) {
  const Ino ino = 103;
  AttrEntry attr = MakeFallocInode(ino, 0);

  const uint64_t len = 2 * kFallocChunkSize;
  FallocateOperation op(trace_, MakeParam(ino, /*mode=*/0, /*offset=*/0, len,
                                          /*slice_num=*/3));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), len);
  EXPECT_EQ(op.GetResult().delta_bytes, static_cast<int64_t>(len));
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 2u);

  for (uint64_t index : {0u, 1u}) {
    ChunkEntry chunk = GetChunk(ino, index);
    ASSERT_EQ(chunk.slices_size(), 1) << "chunk " << index;
    EXPECT_EQ(chunk.slices(0).id(), 0u);
    EXPECT_EQ(chunk.slices(0).pos(), 0u);
    EXPECT_EQ(chunk.slices(0).len(), kFallocChunkSize);
  }
}

// mode == 0 that both appends to the existing max chunk AND spills into a new
// chunk: exercises the max_chunk_exists -> false transition mid-loop.
TEST_F(FallocateRunTest, PreAllocAppendThenNewChunk) {
  const Ino ino = 104;
  AttrEntry attr = MakeFallocInode(ino, 4096);
  SeedChunk(ino, MakeSizedChunk(0, {MakeSlice(/*id=*/4000, /*off=*/0,
                                              /*len=*/4096, /*pos=*/0)}));

  // Extend [4096, 12288): fills the tail of chunk 0 then starts chunk 1.
  FallocateOperation op(trace_,
                        MakeParam(ino, /*mode=*/0, /*offset=*/4096,
                                  /*len=*/kFallocChunkSize, /*slice_num=*/2));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), 12288u);
  EXPECT_EQ(op.GetResult().delta_bytes, static_cast<int64_t>(kFallocChunkSize));
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 2u);

  // Chunk 0 keeps its real slice and gains a zero slice on the tail.
  ChunkEntry chunk0 = GetChunk(ino, 0);
  EXPECT_EQ(chunk0.version(), 1u);
  auto slices0 = SortedSlices(chunk0);
  ASSERT_EQ(slices0.size(), 2u);
  EXPECT_EQ(slices0[0].id(), 4000u);
  EXPECT_EQ(slices0[1].id(), 0u);
  EXPECT_EQ(slices0[1].pos(), 4096u);
  EXPECT_EQ(slices0[1].len(), 4096u);

  // Chunk 1 is freshly created holding the remaining zero slice.
  ChunkEntry chunk1 = GetChunk(ino, 1);
  EXPECT_EQ(chunk1.version(), 1u);
  EXPECT_EQ(chunk1.chunk_size(), kFallocChunkSize);
  ASSERT_EQ(chunk1.slices_size(), 1);
  EXPECT_EQ(chunk1.slices(0).id(), 0u);
  EXPECT_EQ(chunk1.slices(0).pos(), 0u);
  EXPECT_EQ(chunk1.slices(0).len(), 4096u);
}

// mode == 0 where the range needs more zero slices than slice_num allows fails
// with EINTERNAL ("beyond slice num").
TEST_F(FallocateRunTest, PreAllocBeyondSliceNumReturnsError) {
  const Ino ino = 105;
  AttrEntry attr = MakeFallocInode(ino, 0);

  // Range spans 2 chunks but only 1 slice is permitted.
  FallocateOperation op(trace_, MakeParam(ino, /*mode=*/0, /*offset=*/0,
                                          /*len=*/2 * kFallocChunkSize,
                                          /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  auto status = op.RunInBatch(txn, shared_param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::EINTERNAL);
}

// FALLOC_FL_PUNCH_HOLE keeps the file size and shadows the requested byte range
// with a zero slice.
TEST_F(FallocateRunTest, PunchHoleKeepsSizeAndZerosToChunkEnd) {
  const Ino ino = 106;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(trace_, MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                                          /*offset=*/2048, /*len=*/2048,
                                          /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  // PUNCH_HOLE never changes the file size.
  EXPECT_EQ(shared_param.attr.length(), kFallocChunkSize);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 1u);

  ChunkEntry chunk = GetChunk(ino, 0);
  EXPECT_EQ(chunk.version(), 1u);
  auto slices = SortedSlices(chunk);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 1000u);
  EXPECT_EQ(slices[1].id(), 0u);
  EXPECT_EQ(slices[1].pos(), 2048u);
  EXPECT_EQ(slices[1].off(), 0u);
  EXPECT_EQ(slices[1].len(), 2048u);
}

// FALLOC_FL_PUNCH_HOLE clamps end_offset to the current file length, so a range
// running past EOF only touches chunks that actually back the file.
TEST_F(FallocateRunTest, PunchHoleClampedToFileLength) {
  const Ino ino = 107;
  AttrEntry attr = MakeFallocInode(ino, 4096);
  SeedChunk(ino, MakeSizedChunk(0, {MakeSlice(/*id=*/2000, /*off=*/0,
                                              /*len=*/4096, /*pos=*/0)}));

  // len reaches well past EOF and into later chunks; the clamp keeps it inside
  // chunk 0 only.
  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                                  /*offset=*/2048, /*len=*/4 * kFallocChunkSize,
                                  /*slice_num=*/5));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), 4096u);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 1u);
  EXPECT_FALSE(ChunkExists(ino, 1));  // clamp prevented touching later chunks

  ChunkEntry chunk = GetChunk(ino, 0);
  ASSERT_EQ(chunk.slices_size(), 2);
}

// FALLOC_FL_PUNCH_HOLE across a chunk boundary (the realistic PUNCH_HOLE |
// KEEP_SIZE combo) writes a zero slice into every touched chunk while leaving
// the file size unchanged.
TEST_F(FallocateRunTest, PunchHoleSpansMultipleChunks) {
  const Ino ino = 111;
  const uint64_t length = 2 * kFallocChunkSize;
  AttrEntry attr = MakeFallocInode(ino, length);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/5000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));
  SeedChunk(
      ino, MakeSizedChunk(1, {MakeSlice(/*id=*/5001, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  // Punch [4096, 12288): tail of chunk 0 and into chunk 1.
  FallocateOperation op(
      trace_, MakeParam(ino, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                        /*offset=*/4096, /*len=*/kFallocChunkSize,
                        /*slice_num=*/2));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), length);  // size preserved
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 2u);

  // Chunk 0: real slice + zero slice on its tail.
  auto slices0 = SortedSlices(GetChunk(ino, 0));
  ASSERT_EQ(slices0.size(), 2u);
  EXPECT_EQ(slices0[0].id(), 5000u);
  EXPECT_EQ(slices0[1].id(), 0u);
  EXPECT_EQ(slices0[1].pos(), 4096u);
  EXPECT_EQ(slices0[1].len(), kFallocChunkSize - 4096);

  // Chunk 1: real slice + a zero slice covering the requested prefix.
  auto slices1 = SortedSlices(GetChunk(ino, 1));
  ASSERT_EQ(slices1.size(), 2u);
  EXPECT_EQ(slices1[0].id(), 5001u);
  EXPECT_EQ(slices1[1].id(), 0u);
  EXPECT_EQ(slices1[1].pos(), 0u);
  EXPECT_EQ(slices1[1].len(), 4096u);
}

// FALLOC_FL_ZERO_RANGE with FALLOC_FL_KEEP_SIZE zeros the range but leaves the
// file size unchanged.
TEST_F(FallocateRunTest, ZeroRangeWithKeepSizeKeepsLength) {
  const Ino ino = 108;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/3000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(
      trace_, MakeParam(ino, FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                        /*offset=*/0, /*len=*/4096, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), kFallocChunkSize);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);

  ChunkEntry chunk = GetChunk(ino, 0);
  auto slices = SortedSlices(chunk);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[1].id(), 0u);
  EXPECT_EQ(slices[1].pos(), 0u);
  EXPECT_EQ(slices[1].len(), 4096u);
}

// FALLOC_FL_ZERO_RANGE without FALLOC_FL_KEEP_SIZE extends the file when the
// range runs past the current length.
TEST_F(FallocateRunTest, ZeroRangeWithoutKeepSizeExtendsFile) {
  const Ino ino = 109;
  AttrEntry attr = MakeFallocInode(ino, 4096);
  SeedChunk(ino, MakeSizedChunk(0, {MakeSlice(/*id=*/3000, /*off=*/0,
                                              /*len=*/4096, /*pos=*/0)}));

  // Zero [4096, 12288): tail of chunk 0 then the prefix of chunk 1.
  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_ZERO_RANGE,
                                  /*offset=*/4096, /*len=*/kFallocChunkSize,
                                  /*slice_num=*/2));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), 12288u);
  EXPECT_EQ(op.GetResult().delta_bytes, static_cast<int64_t>(kFallocChunkSize));
  ASSERT_EQ(op.GetResult().effected_chunks.size(), 2u);

  ChunkEntry chunk0 = GetChunk(ino, 0);
  auto slices0 = SortedSlices(chunk0);
  ASSERT_EQ(slices0.size(), 2u);
  EXPECT_EQ(slices0[0].id(), 3000u);
  EXPECT_EQ(slices0[1].id(), 0u);
  EXPECT_EQ(slices0[1].pos(), 4096u);
  EXPECT_EQ(slices0[1].len(), 4096u);

  ChunkEntry chunk1 = GetChunk(ino, 1);
  ASSERT_EQ(chunk1.slices_size(), 1);
  EXPECT_EQ(chunk1.slices(0).id(), 0u);
  EXPECT_EQ(chunk1.slices(0).pos(), 0u);
  EXPECT_EQ(chunk1.slices(0).len(), 4096u);
}

// FALLOC_FL_ZERO_RANGE without FALLOC_FL_KEEP_SIZE but with the range fully
// inside the file: the range is zeroed yet the size stays put (the !keep_size
// extend branch is skipped because end_offset <= length).
TEST_F(FallocateRunTest, ZeroRangeWithoutKeepSizeWithinFileKeepsLength) {
  const Ino ino = 112;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/3000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_ZERO_RANGE,
                                  /*offset=*/0, /*len=*/4096, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), kFallocChunkSize);  // unchanged
  EXPECT_EQ(op.GetResult().delta_bytes, 0);

  auto slices = SortedSlices(GetChunk(ino, 0));
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[1].id(), 0u);
  EXPECT_EQ(slices[1].pos(), 0u);
}

// A non-zero mode that matches none of the supported flags (e.g. a bare
// FALLOC_FL_KEEP_SIZE) falls through every branch and is a silent no-op that
// returns OK without touching the file.
TEST_F(FallocateRunTest, UnrecognizedModeIsNoOp) {
  const Ino ino = 113;
  AttrEntry attr = MakeFallocInode(ino, 4096);

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_KEEP_SIZE,
                                  /*offset=*/0, /*len=*/1024, /*slice_num=*/0));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), 4096u);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  EXPECT_TRUE(op.GetResult().effected_chunks.empty());
  EXPECT_FALSE(ChunkExists(ino, 0));
}

// --- AppendZeroSlices edge cases (reached via SetZero) ----------------------

// A punch hole whose range starts at EOF produces an empty [offset,end_offset)
// for AppendZeroSlices (end_offset clamps to length), so the loop never runs:
// nothing is written and the chunk is left untouched.
TEST_F(FallocateRunTest, PunchHoleAtEofIsNoOp) {
  const Ino ino = 114;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                                  /*offset=*/kFallocChunkSize, /*len=*/1024,
                                  /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(shared_param.attr.length(), kFallocChunkSize);
  EXPECT_EQ(op.GetResult().delta_bytes, 0);
  EXPECT_TRUE(op.GetResult().effected_chunks.empty());

  // Chunk is byte-for-byte unchanged (no zero slice, version not bumped).
  ChunkEntry chunk = GetChunk(ino, 0);
  EXPECT_EQ(chunk.version(), 0u);
  ASSERT_EQ(chunk.slices_size(), 1);
  EXPECT_EQ(chunk.slices(0).id(), 1000u);
}

// A zero-length punch hole also yields an empty range -> AppendZeroSlices is a
// no-op.
TEST_F(FallocateRunTest, PunchHoleZeroLengthIsNoOp) {
  const Ino ino = 115;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                                  /*offset=*/2048, /*len=*/0, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_TRUE(op.GetResult().effected_chunks.empty());
  ChunkEntry chunk = GetChunk(ino, 0);
  EXPECT_EQ(chunk.version(), 0u);
  ASSERT_EQ(chunk.slices_size(), 1);
}

// AppendZeroSlices clears effected_chunks at entry, so re-running the same op
// object (as a txn retry does) reflects only the latest run instead of
// accumulating chunks across attempts.
TEST_F(FallocateRunTest, PunchHoleReRunDoesNotAccumulateEffectedChunks) {
  const Ino ino = 116;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));

  FallocateOperation op(
      trace_, MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                        /*offset=*/2048, /*len=*/2048, /*slice_num=*/1));

  // First attempt: run but do NOT commit (mimics a conflicting txn that
  // retries).
  {
    Operation::BatchSharedParam shared_param;
    shared_param.attr = attr;
    auto txn = storage_->NewTxn();
    ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
    EXPECT_EQ(op.GetResult().effected_chunks.size(), 1u);
  }
  // Retry on the same op with a fresh txn + freshly reloaded attr. Storage is
  // unchanged, so the result must again be exactly one chunk, not two.
  {
    Operation::BatchSharedParam shared_param;
    shared_param.attr = attr;
    auto txn = storage_->NewTxn();
    ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
    ASSERT_TRUE(txn->Commit().ok());
    EXPECT_EQ(op.GetResult().effected_chunks.size(), 1u);
  }
}

// The version bump on an existing chunk increments the stored value rather than
// resetting it to a constant.
TEST_F(FallocateRunTest, PunchHoleBumpsExistingChunkVersionFromNonZero) {
  const Ino ino = 117;
  AttrEntry attr = MakeFallocInode(ino, kFallocChunkSize);
  ChunkEntry seeded =
      MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                   /*len=*/kFallocChunkSize, /*pos=*/0)});
  seeded.set_version(5);
  SeedChunk(ino, seeded);

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_PUNCH_HOLE,
                                  /*offset=*/0, /*len=*/kFallocChunkSize,
                                  /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  EXPECT_EQ(GetChunk(ino, 0).version(), 6u);
}

// FALLOC_FL_COLLAPSE_RANGE is rejected with ENOT_SUPPORT.
TEST_F(FallocateRunTest, CollapseRangeNotSupported) {
  const Ino ino = 110;
  AttrEntry attr = MakeFallocInode(ino, 4096);

  FallocateOperation op(trace_,
                        MakeParam(ino, FALLOC_FL_COLLAPSE_RANGE,
                                  /*offset=*/0, /*len=*/1024, /*slice_num=*/1));
  Operation::BatchSharedParam shared_param;
  shared_param.attr = attr;

  auto txn = storage_->NewTxn();
  auto status = op.RunInBatch(txn, shared_param);
  ASSERT_FALSE(status.ok());
  EXPECT_EQ(status.error_code(), pb::error::ENOT_SUPPORT);
}

// ---------------------------------------------------------------------------
// UpdateAttrOperation::RunInBatch against DummyStorage. RunInBatch applies the
// fields selected by the `to_set` bitmask from attr_ onto shared_param.attr;
// kSetAttrSize additionally computes delta_bytes and, on shrink, appends zero
// slices over the dropped range.
//
// The strategy below uses a baseline inode whose every field differs from the
// "new values" inode, so each single-flag test proves both that the targeted
// field IS copied and that no other field is touched.
// ---------------------------------------------------------------------------

namespace {

// Baseline "current" inode; every mutable field has a known sentinel value.
AttrEntry MakeFullInode(Ino ino, uint64_t length) {
  AttrEntry attr;
  attr.set_fs_id(kFallocFsId);
  attr.set_ino(ino);
  attr.set_type(pb::mds::FileType::FILE);
  attr.set_version(1);
  attr.set_length(length);
  attr.set_mode(0644);
  attr.set_uid(1000);
  attr.set_gid(1000);
  attr.set_atime(111);
  attr.set_mtime(222);
  attr.set_ctime(333);
  attr.set_nlink(1);
  attr.set_flags(0);
  return attr;
}

// "New values" inode; every field differs from MakeFullInode so an accidental
// copy of an unrequested field is detectable.
AttrEntry MakeNewValues() {
  AttrEntry attr;
  attr.set_mode(0755);
  attr.set_uid(2000);
  attr.set_gid(3000);
  attr.set_length(999);
  attr.set_atime(444);
  attr.set_mtime(555);
  attr.set_ctime(666);
  attr.set_nlink(7);
  attr.set_flags(9);
  return attr;
}

void ExpectAttrEq(const AttrEntry& got, const AttrEntry& want) {
  EXPECT_EQ(got.mode(), want.mode());
  EXPECT_EQ(got.uid(), want.uid());
  EXPECT_EQ(got.gid(), want.gid());
  EXPECT_EQ(got.length(), want.length());
  EXPECT_EQ(got.atime(), want.atime());
  EXPECT_EQ(got.mtime(), want.mtime());
  EXPECT_EQ(got.ctime(), want.ctime());
  EXPECT_EQ(got.nlink(), want.nlink());
  EXPECT_EQ(got.flags(), want.flags());
  EXPECT_EQ(got.version(), want.version());  // op never bumps version itself
}

}  // namespace

class UpdateAttrRunTest : public ::testing::Test {
 protected:
  void SetUp() override {
    storage_ = DummyStorage::New();
    ASSERT_TRUE(storage_->Init(""));
  }

  void SeedChunk(Ino ino, const ChunkEntry& chunk) {
    ASSERT_TRUE(
        storage_
            ->Put(KVStorage::WriteOption(),
                  MetaCodec::EncodeChunkKey(kFallocFsId, ino, chunk.index()),
                  MetaCodec::EncodeChunkValue(chunk))
            .ok());
  }

  bool ChunkExists(Ino ino, uint64_t index) {
    std::string value;
    return storage_
        ->Get(MetaCodec::EncodeChunkKey(kFallocFsId, ino, index), value)
        .ok();
  }

  std::vector<SliceEntry> SortedSlices(Ino ino, uint64_t index) {
    std::string value;
    EXPECT_TRUE(
        storage_->Get(MetaCodec::EncodeChunkKey(kFallocFsId, ino, index), value)
            .ok());
    ChunkEntry chunk = MetaCodec::DecodeChunkValue(value);
    std::vector<SliceEntry> slices(chunk.slices().begin(),
                                   chunk.slices().end());
    SortByPos(slices);
    return slices;
  }

  // Runs UpdateAttrOperation over `current` and commits. Returns the mutated
  // attr; optional out-params expose the result's delta_bytes / effected count.
  AttrEntry RunUpdate(Ino ino, uint32_t to_set, const AttrEntry& new_vals,
                      const AttrEntry& current, int64_t* delta_bytes = nullptr,
                      size_t* effected = nullptr) {
    UpdateAttrOperation::ExtraParam extra;
    extra.chunk_size = kFallocChunkSize;
    extra.block_size = kFallocBlockSize;
    UpdateAttrOperation op(trace_, ino, to_set, new_vals, extra);

    Operation::BatchSharedParam shared_param;
    shared_param.attr = current;

    auto txn = storage_->NewTxn();
    EXPECT_TRUE(op.RunInBatch(txn, shared_param).ok());
    EXPECT_TRUE(txn->Commit().ok());

    if (delta_bytes != nullptr) *delta_bytes = op.GetResult().delta_bytes;
    if (effected != nullptr) *effected = op.GetResult().effected_chunks.size();
    return shared_param.attr;
  }

  Trace trace_;
  KVStorageSPtr storage_;
};

// --- single-field updates: target changes, everything else is preserved -----

// Note: setting mode does NOT auto-bump ctime here (unlike UpdateXAttrOperation
// etc.) — ctime only changes when kSetAttrCtime is explicitly requested.
TEST_F(UpdateAttrRunTest, UpdateModeOnly) {
  const Ino ino = 200;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrMode, new_vals, baseline);

  auto expected = baseline;
  expected.set_mode(new_vals.mode());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateUidOnly) {
  const Ino ino = 201;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrUid, new_vals, baseline);

  auto expected = baseline;
  expected.set_uid(new_vals.uid());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateGidOnly) {
  const Ino ino = 202;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrGid, new_vals, baseline);

  auto expected = baseline;
  expected.set_gid(new_vals.gid());
  ExpectAttrEq(got, expected);
}

// atime update must leave mtime and ctime untouched.
TEST_F(UpdateAttrRunTest, UpdateAtimeOnly) {
  const Ino ino = 203;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrAtime, new_vals, baseline);

  auto expected = baseline;
  expected.set_atime(new_vals.atime());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateMtimeOnly) {
  const Ino ino = 204;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrMtime, new_vals, baseline);

  auto expected = baseline;
  expected.set_mtime(new_vals.mtime());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateCtimeOnly) {
  const Ino ino = 205;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrCtime, new_vals, baseline);

  auto expected = baseline;
  expected.set_ctime(new_vals.ctime());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateNlinkOnly) {
  const Ino ino = 206;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrNlink, new_vals, baseline);

  auto expected = baseline;
  expected.set_nlink(new_vals.nlink());
  ExpectAttrEq(got, expected);
}

TEST_F(UpdateAttrRunTest, UpdateFlagsOnly) {
  const Ino ino = 207;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got = RunUpdate(ino, kSetAttrFlags, new_vals, baseline);

  auto expected = baseline;
  expected.set_flags(new_vals.flags());
  ExpectAttrEq(got, expected);
}

// --- masks: combined, empty, and unsupported -------------------------------

// All metadata flags at once (no size): every selected field is applied
// independently; length stays put because kSetAttrSize was not requested.
TEST_F(UpdateAttrRunTest, UpdateMetadataFieldsTogether) {
  const Ino ino = 208;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();
  const uint32_t to_set = kSetAttrMode | kSetAttrUid | kSetAttrGid |
                          kSetAttrNlink | kSetAttrFlags | kSetAttrAtime |
                          kSetAttrMtime | kSetAttrCtime;

  int64_t delta = -1;
  auto got = RunUpdate(ino, to_set, new_vals, baseline, &delta);

  auto expected = baseline;
  expected.set_mode(new_vals.mode());
  expected.set_uid(new_vals.uid());
  expected.set_gid(new_vals.gid());
  expected.set_nlink(new_vals.nlink());
  expected.set_flags(new_vals.flags());
  expected.set_atime(new_vals.atime());
  expected.set_mtime(new_vals.mtime());
  expected.set_ctime(new_vals.ctime());
  ExpectAttrEq(got, expected);
  EXPECT_EQ(delta,
            0);  // no size flag -> no delta even though new length differs
}

TEST_F(UpdateAttrRunTest, NoFlagsLeavesAttrUnchanged) {
  const Ino ino = 209;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  int64_t delta = -1;
  size_t effected = 99;
  auto got =
      RunUpdate(ino, /*to_set=*/0, new_vals, baseline, &delta, &effected);

  ExpectAttrEq(got, baseline);
  EXPECT_EQ(delta, 0);
  EXPECT_EQ(effected, 0u);
}

// The *_NOW flags are resolved on the client (mds_client.cc translates them to
// explicit kSetAttrAtime/kSetAttrMtime). RunInBatch has no branch for them, so
// passing them straight to the op is a silent no-op.
TEST_F(UpdateAttrRunTest, AtimeNowAndMtimeNowFlagsAreIgnored) {
  const Ino ino = 210;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  auto got =
      RunUpdate(ino, kSetAttrAtimeNow | kSetAttrMtimeNow, new_vals, baseline);

  ExpectAttrEq(got, baseline);  // atime/mtime unchanged
}

// --- size: truncate up / down / same ---------------------------------------

// Truncate up sets the new length, reports a positive delta, and appends NO
// zero slices (the extended region is sparse). It also leaves the timestamps
// untouched (size-only setattr does not bump mtime/ctime in this op).
TEST_F(UpdateAttrRunTest, SizeTruncateUpSetsLengthWithoutZeroSlices) {
  const Ino ino = 211;
  auto baseline = MakeFullInode(ino, 4096);
  auto new_vals = MakeNewValues();
  new_vals.set_length(8192);

  int64_t delta = 0;
  size_t effected = 99;
  auto got =
      RunUpdate(ino, kSetAttrSize, new_vals, baseline, &delta, &effected);

  EXPECT_EQ(delta, 4096);
  EXPECT_EQ(effected, 0u);
  EXPECT_FALSE(ChunkExists(ino, 0));

  auto expected = baseline;
  expected.set_length(8192);
  ExpectAttrEq(got, expected);
}

// Truncate down sets the new length, reports a negative delta, and appends a
// zero slice over the dropped tail of the chunk.
TEST_F(UpdateAttrRunTest, SizeTruncateDownAppendsZeroSlices) {
  const Ino ino = 212;
  auto baseline = MakeFullInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, /*off=*/0,
                                        /*len=*/kFallocChunkSize, /*pos=*/0)}));
  auto new_vals = MakeNewValues();
  new_vals.set_length(4096);

  int64_t delta = 0;
  size_t effected = 0;
  auto got =
      RunUpdate(ino, kSetAttrSize, new_vals, baseline, &delta, &effected);

  EXPECT_EQ(delta, -static_cast<int64_t>(kFallocChunkSize - 4096));
  ASSERT_EQ(effected, 1u);

  auto slices = SortedSlices(ino, 0);
  ASSERT_EQ(slices.size(), 2u);
  EXPECT_EQ(slices[0].id(), 1000u);
  EXPECT_EQ(slices[1].id(), 0u);  // zero slice neutralizes the dropped range
  EXPECT_EQ(slices[1].pos(), 4096u);
  EXPECT_EQ(slices[1].len(), kFallocChunkSize - 4096);

  // Only length changed; timestamps are left as-is.
  auto expected = baseline;
  expected.set_length(4096);
  ExpectAttrEq(got, expected);
}

// Truncate to zero across multiple chunks zeros every touched chunk.
TEST_F(UpdateAttrRunTest, SizeTruncateToZeroSpansChunks) {
  const Ino ino = 213;
  const uint64_t length = 2 * kFallocChunkSize;
  auto baseline = MakeFullInode(ino, length);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/2000, 0, kFallocChunkSize, 0)}));
  SeedChunk(
      ino, MakeSizedChunk(1, {MakeSlice(/*id=*/2001, 0, kFallocChunkSize, 0)}));
  auto new_vals = MakeNewValues();
  new_vals.set_length(0);

  int64_t delta = 0;
  size_t effected = 0;
  auto got =
      RunUpdate(ino, kSetAttrSize, new_vals, baseline, &delta, &effected);

  EXPECT_EQ(got.length(), 0u);
  EXPECT_EQ(delta, -static_cast<int64_t>(length));
  ASSERT_EQ(effected, 2u);
  EXPECT_EQ(SortedSlices(ino, 0).size(), 2u);  // real + zero
  EXPECT_EQ(SortedSlices(ino, 1).size(), 2u);
}

// Setting size to the current length is a no-op for data: delta 0, no zero
// slices, no chunk created.
TEST_F(UpdateAttrRunTest, SizeSameLengthNoZeroSlices) {
  const Ino ino = 214;
  auto baseline = MakeFullInode(ino, 4096);
  auto new_vals = MakeNewValues();
  new_vals.set_length(4096);

  int64_t delta = -1;
  size_t effected = 99;
  auto got =
      RunUpdate(ino, kSetAttrSize, new_vals, baseline, &delta, &effected);

  EXPECT_EQ(got.length(), 4096u);
  EXPECT_EQ(delta, 0);
  EXPECT_EQ(effected, 0u);
  EXPECT_FALSE(ChunkExists(ino, 0));
}

// Realistic truncate: size + mtime + ctime together. Length, mtime and ctime
// all change; atime and the rest stay put.
TEST_F(UpdateAttrRunTest, TruncateUpdatesSizeAndTimestamps) {
  const Ino ino = 215;
  auto baseline = MakeFullInode(ino, kFallocChunkSize);
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/1000, 0, kFallocChunkSize, 0)}));
  auto new_vals = MakeNewValues();
  new_vals.set_length(2048);

  int64_t delta = 0;
  auto got = RunUpdate(ino, kSetAttrSize | kSetAttrMtime | kSetAttrCtime,
                       new_vals, baseline, &delta);

  EXPECT_EQ(delta, -static_cast<int64_t>(kFallocChunkSize - 2048));

  auto expected = baseline;
  expected.set_length(2048);
  expected.set_mtime(new_vals.mtime());
  expected.set_ctime(new_vals.ctime());
  ExpectAttrEq(got, expected);
}

// Truncate down over a sparse file: AppendZeroSlices appends to the existing
// chunks (0 and 2) AND creates the missing middle chunk (1) in a single pass,
// exercising both branches within one range.
TEST_F(UpdateAttrRunTest, SizeTruncateDownSparseMiddleChunk) {
  const Ino ino = 217;
  const uint64_t length = 3 * kFallocChunkSize;
  auto baseline = MakeFullInode(ino, length);
  // chunk 1 intentionally absent (sparse hole).
  SeedChunk(
      ino, MakeSizedChunk(0, {MakeSlice(/*id=*/3000, 0, kFallocChunkSize, 0)}));
  SeedChunk(
      ino, MakeSizedChunk(2, {MakeSlice(/*id=*/3002, 0, kFallocChunkSize, 0)}));
  auto new_vals = MakeNewValues();
  new_vals.set_length(0);

  int64_t delta = 0;
  size_t effected = 0;
  auto got =
      RunUpdate(ino, kSetAttrSize, new_vals, baseline, &delta, &effected);

  EXPECT_EQ(got.length(), 0u);
  EXPECT_EQ(delta, -static_cast<int64_t>(length));
  ASSERT_EQ(effected, 3u);  // chunks 0, 1, 2 all touched

  EXPECT_EQ(SortedSlices(ino, 0).size(), 2u);  // real + zero (append branch)
  auto mid = SortedSlices(ino, 1);
  ASSERT_EQ(mid.size(), 1u);  // sparse chunk created fresh (create branch)
  EXPECT_EQ(mid[0].id(), 0u);
  EXPECT_EQ(mid[0].pos(), 0u);
  EXPECT_EQ(mid[0].len(), kFallocChunkSize);
  EXPECT_EQ(SortedSlices(ino, 2).size(), 2u);  // real + zero (append branch)
}

// SetResultAttr copies the mutated shared_param.attr into the result for the
// caller (the batch framework calls this after a successful run).
TEST_F(UpdateAttrRunTest, SetResultAttrCopiesMutatedAttr) {
  const Ino ino = 216;
  auto baseline = MakeFullInode(ino, 100);
  auto new_vals = MakeNewValues();

  UpdateAttrOperation::ExtraParam extra;
  extra.chunk_size = kFallocChunkSize;
  extra.block_size = kFallocBlockSize;
  UpdateAttrOperation op(trace_, ino, kSetAttrMode, new_vals, extra);

  Operation::BatchSharedParam shared_param;
  shared_param.attr = baseline;

  auto txn = storage_->NewTxn();
  ASSERT_TRUE(op.RunInBatch(txn, shared_param).ok());
  ASSERT_TRUE(txn->Commit().ok());

  op.SetResultAttr(shared_param);
  EXPECT_EQ(op.GetResult().attr.mode(), new_vals.mode());
  EXPECT_EQ(op.GetResult().attr.ino(), ino);
}

}  // namespace unit_test
}  // namespace mds
}  // namespace dingofs