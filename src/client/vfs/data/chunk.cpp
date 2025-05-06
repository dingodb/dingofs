/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License";
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

#include "client/vfs/data/chunk.h"

#include <glog/logging.h>

#include <cstdint>

#include "cache/blockcache/cache_store.h"
#include "common/status.h"
#include "client/common/utils.h"
#include "client/vfs/data/common.h"
#include "client/vfs/data/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

using namespace cache::blockcache;

Chunk::Chunk(VFSHub* hub, uint64_t ino, uint64_t index)
    : hub_(hub),
      ino_(ino),
      index_(index),
      fs_id_(hub->GetFsInfo().id),
      chunk_size_(hub->GetFsInfo().chunk_size),
      block_size_(hub->GetFsInfo().block_size),
      chunk_start_(index * chunk_size_),
      chunk_end_(chunk_start_ + chunk_size_) {}

Status Chunk::WriteToBlockCache(const BlockKey& key, const Block& block,
                                BlockContext ctx) {
  return hub_->GetBlockCache()->Put(key, block, ctx);
}

Status Chunk::AllockChunkId(uint64_t* chunk_id) {
  return hub_->GetMetaSystem()->NewSliceId(chunk_id);
}

Status Chunk::CommitSlices(const std::vector<Slice>& slices) {
  return hub_->GetMetaSystem()->WriteSlice(ino_, index_, slices);
}

Status Chunk::Write(const char* buf, uint64_t size, uint64_t chunk_offset) {
  VLOG(4) << "ChunkWrite ino: " << ino_ << ", index: " << index_
          << ", buf: " << Char2Addr(buf) << ", chunk_offset: " << chunk_offset
          << ", size: " << size
          << ", file_offst: " << (chunk_start_ + chunk_offset);
  CHECK_GE(chunk_end_, (chunk_start_ + chunk_offset + size));

  const char* buf_pos = buf;

  // TODO: refact this code
  uint64_t chunk_id;
  DINGOFS_RETURN_NOT_OK(AllockChunkId(&chunk_id));

  uint64_t block_offset = chunk_offset % block_size_;
  uint64_t block_index = chunk_offset / block_size_;

  uint64_t remain_len = size;

  while (remain_len > 0) {
    uint64_t write_size = std::min(remain_len, block_size_ - block_offset);
    BlockKey key(fs_id_, ino_, chunk_id, block_index, 0);
    Block block(buf_pos, write_size);
    BlockContext ctx(BlockFrom::kCtoFlush);

    VLOG(4) << "ChunkWrite ino: " << ino_ << ", index: " << index_
            << ", block_key: " << key.StoreKey()
            << ", buf: " << Char2Addr(buf_pos)
            << ", write_size: " << write_size;
    WriteToBlockCache(key, block, ctx);

    remain_len -= write_size;
    buf_pos += write_size;
    block_offset = 0;
    ++block_index;
  }

  Slice slice{chunk_id, (chunk_start_ + chunk_offset), size, 0, false, size};
  VLOG(4) << "ChunkWrite ino: " << ino_ << ", index: " << index_
          << ", slice: " << Slice2Str(slice);

  std::vector<Slice> slices;
  slices.push_back(slice);

  return CommitSlices(slices);
}

Status Chunk::Read(char* buf, uint64_t size, uint64_t chunk_offset) {
  VLOG(4) << "ChunkRead ino: " << ino_ << ", index: " << index_
          << ", offset: " << chunk_offset << ", buf: " << Char2Addr(buf)
          << ", size: " << size
          << ", file_offst: " << (chunk_start_ + chunk_offset);

  uint64_t read_file_offset = chunk_start_ + chunk_offset;
  CHECK_GE(chunk_end_, (read_file_offset + size));

  uint64_t block_offset = chunk_offset % block_size_;
  uint64_t block_index = chunk_offset / block_size_;

  uint64_t remain_len = size;

  std::vector<Slice> slices;
  DINGOFS_RETURN_NOT_OK(
      hub_->GetMetaSystem()->ReadSlice(ino_, index_, &slices));

  FileRange range{.offset = read_file_offset, .len = size};
  std::vector<SliceReadReq> slice_reqs = ProcessReadRequest(slices, range);

  std::vector<BlockReadReq> block_reqs;

  for (auto& slice_req : slice_reqs) {
    VLOG(6) << "ChunkRead slice_req: " << slice_req.ToString();

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, fs_id_, ino_, chunk_size_, block_size_);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    } else {
      char* buf_pos = buf + (slice_req.file_offset - read_file_offset);
      VLOG(4) << "ChunkRead ino: " << ino_ << ", index: " << index_
              << ", buf: " << Char2Addr(buf_pos)
              << ", zero fill, read_size: " << slice_req.len;
      memset(buf_pos, 0, slice_req.len);
    }
  }

  for (auto& block_req : block_reqs) {
    VLOG(6) << "ChunkRead block_req: " << block_req.ToString();
    BlockKey key(fs_id_, ino_, block_req.block.slice_id, block_req.block.index,
                 block_req.block.version);

    char* buf_pos = buf + (block_req.block.file_offset +
                           block_req.block_offset - read_file_offset);

    VLOG(4) << "ChunkRead ino: " << ino_ << ", chunk_index: " << index_
            << ", block_key: " << key.StoreKey()
            << ", block_offset: " << block_req.block_offset
            << ", read_size: " << block_req.len
            << ", buf: " << Char2Addr(buf_pos);

    bool retrive = true;
    DINGOFS_RETURN_NOT_OK(hub_->GetBlockCache()->Range(
        key, block_req.block_offset, block_req.len, buf_pos, retrive));
  }

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs