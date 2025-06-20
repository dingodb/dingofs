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

#include "client/vfs/data/slice/slice_data.h"

#include <butil/iobuf.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <mutex>
#include <vector>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string SliceData::ToString() const {
  return fmt::format(
      "{(ino:{}, chunk_indxe:{}, seq:{}) chunk_range: [{}-{}], len: {}, id: "
      "{}, flushed: {}, block_data_count: {}}",
      context_.ino, context_.chunk_index, context_.seq, chunk_offset_, End(),
      len_, id_, (flushed_.load(std::memory_order_relaxed) ? "true" : "false"),
      block_datas_.size());
}

BlockData* SliceData::FindOrCreateBlockDataUnlocked(uint64_t block_index,
                                                    uint64_t chunk_offset,
                                                    uint64_t len) {
  auto [iter, inserted] = block_datas_.try_emplace(
      block_index,
      std::make_unique<BlockData>(context_, vfs_hub_->GetPageAllocator(),
                                  block_index, chunk_offset, len));
  if (inserted) {
    VLOG(4) << fmt::format(
        "Creating new block data for block index: {}, chunk_range:[{}-{}], "
        "len: {}, in slice: {}",
        block_index, chunk_offset, (chunk_offset + len), len, ToString());
  } else {
    VLOG(4) << fmt::format(
        "Found existing block data for block index: {}, chunk_range:[{}-{}]"
        ", len: {}, in slice: {}",
        block_index, chunk_offset, (chunk_offset + len), len, ToString());
  }

  return iter->second.get();
}

Status SliceData::Write(const char* buf, uint64_t size, uint64_t chunk_offset) {
  uint64_t end_in_chunk = chunk_offset + size;

  VLOG(4) << fmt::format(
      "Start writing chunk_range: [{}-{}] len: {} to slice {}", chunk_offset,
      end_in_chunk, size, ToString());

  CHECK_GT(size, 0);
  CHECK_LE(chunk_offset, End());
  CHECK_GE(chunk_offset, chunk_offset);

  uint64_t block_size = context_.block_size;
  uint64_t block_index = chunk_offset / block_size;
  uint64_t block_offset = chunk_offset % block_size;

  const char* buf_pos = buf;

  uint64_t remain_len = size;

  std::lock_guard<std::mutex> lg(lock_);

  while (remain_len > 0) {
    uint64_t write_size = std::min(remain_len, block_size - block_offset);
    BlockData* block_data =
        FindOrCreateBlockDataUnlocked(block_index, block_offset, write_size);

    Status s = block_data->Write(buf_pos, write_size, block_offset);
    CHECK(s.ok()) << fmt::format(
        "Failed to write data to block data, block_index: {}, chunk_range: "
        "[{}-{}], len: {}, slice: {}, status: {}",
        block_index, block_offset, (block_offset + write_size), write_size,
        ToString(), s.ToString());

    remain_len -= write_size;
    buf_pos += write_size;
    block_offset = 0;
    ++block_index;
  }

  uint64_t old_end_offset_in_chunk = End();
  if (end_in_chunk > old_end_offset_in_chunk) {
    uint64_t old_len = len_;
    len_ += end_in_chunk - old_end_offset_in_chunk;

    VLOG(4) << fmt::format(
        "Updating len of slice data, old len: {}, old end offset in chunk: {}, "
        "slice: {}",
        old_len, old_end_offset_in_chunk, ToString());
  }

  VLOG(4) << fmt::format(
      "End writing chunk_range: [{}-{}], len: {} to slice: {}", chunk_offset,
      end_in_chunk, size, ToString());

  return Status::OK();
}

void SliceData::FlushAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("Start FlushAsync slice_data: {}", ToString());

  {
    std::lock_guard<std::mutex> lg(lock_);
    CHECK(!flushing_)
        << "Flush already in progress, unexpected state for slice: "
        << ToString();
    flushing_ = true;
    flush_cb_.swap(cb);
  }

  vfs_hub_->GetFlushExecutor()->Execute([this]() { this->DoFlush(); });

  VLOG(4) << fmt::format(
      "End FlushAsync DoFlushAsync is scheduled for slice_data: {}",
      ToString());
}

void SliceData::DoFlush() {
  VLOG(4) << fmt::format("Starting flush for slice: {}", ToString());

  uint64_t slice_id = 0;
  Status s = vfs_hub_->GetMetaSystem()->NewSliceId(context_.ino, &slice_id);
  if (!s.ok()) {
    LOG(ERROR) << fmt::format(
        "Failed to get new slice id for slice: {}, status: {}", ToString(),
        s.ToString());
    FlushDone(s);
    return;
  } else {
    VLOG(4) << fmt::format("Got slice id: {} for slice: {}", slice_id,
                           ToString());
  }

  std::lock_guard<std::mutex> lg(lock_);
  id_ = slice_id;

  CHECK_GT(block_datas_.size(), 0)
      << "No block data to flush for slice: " << ToString();
  flush_block_data_count_.store(block_datas_.size());

  for (const auto& [block_index, block_data_ptr] : block_datas_) {
    BlockData* block_data = block_data_ptr.get();
    VLOG(6) << fmt::format(
        "Flushing block data for block index: {}, block_data: {}", block_index,
        block_data->ToString());
    CHECK_EQ(block_data->BlockIndex(), block_index);

    IOBuffer io_buffer = block_data->ToIOBuffer();

    // TODO: read write back option from somewhere, currently using default
    cache::PutOption option;
    // TODO: Block should  take own the iobuf
    cache::BlockKey key(context_.fs_id, context_.ino, id_, block_index, 0);
    vfs_hub_->GetBlockCache()->AsyncPut(
        key, cache::Block(io_buffer),
        [this, block_data](auto&& ph1) {
          BlockDataFlushed(block_data, std::forward<decltype(ph1)>(ph1));
        },
        option);
  }
}

// callback from block cache, maybe in bthread
// Add callback pool to exec thi callback
void SliceData::BlockDataFlushed(BlockData* block_data, Status status) {
  VLOG(6) << fmt::format(
      "Block data flushed for block index: {}, status: {}, block_data: {}",
      block_data->BlockIndex(), status.ToString(), block_data->ToString());

  if (!status.ok()) {
    LOG(WARNING) << fmt::format(
        "Failed to flush block data for block index: {}, status: {}, "
        "block_data: {}",
        block_data->BlockIndex(), status.ToString(), block_data->ToString());

    std::lock_guard<std::mutex> lg(lock_);
    // TODO: save all errors
    flush_status_ = status;
  }

  if (flush_block_data_count_.fetch_sub(1) == 1) {
    // All block data flushed, finalize the flush operation.
    Status flush_status;
    {
      std::lock_guard<std::mutex> lg(lock_);
      flush_status = flush_status_;
    }
    FlushDone(flush_status);
  }
}

void SliceData::FlushDone(Status s) {
  VLOG(4) << fmt::format("Flush done for slice: {}, status: {}", ToString(),
                         s.ToString());

  StatusCallback cb;
  {
    std::lock_guard<std::mutex> lg(lock_);
    cb.swap(flush_cb_);
  }

  cb(s);

  VLOG(4) << fmt::format("Flush callback executed for slice: {}, status: {}",
                         ToString(), s.ToString());
}

void SliceData::GetCommitSlices(std::vector<Slice>& slices) {
  uint64_t chunk_start_in_file = context_.chunk_index * context_.chunk_size;

  std::lock_guard<std::mutex> lg(lock_);

  for (const auto& [block_index, block_data_ptr] : block_datas_) {
    slices.push_back(Slice{
        .id = id_,
        .offset = chunk_start_in_file + block_data_ptr->ChunkOffset(),
        .length = block_data_ptr->Len(),
        .compaction = 0,
        .is_zero = false,
        .size = block_data_ptr->Len(),
    });

    VLOG(4) << fmt::format(
        "Generated commit_slice: {}, for block: {} in slice: {}",
        Slice2Str(slices.back()), block_data_ptr->ToString(), ToString());
  }
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs