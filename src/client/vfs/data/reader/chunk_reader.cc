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

#include "client/vfs/data/reader/chunk_reader.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "cache/utils/context.h"
#include "client/common/const.h"
#include "client/common/utils.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/reader/reader_common.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("ChunkReader::" + std::string(__FUNCTION__))

ChunkReader::ChunkReader(VFSHub* hub, uint64_t fh, uint64_t ino, uint64_t index)
    : hub_(hub),
      fh_(fh),
      chunk_(hub->GetFsInfo().id, ino, index, hub->GetFsInfo().chunk_size,
             hub->GetFsInfo().block_size, hub->GetPageSize()) {}

void ChunkReader::BlockReadCallback(ContextSPtr ctx, ChunkReader* reader,
                                    const BlockCacheReadReq& req,
                                    ReaderSharedState& shared, Status s) {
  auto span = reader->hub_->GetTracer()->StartSpanWithContext(
      kVFSDataMoudule, METHOD_NAME(), ctx);

  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "{} ChunkReader fail read block_key: {}, buf_pos: {}, block_req: {} "
        "status: {}",
        reader->UUID(), req.key.StoreKey(), Char2Addr(req.buf_pos),
        req.block_req.ToString(), s.ToString());
  } else {
    VLOG(6) << fmt::format(
        "{} ChunkReader success read block_key: {}, buf_pos: {}, block_req: "
        "{}, io_buf_size: {}",
        reader->UUID(), req.key.StoreKey(), Char2Addr(req.buf_pos),
        req.block_req.ToString(), req.io_buffer.Size());
  }

  {
    auto copy_span = reader->hub_->GetTracer()->StartSpanWithParent(
        kVFSDataMoudule, "ChunkReader::BlockReadCallback.IoBufferCopy", *span);
    if (s.ok()) {
      req.io_buffer.CopyTo(req.buf_pos);
    }
  }

  {
    std::lock_guard<std::mutex> lock(shared.mtx);
    if (!s.ok()) {
      // Handle read failure with error priority: other errors > NotFound
      if (shared.status.ok()) {
        // First error, record it directly
        shared.status = s;
      } else if (shared.status.IsNotFound() && !s.IsNotFound()) {
        // If current status is NotFound but new error is not, override with
        // higher priority error
        shared.status = s;
      }
      // For all other cases, keep the first/higher priority error
    }

    if (++shared.num_done >= shared.total) {
      shared.cv.notify_all();
    }
  }
}

void ChunkReader::ReadAsync(ContextSPtr ctx, const ChunkReadReq& req,
                            StatusCallback cb) {
  hub_->GetReadExecutor()->Execute(
      [this, ctx, &req, cb]() { DoRead(ctx, req, cb); });
}

void ChunkReader::DoRead(ContextSPtr ctx, const ChunkReadReq& req,
                         StatusCallback cb) {
  auto* tracer = hub_->GetTracer();
  auto span = tracer->StartSpanWithContext(kVFSDataMoudule, METHOD_NAME(), ctx);

  uint64_t chunk_offset = req.offset;
  uint64_t size = req.to_read_size;
  char* buf = req.buf;

  uint64_t read_file_offset = chunk_.chunk_start + chunk_offset;
  uint64_t end_read_file_offset = read_file_offset + size;

  uint64_t end_read_chunk_offet = chunk_offset + size;

  VLOG(4) << fmt::format(
      "{} ChunkReader Read Start buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      UUID(), Char2Addr(buf), size, chunk_offset, end_read_chunk_offet,
      read_file_offset, end_read_file_offset);

  CHECK_GE(chunk_.chunk_end, end_read_file_offset);

  int32_t retry = 0;
  Status ret;
  do {
    uint64_t remain_len = size;

    ChunkSlices chunk_slices;
    Status s = GetSlices(span->GetContext(), &chunk_slices);
    if (!s.ok()) {
      LOG(WARNING) << fmt::format("{} Failed GetSlices, status: {}", UUID(),
                                  s.ToString());
      cb(s);
      return;
    }

    std::vector<SliceReadReq> slice_reqs;
    FileRange range{.offset = read_file_offset, .len = size};
    {
      auto process_slice_reqs_span = tracer->StartSpanWithParent(
          kVFSDataMoudule, "ChunkReader::DoRead.ProcessReadRequest", *span);
      slice_reqs = ProcessReadRequest(chunk_slices.slices, range);
    }

    std::vector<BlockReadReq> block_reqs;

    {
      auto slice_req_to_block_req_span = tracer->StartSpanWithParent(
          kVFSDataMoudule,
          "ChunkReader::DoRead.ConvertSliceReadReqToBlockReadReqs", *span);

      for (auto& slice_req : slice_reqs) {
        VLOG(6) << fmt::format("{} Read slice_req:", UUID(),
                               slice_req.ToString());

        if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
          std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
              slice_req, chunk_.fs_id, chunk_.ino, chunk_.chunk_size,
              chunk_.block_size);

          block_reqs.insert(block_reqs.end(),
                            std::make_move_iterator(reqs.begin()),
                            std::make_move_iterator(reqs.end()));
        } else {
          char* buf_pos = buf + (slice_req.file_offset - read_file_offset);
          VLOG(6) << fmt::format("{} Read buf: {}, zero fill, read_size: {}",
                                 UUID(), Char2Addr(buf_pos), slice_req.len);
          memset(buf_pos, 0, slice_req.len);
        }
      }
    }

    std::vector<BlockCacheReadReq> block_cache_reqs;
    block_cache_reqs.reserve(block_reqs.size());

    for (auto& block_req : block_reqs) {
      cache::BlockKey key(chunk_.fs_id, chunk_.ino, block_req.block.slice_id,
                          block_req.block.index, block_req.block.version);

      char* buf_pos = buf + (block_req.block.file_offset +
                             block_req.block_offset - read_file_offset);

      VLOG(6) << fmt::format("{} Read block_key: {}, buf: {}, block_req: {}",
                             UUID(), key.StoreKey(), Char2Addr(buf_pos),
                             block_req.ToString());

      cache::RangeOption option;
      option.retrive = true;
      option.block_size = block_req.block.block_len;

      block_cache_reqs.emplace_back(BlockCacheReadReq{.key = key,
                                                      .option = option,
                                                      .io_buffer = IOBuffer(),
                                                      .buf_pos = buf_pos,
                                                      .block_req = block_req});
    }

    ReaderSharedState shared;
    shared.total = block_cache_reqs.size();
    shared.num_done = 0;
    shared.status = Status::OK();

    for (auto& block_cache_req : block_cache_reqs) {
      auto block_cache_range_span = tracer->StartSpanWithParent(
          kVFSDataMoudule, "ChunkReader::DoRead.AsyncRange", *span);

      auto callback = [this, &span, &block_cache_req, &shared,
                       span_ptr = block_cache_range_span.release()](Status s) {
        std::unique_ptr<ITraceSpan> block_cache_range_span(span_ptr);
        block_cache_range_span->End();
        BlockReadCallback(span->GetContext(), this, block_cache_req, shared, s);
      };

      hub_->GetBlockCache()->AsyncRange(
          cache::NewContext(), block_cache_req.key,
          block_cache_req.block_req.block_offset, block_cache_req.block_req.len,
          &block_cache_req.io_buffer, std::move(callback),
          block_cache_req.option);
    }

    {
      std::unique_lock<std::mutex> lock(shared.mtx);
      while (shared.num_done < shared.total) {
        shared.cv.wait(lock);
      }

      ret = shared.status;
    }

    LOG_IF(WARNING, !ret.ok()) << fmt::format(
        "{} ChunkReader Read failed, status: {}, retry: {}, "
        "chunk_range: [{}-{}], file_range: [{}-{}]",
        UUID(), ret.ToString(), retry, chunk_offset, end_read_chunk_offet,
        read_file_offset, end_read_file_offset);

    if (ret.IsNotFound()) {
      InvalidateSlices(chunk_slices.version);
    }
  } while (ret.IsNotFound() &&
           retry++ < FLAGS_client_vfs_read_max_retry_block_not_found);

  VLOG(4) << fmt::format("{} ChunkReader Read End", UUID());

  cb(ret);
}

void ChunkReader::Invalidate() {
  VLOG(4) << fmt::format("{} Invalidate, cversion: {}", UUID(),
                         cversion_.load(std::memory_order_relaxed));
  std::lock_guard<std::mutex> lg(mutex_);
  cversion_ = kInvalidVersion;
  slices_.clear();
}

static std::string SlicesToString(const std::vector<Slice>& slices) {
  std::ostringstream oss;
  oss << "[";
  for (size_t i = 0; i < slices.size(); ++i) {
    oss << Slice2Str(slices[i]);
    if (i < slices.size() - 1) {
      oss << ", ";
    }
  }
  oss << "]";
  return oss.str();
}

Status ChunkReader::GetSlices(ContextSPtr ctx, ChunkSlices* chunk_slices) {
  auto* tracer = hub_->GetTracer();
  auto span = tracer->StartSpanWithContext(kVFSDataMoudule, METHOD_NAME(), ctx);

  std::lock_guard<std::mutex> lg(mutex_);
  if (cversion_ == kInvalidVersion) {
    VLOG(3) << fmt::format("{} cached chunk_slices invalidate, read from meta",
                           UUID());

    auto slice_span = tracer->StartSpanWithParent(
        kVFSDataMoudule, "ChunkReader::GetSlices.ReadSlice", *span);

    std::vector<Slice> slices;
    DINGOFS_RETURN_NOT_OK(hub_->GetMetaSystem()->ReadSlice(
        slice_span->GetContext(), chunk_.ino, chunk_.index, fh_, &slices));

    cversion_.store(next_version_, std::memory_order_relaxed);
    slices_ = std::move(slices);

    next_version_++;
  }

  chunk_slices->version = cversion_;
  chunk_slices->slices = slices_;

  VLOG(9) << fmt::format("{} GetSlices, version: {}, slices: {}", UUID(),
                         chunk_slices->version,
                         SlicesToString(chunk_slices->slices));

  return Status::OK();
}

void ChunkReader::InvalidateSlices(uint32_t version) {
  VLOG(4) << fmt::format("{} InvalidateSlices, version: {}, cversion: {}",
                         UUID(), version,
                         cversion_.load(std::memory_order_relaxed));
  std::lock_guard<std::mutex> lg(mutex_);
  if (cversion_ <= version) {
    cversion_ = kInvalidVersion;
    slices_.clear();
  }
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs
