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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "cache/utils/context.h"
#include "cache/utils/helper.h"
#include "client/common/const.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/reader/reader_common.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"

static bvar::Adder<uint64_t> vfs_block_rreq_inflighting(
    "vfs_block_rreq_inflighting");

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("ChunkReader::" + std::string(__FUNCTION__))

std::string BlockCacheReadReq::UUID() const {
  return fmt::format("rreq-{}-breq-{}", req_id, req_index);
}

std::string BlockCacheReadReq::ToString() const {
  return fmt::format("(uuid: {}, block_key: {}, block_req: {})", UUID(),
                     key.StoreKey(), block_req.ToString());
}

ChunkReader::ChunkReader(VFSHub* hub, uint64_t fh, const ChunkReadReq& req)
    : hub_(hub),
      fh_(fh),
      chunk_(hub->GetFsInfo().id, req.ino, req.index,
             hub->GetFsInfo().chunk_size, hub->GetFsInfo().block_size,
             hub->GetPageSize()),
      req_(req) {}

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

  std::vector<Slice> slices;
  uint64_t chunk_version = 0;
  DINGOFS_RETURN_NOT_OK(hub_->GetMetaSystem()->ReadSlice(
      span->GetContext(), chunk_.ino, chunk_.index, fh_, &slices,
      chunk_version));

  chunk_slices->version = chunk_version;
  chunk_slices->slices = std::move(slices);

  VLOG(9) << fmt::format("{} GetSlices, version: {}, slice_num: {}, slices: {}",
                         UUID(), chunk_slices->version,
                         chunk_slices->slices.size(),
                         SlicesToString(chunk_slices->slices));

  return Status::OK();
}

std::vector<SliceReadReq> ChunkReader::ConvertToSliceReadReqs(
    ContextSPtr ctx, const std::vector<Slice>& slices,
    const FileRange& frange) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  return ProcessReadRequest(slices, frange);
}

std::vector<BlockReadReq> ChunkReader::GetBlockReadReqs(
    ContextSPtr ctx, const std::vector<SliceReadReq>& slice_reqs) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  std::vector<BlockReadReq> block_reqs;

  for (const auto& slice_req : slice_reqs) {
    VLOG(6) << fmt::format("{} Read slice_req: {}", UUID(),
                           slice_req.ToString());

    if (slice_req.slice.has_value() && !slice_req.slice.value().is_zero) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, chunk_.fs_id, chunk_.ino, chunk_.chunk_size,
          chunk_.block_size);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    } else {
      block_reqs.insert(block_reqs.end(),
                        BlockReadReq{
                            .file_offset = slice_req.file_offset,
                            .block_offset = 0,
                            .len = slice_req.len,
                            .block = BlockDesc{},
                            .fake = true,
                        });
    }
  }

  return block_reqs;
}

// protected by shared state mtx
IOBuffer ChunkReader::GatherIoBuf(ContextSPtr ctx, ReaderSharedState* shared) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  IOBuffer ret;
  for (auto& block_cache_req : shared->block_cache_reqs) {
    ret.Append(&block_cache_req->io_buffer);
    VLOG(6) << fmt::format(
        "{} GatherIoBuf: Append iobuf: {} to out_buf: {}"
        "block_req_index: {}, block_req: {}",
        UUID(), block_cache_req->io_buffer.Describe(), ret.Describe(),
        block_cache_req->req_index, block_cache_req->block_req.ToString());
  }

  return ret;
}

void ChunkReader::OnAllBlocksComplete(ReaderSharedState* shared) {
  // append all read block iobufs
  Status final_status;
  IOBuffer data;
  {
    std::lock_guard<std::mutex> lock(shared->mtx);

    auto span = hub_->GetTracer()->StartSpanWithParent(
        kVFSDataMoudule, METHOD_NAME(), *shared->read_span);

    final_status = shared->status;

    if (final_status.ok()) {
      data = GatherIoBuf(span->GetContext(), shared);
    } else {
      LOG(WARNING) << fmt::format(
          "{} ChunkReader Read failed, status: {}, req: {}", UUID(),
          final_status.ToString(), req_.ToString());
    }
  }

  // for final_status not found, for now no need process
  StatusCallback cb;
  {
    std::lock_guard<std::mutex> lg(mtx_);
    data_buf_ = std::move(data);
    ready_ = true;
    cb.swap(cb_);
  }

  delete shared;
  root_span_->End();
  // for final_status not found, for now no need process
  VLOG(4) << fmt::format("{} ChunkReader Read End", UUID());

  // TODO: maybe use reference count to manage chunkreader?
  // after this callback, this maybe deleted, so no more code after this
  cb(final_status);
}

void ChunkReader::OnBlockReadComplete(ReaderSharedState* shared,
                                      BlockCacheReadReq* req, Status s) {
  bool all_done = false;
  {
    req->read_span->End();

    if (s.ok()) {
      VLOG(6) << fmt::format(
          "{} Success read block_req: {}, iobuf: {}, io_buf_size: {}", UUID(),
          req->ToString(), req->io_buffer.Describe(), req->io_buffer.Size());
    } else {
      LOG(WARNING) << fmt::format("{} Fail read block_req: {}, status: {}",
                                  UUID(), req->ToString(), s.ToString());
    }

    {
      std::lock_guard<std::mutex> lock(shared->mtx);
      if (!s.ok()) {
        // Handle read failure with error priority: other errors > NotFound
        if (shared->status.ok()) {
          // First error, record it directly
          shared->status = s;
        } else if (shared->status.IsNotFound() && !s.IsNotFound()) {
          // If current status is NotFound but new error is not, override with
          // higher priority error
          shared->status = s;
        }
        // For all other cases, keep the first/higher priority error
      }

      if (++shared->num_done >= shared->total) {
        all_done = true;
      }
    }
  }

  // first delete span in shard, then delete self span
  // if there no retry case, we can only save one span in future
  if (all_done) {
    OnAllBlocksComplete(shared);
  }
}

void ChunkReader::AsyncRange(ContextSPtr ctx, ReaderSharedState* shared,
                             BlockCacheReadReq* block_cache_req) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  auto callback = [this, shared, block_cache_req,
                   span_ptr = span.release()](Status s) {
    vfs_block_rreq_inflighting << -1;
    // capture this ptr to extend its lifetime
    std::unique_ptr<ITraceSpan> scoped_span(span_ptr);
    scoped_span->End();
    // dedicated use ctx for callback
    OnBlockReadComplete(shared, block_cache_req, s);
  };

  vfs_block_rreq_inflighting << 1;

  hub_->GetBlockCache()->AsyncRange(
      cache::NewContext(), block_cache_req->key,
      block_cache_req->block_req.block_offset, block_cache_req->block_req.len,
      &block_cache_req->io_buffer, std::move(callback),
      block_cache_req->option);
}

void ChunkReader::ProcessBlockCacheReadReq(ContextSPtr ctx,
                                           ReaderSharedState* shared,
                                           BlockCacheReadReq* block_cache_req) {
  auto span = hub_->GetTracer()->StartSpanWithContext(kVFSDataMoudule,
                                                      METHOD_NAME(), ctx);
  ContextSPtr span_ctx = span->GetContext();
  block_cache_req->read_span = std::move(span);

  // check block is zero block
  if (block_cache_req->block_req.fake) {
    VLOG(6) << fmt::format("{} Read fake block, block_req: {}", UUID(),
                           block_cache_req->block_req.ToString());

    // zero block, no need to read from block cache, just fill zero
    char* data = new char[block_cache_req->block_req.len];
    std::fill(data, data + block_cache_req->block_req.len, 0);
    block_cache_req->io_buffer.AppendUserData(
        data, block_cache_req->block_req.len, cache::Helper::DeleteBuffer);
    Status s = Status::OK();
    OnBlockReadComplete(shared, block_cache_req, s);
  } else {
    AsyncRange(span_ctx, shared, block_cache_req);
  }
}

void ChunkReader::ExecuteAsyncRead() {
  ITraceSpan* root;
  {
    std::lock_guard<std::mutex> lg(mtx_);
    root = root_span_.get();
  }

  auto span = hub_->GetTracer()->StartSpanWithParent(kVFSDataMoudule,
                                                     METHOD_NAME(), *root);

  ChunkSlices chunk_slices;
  Status s = GetSlices(span->GetContext(), &chunk_slices);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format("{} Failed GetSlices, status: {}", UUID(),
                                s.ToString());

    StatusCallback cb;
    {
      std::lock_guard<std::mutex> lg(mtx_);
      cb_.swap(cb);
    }

    cb(s);
    root->End();

    return;
  }

  ContextSPtr span_ctx = span->GetContext();

  std::vector<SliceReadReq> slice_reqs =
      ConvertToSliceReadReqs(span_ctx, chunk_slices.slices, req_.frange);
  std::vector<BlockReadReq> block_reqs = GetBlockReadReqs(span_ctx, slice_reqs);

  std::vector<BlockCacheReadReqUPtr> block_cache_reqs;
  block_cache_reqs.reserve(block_reqs.size());

  uint32_t block_req_index = 0;
  for (auto& block_req : block_reqs) {
    cache::BlockKey key(chunk_.fs_id, chunk_.ino, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);

    VLOG(6) << fmt::format("{} Read block_key: {}, block_req: {}", UUID(),
                           key.StoreKey(), block_req.ToString());

    cache::RangeOption option;
    option.retrive = true;
    option.block_size = block_req.block.block_len;

    // TODO: optimize memory copy for block_req
    block_cache_reqs.emplace_back(std::make_unique<BlockCacheReadReq>(
        BlockCacheReadReq{.req_id = req_.req_id,
                          .req_index = block_req_index++,
                          .block_req = block_req,
                          .key = key,
                          .option = option,
                          .io_buffer = IOBuffer()}));
  }

  std::vector<BlockCacheReadReq*> to_process_reqs;
  to_process_reqs.reserve(block_cache_reqs.size());
  for (auto& ptr : block_cache_reqs) {
    to_process_reqs.push_back(ptr.get());
  }

  auto* shared = new ReaderSharedState();
  {
    std::unique_lock<std::mutex> lock(shared->mtx);
    shared->total = block_cache_reqs.size();
    shared->num_done = 0;
    shared->status = Status::OK();
    shared->read_span = std::move(span);
    shared->block_cache_reqs = std::move(block_cache_reqs);
  }

  for (BlockCacheReadReq* block_cache_req : to_process_reqs) {
    ProcessBlockCacheReadReq(span_ctx, shared, block_cache_req);
  }
}

void ChunkReader::ReadAsync(ContextSPtr ctx, StatusCallback cb) {
  VLOG(4) << fmt::format("{} ChunkReader Read req: {}", UUID(),
                         req_.ToString());
  CHECK_GE(chunk_.chunk_end, req_.frange.End());

  auto* tracer = hub_->GetTracer();
  auto span = tracer->StartSpanWithContext(kVFSDataMoudule, METHOD_NAME(), ctx);

  {
    std::lock_guard<std::mutex> lg(mtx_);
    cb_.swap(cb);
    root_span_ = std::move(span);
  }

  ExecuteAsyncRead();
}

IOBuffer ChunkReader::GetDataBuffer() const {
  IOBuffer buf;
  {
    std::lock_guard<std::mutex> lg(mtx_);
    CHECK(ready_);
    buf = data_buf_;
  }
  return buf;
}
}  // namespace vfs

}  // namespace client

}  // namespace dingofs
