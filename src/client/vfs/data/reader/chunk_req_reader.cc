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

#include "client/vfs/data/reader/chunk_req_reader.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "cache/utils/helper.h"
#include "client/vfs/data/common/common.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/data/reader/chunk_req.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/status.h"
#include "common/trace/context.h"

static bvar::Adder<uint64_t> vfs_block_rreq_inflighting(
    "vfs_block_rreq_inflighting");

namespace dingofs {
namespace client {
namespace vfs {

namespace {

BlockKey GenerateBlockKey(const BlockCacheReadReq* req) {
  BlockKey key(req->fs_id, req->ino, req->block_req.block.slice_id,
               req->block_req.block.index, req->block_req.block.version);
  return key;
}

};  // namespace

std::string BlockCacheReadReq::UUID() const {
  return fmt::format("rreq-{}-breq-{}", req_id, req_index);
}

std::string BlockCacheReadReq::ToString() const {
  return fmt::format("(uuid: {}, block_key: {}, block_req: {})", UUID(),
                     GenerateBlockKey(this).StoreKey(), block_req.ToString());
}

ChunkReqReader::ChunkReqReader(VFSHub* hub, const ChunkReq& req)
    : hub_(hub),
      chunk_(hub->GetFsInfo().id, req.ino, req.index,
             hub->GetFsInfo().chunk_size, hub->GetFsInfo().block_size),
      req_(req) {}

std::vector<BlockReadReq> ChunkReqReader::GetBlockReadReqs(
    const std::vector<SliceReadReq>& slice_reqs) {
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
IOBuffer ChunkReqReader::GatherIoBuf(ContextSPtr ctx,
                                         ReaderSharedState* shared) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "ChunkReqReader::GatherIoBuf", ctx->GetTraceSpan());
  IOBuffer ret;
  for (const auto& block_cache_req : shared->block_cache_reqs) {
    ret.Append(&block_cache_req->io_buffer);
    VLOG(6) << fmt::format(
        "{} GatherIoBuf: Append iobuf: {} to out_buf: {}"
        "block_req_index: {}, block_req: {}",
        UUID(), block_cache_req->io_buffer.Describe(), ret.Describe(),
        block_cache_req->req_index, block_cache_req->block_req.ToString());
  }

  return ret;
}

void ChunkReqReader::OnAllBlocksComplete(ReaderSharedState* shared) {
  // append all read block iobufs
  Status final_status;
  IOBuffer data;
  {
    std::lock_guard<std::mutex> lock(shared->mtx);

    auto span = hub_->GetTraceManager()->StartChildSpan(
        "ChunkReqReader::OnAllBlocksComplete", shared->read_span);

    final_status = shared->status;

    if (final_status.ok()) {
      data = GatherIoBuf(SpanScope::GetContext(span), shared);
    } else {
      LOG(WARNING) << fmt::format(
          "{} ChunkReqReader Read failed, status: {}, req: {}", UUID(),
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
  //  for final_status not found, for now no need process
  VLOG(4) << fmt::format("{} ChunkReqReader Read End", UUID());

  // TODO: maybe use reference count to manage ChunkReqReader?
  // after this callback, this maybe deleted, so no more code after this
  cb(final_status);
}

void ChunkReqReader::OnBlockReadComplete(ReaderSharedState* shared,
                                             BlockCacheReadReq* req, Status s) {
  if (s.ok()) {
    VLOG(6) << fmt::format(
        "{} Success read block_req: {}, iobuf: {}, io_buf_size: {}", UUID(),
        req->ToString(), req->io_buffer.Describe(), req->io_buffer.Size());
  } else {
    LOG(WARNING) << fmt::format("{} Fail read block_req: {}, status: {}",
                                UUID(), req->ToString(), s.ToString());

    auto span = hub_->GetTraceManager()->StartChildSpan(
        "ChunkReqReader::WaitAllBlocksLock", shared->read_span);

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
  }

  // whe need RMW here
  uint64_t done = shared->num_done.fetch_add(1) + 1;
  uint64_t total = shared->total.load();
  CHECK_GE(total, done);
  if (done == total) {
    OnAllBlocksComplete(shared);
  }
}

void ChunkReqReader::ProcessBlockCacheReadReq(
    ContextSPtr ctx, ReaderSharedState* shared,
    BlockCacheReadReq* block_cache_req) {
  auto span = hub_->GetTraceManager()->StartChildSpan(
      "ChunkReqReader::ProcessBlockCacheReadReq", ctx->GetTraceSpan());
  ContextSPtr span_ctx = SpanScope::GetContext(span);

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
    vfs_block_rreq_inflighting << 1;

    auto callback = [this, shared, block_cache_req](Status s) {
      vfs_block_rreq_inflighting << -1;
      OnBlockReadComplete(shared, block_cache_req, s);
    };

    RangeReq req;
    req.block = GenerateBlockKey(block_cache_req);
    req.block_size = block_cache_req->block_req.block.block_len;
    req.offset = block_cache_req->block_req.block_offset;
    req.length = block_cache_req->block_req.len;
    req.data = &block_cache_req->io_buffer;

    hub_->GetBlockStore()->RangeAsync(span_ctx, req, std::move(callback));
  }
}

void ChunkReqReader::ReadAsync(ContextSPtr ctx,
                                   const std::vector<Slice>& slices,
                                   StatusCallback cb) {
  VLOG(4) << fmt::format("{} ChunkReqReader Read req: {}", UUID(),
                         req_.ToString());
  CHECK_GE(chunk_.chunk_end, req_.frange.End());

  auto span = hub_->GetTraceManager()->StartChildSpan(
      "ChunkReqReader::ReadAsync", ctx->GetTraceSpan());
  {
    std::lock_guard<std::mutex> lg(mtx_);
    cb_.swap(cb);
  }

  std::vector<SliceReadReq> slice_reqs =
      ProcessReadRequest(slices, req_.frange);

  std::vector<BlockReadReq> block_reqs = GetBlockReadReqs(slice_reqs);

  std::vector<BlockCacheReadReqUPtr> block_cache_reqs;
  block_cache_reqs.reserve(block_reqs.size());

  uint32_t block_req_index = 0;
  for (auto& block_req : block_reqs) {
    cache::BlockKey key(chunk_.fs_id, chunk_.ino, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);

    VLOG(6) << fmt::format("{} Read block_key: {}, block_req: {}", UUID(),
                           key.StoreKey(), block_req.ToString());

    // TODO: optimize memory copy for block_req
    block_cache_reqs.emplace_back(std::make_unique<BlockCacheReadReq>(
        BlockCacheReadReq{.req_id = req_.req_id,
                          .req_index = block_req_index++,
                          .fs_id = chunk_.fs_id,
                          .ino = chunk_.ino,
                          .block_req = block_req,
                          .io_buffer = IOBuffer()}));
  }

  std::vector<BlockCacheReadReq*> to_process_reqs;
  to_process_reqs.reserve(block_cache_reqs.size());
  for (auto& ptr : block_cache_reqs) {
    to_process_reqs.push_back(ptr.get());
  }

  auto* shared = new ReaderSharedState();
  shared->total.store(block_cache_reqs.size());
  shared->num_done.store(0);

  {
    std::unique_lock<std::mutex> lock(shared->mtx);
    shared->status = Status::OK();
    shared->read_span = std::move(span);
    shared->block_cache_reqs = std::move(block_cache_reqs);
  }

  ContextSPtr span_ctx = SpanScope::GetContext(span);

  for (BlockCacheReadReq* block_cache_req : to_process_reqs) {
    ProcessBlockCacheReadReq(span_ctx, shared, block_cache_req);
  }
}

IOBuffer ChunkReqReader::GetDataBuffer() const {
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
