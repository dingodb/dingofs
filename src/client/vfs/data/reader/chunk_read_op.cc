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

#include "client/vfs/data/reader/chunk_read_op.h"

#include <bvar/bvar.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <cstring>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>

#include "client/vfs/blockstore/block_store.h"
#include "client/vfs/data/common/data_utils.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

namespace {

bvar::Adder<uint64_t> vfs_block_rreq_inflighting("vfs_block_rreq_inflighting");

Chunk MakeChunk(VFSHub* hub, const ChunkReq& req) {
  const FsInfo fs_info = hub->GetFsInfo();
  return Chunk(fs_info.id, req.ino, req.index, fs_info.chunk_size,
               fs_info.block_size);
}

}  // namespace

namespace detail {

ChunkReadOp::BlockCacheReadReq::BlockCacheReadReq(
    uint64_t _req_id, uint32_t _req_index, uint32_t _fs_id, uint64_t _ino,
    const BlockReadReq& _block_req, ReadBufView _dst)
    : req_id(_req_id),
      req_index(_req_index),
      fs_id(_fs_id),
      ino(_ino),
      block_req(_block_req),
      dst(_dst) {}

ChunkReadOp::BlockCacheReadReq::BlockCacheReadReq(
    BlockCacheReadReq&& other) noexcept
    : req_id(other.req_id),
      req_index(other.req_index),
      fs_id(other.fs_id),
      ino(other.ino),
      block_req(other.block_req),
      dst(other.dst),
      completed(other.completed.load(std::memory_order_relaxed)) {}

std::string ChunkReadOp::BlockCacheReadReq::UUID() const {
  return fmt::format("rreq-{}-breq-{}", req_id, req_index);
}

std::string ChunkReadOp::BlockCacheReadReq::ToString() const {
  const std::string key_str =
      block_req.key.has_value() ? block_req.key->StoreKey() : "hole";
  return fmt::format("(uuid: {}, block_key: {}, block_req: {})", UUID(),
                     key_str, block_req.ToString());
}

ChunkReadOp::ChunkReadOp(VFSHub* hub, const ChunkReq& req,
                         StatusCallback callback)
    : hub_(hub),
      chunk_(MakeChunk(hub, req)),
      req_(req),
      callback_(std::move(callback)) {}

void ChunkReadOp::Run(ContextSPtr ctx, const std::vector<Slice>& slices,
                      ReadBufView dst) {
  Prepare(slices, dst);
  DispatchAll(std::move(ctx));
}

std::string ChunkReadOp::UUID() const {
  return fmt::format("rreq-{}-chunk-{}", req_.req_id, chunk_.UUID());
}

std::vector<BlockReadReq> ChunkReadOp::BuildBlockReadReqs(
    const std::vector<SliceReadReq>& slice_reqs) const {
  std::vector<BlockReadReq> block_reqs;

  for (const auto& slice_req : slice_reqs) {
    VLOG(6) << fmt::format("{} Read slice_req: {}", UUID(),
                           slice_req.ToString());

    if (slice_req.slice.has_value() && slice_req.slice.value().id != 0) {
      std::vector<BlockReadReq> reqs = ConvertSliceReadReqToBlockReadReqs(
          slice_req, chunk_.fs_id, chunk_.ino, chunk_.chunk_size,
          chunk_.block_size, chunk_.chunk_start);

      block_reqs.insert(block_reqs.end(), std::make_move_iterator(reqs.begin()),
                        std::make_move_iterator(reqs.end()));
    } else {
      block_reqs.emplace_back(BlockReadReq{
          .file_offset = slice_req.file_offset,
          .block_offset = 0,
          .len = static_cast<int32_t>(slice_req.len),
          .key = std::nullopt,
      });
    }
  }

  return block_reqs;
}

void ChunkReadOp::Prepare(const std::vector<Slice>& slices, ReadBufView dst) {
  CHECK_GE(chunk_.chunk_end, req_.frange.End());
  CHECK_GT(req_.frange.len, 0);
  CHECK_NOTNULL(dst.base);

  std::vector<SliceReadReq> slice_reqs =
      ProcessReadRequest(slices, req_.frange, chunk_.chunk_start);
  std::vector<BlockReadReq> block_reqs = BuildBlockReadReqs(slice_reqs);
  CHECK(!block_reqs.empty());

  VLOG(4) << fmt::format("{} ChunkReadOp read req: {}, block_num: {}", UUID(),
                         req_.ToString(), block_reqs.size());

  requests_.reserve(block_reqs.size());

  size_t slot_off = 0;
  for (const auto& block_req : block_reqs) {
    const size_t block_len = static_cast<size_t>(block_req.len);
    CHECK_LE(slot_off + block_len, dst.len);

    requests_.emplace_back(
        req_.req_id, static_cast<uint32_t>(requests_.size()), chunk_.fs_id,
        chunk_.ino, block_req,
        ReadBufView{dst.base, dst.offset + slot_off, block_len});
    slot_off += block_len;
  }

  CHECK_EQ(slot_off, dst.len);
  remaining_.store(requests_.size(), std::memory_order_release);
}

void ChunkReadOp::DispatchAll(ContextSPtr ctx) {
  for (size_t i = 0; i < requests_.size(); ++i) {
    DispatchOne(ctx, i);  // each request gets its own ContextSPtr reference
  }
}

void ChunkReadOp::DispatchOne(ContextSPtr ctx, size_t index) {
  auto& read_req = requests_[index];

  if (read_req.block_req.IsHole()) {
    VLOG(6) << fmt::format("{} Read hole block, block_req: {}", UUID(),
                           read_req.ToString());
    std::memset(read_req.dst.data(), 0, read_req.dst.len);
    OnBlockDone(index, Status::OK());
    return;
  }

  vfs_block_rreq_inflighting << 1;

  RangeReq range_req;
  range_req.handle =
      BlockHandle(read_req.fs_id, read_req.block_req.key.value());
  range_req.offset = read_req.block_req.block_offset;
  range_req.length = read_req.block_req.len;
  range_req.dst = read_req.dst;

  auto self = shared_from_this();
  hub_->GetBlockStore()->RangeAsync(
      std::move(ctx), std::move(range_req),
      [self = std::move(self), index](Status status) mutable {
        vfs_block_rreq_inflighting << -1;
        self->OnBlockDone(index, std::move(status));
      });
}

void ChunkReadOp::OnBlockDone(size_t index, Status status) {
  BlockCacheReadReq& read_req = requests_[index];

  CHECK(!read_req.completed.exchange(true, std::memory_order_relaxed))
      << UUID() << " duplicate block read callback: " << read_req.ToString();

  if (status.ok()) {
    VLOG(6) << fmt::format("{} Success read block_req: {}", UUID(),
                           read_req.ToString());
  } else {
    LOG(WARNING) << fmt::format("{} Fail read block_req: {}, status: {}",
                                UUID(), read_req.ToString(), status.ToString());

    std::lock_guard<std::mutex> lock(mutex_);
    if (status_.ok()) {
      status_ = std::move(status);
    } else if (status_.IsNotFound() && !status.IsNotFound()) {
      status_ = std::move(status);
    }
  }

  // Publishing completion is the last normal access for a non-final callback.
  const uint64_t previous = remaining_.fetch_sub(1, std::memory_order_acq_rel);
  CHECK_GT(previous, 0) << UUID() << " completion counter underflow";

  if (previous == 1) {
    Finish();
  }
}

void ChunkReadOp::Finish() {
  Status final_status;
  StatusCallback callback;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    final_status = std::move(status_);
    if (!final_status.ok()) {
      const std::string original_status = final_status.ToString();
      LOG(ERROR) << fmt::format(
          "{} Data block read failed, convert status to IoError, "
          "original_status: {}, req: {}",
          UUID(), original_status, req_.ToString());
      final_status = Status::IoError(
          fmt::format("data block read failed: {}", original_status));
    }

    callback = std::move(callback_);
  }

  VLOG(4) << fmt::format("{} ChunkReadOp finished", UUID());
  callback(std::move(final_status));
  // Do not access operation members after invoking the final callback.
}

}  // namespace detail

void StartChunkRead(ContextSPtr ctx, VFSHub* hub, const ChunkReq& req,
                    const std::vector<Slice>& slices, ReadBufView dst,
                    StatusCallback callback) {
  auto op =
      std::make_shared<detail::ChunkReadOp>(hub, req, std::move(callback));
  op->Run(std::move(ctx), slices, dst);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
