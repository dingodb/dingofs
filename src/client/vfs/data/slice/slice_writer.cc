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

#include "client/vfs/data/slice/slice_writer.h"

#include <butil/iobuf.h>
#include <butil/time.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "client/vfs/common/helper.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

BlockData* SliceWriter::FindBlockDataUnlocked(uint32_t block_index) {
  auto iter = block_datas_.find(block_index);
  if (iter == block_datas_.end()) {
    return nullptr;
  }
  VLOG(4) << fmt::format(
      "{} Found existing block data for block index: {}, block: {}", UUID(),
      block_index, iter->second->ToString());
  return iter->second.get();
}

BlockData* SliceWriter::CreateBlockDataUnlocked(uint32_t block_index,
                                                int32_t block_offset) {
  auto [new_iter, inserted] = block_datas_.emplace(
      block_index, std::make_unique<BlockData>(context_, vfs_hub_,
                                               vfs_hub_->GetWriteMemPool(),
                                               block_index, block_offset));
  CHECK(inserted) << fmt::format(
      "{} CreateBlockDataUnlocked on existing block index: {}", UUID(),
      block_index);

  VLOG(4) << fmt::format(
      "{} Creating new block data for block index: {}, block: {}", UUID(),
      block_index, new_iter->second->ToString());

  return new_iter->second.get();
}

// --- slice_id async pre-allocation ---

void SliceWriter::StartPrepareSliceId() {
  auto state = slice_id_state_;
  auto* meta = vfs_hub_->GetMetaSystem();
  auto* trace_manager = vfs_hub_->GetTraceManager();
  Ino ino = context_.ino;
  std::string uuid = UUID();

  // VFSHub drains write_background_executor before MetaSystem/TraceManager.
  // The task owns only SliceIdState and immutable values, never SliceWriter.
  vfs_hub_->GetWriteBackgroundExecutor()->Execute([state = std::move(state),
                                                   meta, trace_manager, ino,
                                                   uuid = std::move(uuid)]() {
    auto span = trace_manager->StartSpan("SliceWriter::PrepareSliceId");
    auto ctx = SpanScope::GetContext(span);
    uint64_t slice_id = 0;
    Status s = meta->NewSliceId(ctx, ino, &slice_id);

    VLOG(2) << fmt::format("{} PrepareSliceId status={}", uuid, s.ToString());
    if (s.ok()) {
      CHECK_GT(slice_id, 0);
      if (!state->Publish(slice_id)) {
        // INFO on purpose: the abandoned id still shows up as a vfs_meta
        // new_slice_id record with no downstream write_slice/block_access
        // trace; without this line a miscompare analyst reads it as a lost
        // write.
        LOG(INFO) << fmt::format(
            "{} PrepareSliceId raced DoFlush fallback, abandoning id {} "
            "(winner {})",
            uuid, slice_id, state->Get());
      }
    } else {
      LOG(WARNING) << fmt::format(
          "{} PrepareSliceId failed: {}, DoFlush will retry synchronously",
          uuid, s.ToString());
    }
  });
}

// --- Write with streaming upload ---

Status SliceWriter::Write(ContextSPtr ctx, const char* buf, int32_t size,
                          int32_t chunk_offset) {
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("SliceWriter::Write",
                                                          ctx->GetTraceSpan());

  int32_t end_in_chunk = chunk_offset + size;

  VLOG(4) << fmt::format("{} Start writing chunk_range: [{}-{}] len: {}",
                         UUID(), chunk_offset, end_in_chunk, size);

  CHECK_GT(size, 0);
  CHECK_EQ(chunk_offset, End()) << fmt::format(
      "{} Unexpected chunk offset: {}, End(): {}, chunk_offset_: {}",
      ToString(), chunk_offset, End(), chunk_offset_);

  int32_t block_size = context_.block_size;
  int32_t slice_internal_offset = chunk_offset - chunk_offset_;
  uint32_t block_index = slice_internal_offset / block_size;
  int32_t block_offset = slice_internal_offset % block_size;

  // One memcpy unit in phase 2: which block, where in it, from where in buf.
  struct BlockApply {
    BlockData* block;
    const char* buf;
    int32_t size;
    int32_t block_offset;
  };
  // Pages this call newly reserved in a block -- the rollback unit.
  struct Reserved {
    BlockData* block;
    std::vector<uint32_t> pages;
  };

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK(!flush_requested_) << fmt::format(
        "{} Write called after FlushAsync closed the write epoch", UUID());

    std::vector<BlockApply> applies;
    std::vector<Reserved> reserved;        // every block's new pages this call
    std::vector<uint32_t> created_blocks;  // BlockData newly created this call

    // ---- Phase 1: reserve pages across ALL blocks (allocate only) ----
    // Nothing is memcpy'd and len_ is untouched, so any pool miss can be fully
    // rolled back, leaving zero visible state -- a same-offset retry stays
    // clean (no contiguity CHECK trip) and flush can't pick up a half-write.
    Status fail_status;
    bool failed = false;
    {
      uint32_t bi = block_index;
      int32_t bo = block_offset;
      const char* p = buf;
      int32_t remain_len = size;
      while (remain_len > 0) {
        int32_t write_size = std::min(remain_len, block_size - bo);

        BlockData* block = FindBlockDataUnlocked(bi);
        if (block == nullptr) {
          block = CreateBlockDataUnlocked(bi, bo);
          created_blocks.push_back(bi);
        }

        std::vector<uint32_t> pages;
        Status s = block->ReservePages(write_size, bo, &pages);
        reserved.push_back({block, std::move(pages)});
        if (!s.ok()) {
          fail_status = s;
          failed = true;
          break;
        }
        applies.push_back({block, p, write_size, bo});

        remain_len -= write_size;
        p += write_size;
        bo = 0;
        ++bi;
      }
    }

    // ---- Rollback: free this call's new pages, drop this call's new blocks
    // ----
    if (failed) {
      for (auto it = reserved.rbegin(); it != reserved.rend(); ++it) {
        it->block->RollbackPages(it->pages);
      }
      for (auto it = created_blocks.rbegin(); it != created_blocks.rend();
           ++it) {
        block_datas_.erase(*it);
      }
      // Page pool could not back this slice; record the rolled-back range so
      // the locality is visible by default (the VFSImpl boundary log only knows
      // the whole-write offset/size, not which slice failed).
      LOG(INFO) << fmt::format(
          "{} Write reserve failed, rolled back chunk_range: [{}-{}], "
          "status: {}",
          UUID(), chunk_offset, end_in_chunk, fail_status.ToString());
      return fail_status;
    }

    // ---- Phase 2: every page exists -- memcpy + bump each BlockData::len_
    // ----
    for (const auto& a : applies) {
      a.block->ApplyWrite(SpanScope::GetContext(span), a.buf, a.size,
                          a.block_offset);
    }

    // ---- Publish slice len (only after full success) ----
    // chunk_offset_ is const (forward-append only); only len_ grows.
    int32_t old_len = len_;
    len_ += size;
    VLOG(4) << fmt::format("{} Update slice data, old_len: {}, updated: {}",
                           UUID(), old_len, ToStringUnlocked());

    // ---- Trigger streaming upload ----
    // flush_requested_ was checked under this same lock, so this writer still
    // belongs to its write epoch. ChunkWriter normally enforces the same
    // ownership transition; the local CHECK makes violations fail loudly.
    if (len_ >= context_.block_size) {
      FlushUpTo(len_);
    }
  }

  VLOG(4) << fmt::format("{} End writing chunk_range: [{}-{}], len: {}", UUID(),
                         chunk_offset, end_in_chunk, size);

  return Status::OK();
}

// --- FlushUpTo: upload all blocks with end <= written_len ---

void SliceWriter::FlushUpTo(int32_t written_len) {
  uint64_t sid = slice_id_state_->Get();
  if (sid == 0) {
    LOG(INFO) << fmt::format(
        "{} FlushUpTo skipped: slice_id not ready (pending or alloc failed)",
        UUID());
    return;
  }

  int32_t block_size = context_.block_size;
  uint32_t blocks_uploaded = 0;

  for (auto it = block_datas_.begin(); it != block_datas_.end();) {
    int32_t end = (it->first + 1) * block_size;
    if (end <= written_len) {
      flush_barrier_->TrackStreamingUpload(FlushBarrier::BlockInfo{
          .index = it->first,
          .size = static_cast<uint32_t>(it->second->Len()),
          .start_us = static_cast<uint64_t>(butil::cpuwide_time_us())});
      BlockData* raw = it->second.release();

      SubmitBlockUpload(sid, it->first, raw);

      it = block_datas_.erase(it);
      ++blocks_uploaded;
    } else {
      ++it;
    }
  }

  if (blocks_uploaded > 0) {
    VLOG(2) << fmt::format(
        "{} FlushUpTo uploaded={}, remaining={}, written_len={}", UUID(),
        blocks_uploaded, block_datas_.size(), written_len);
  }
}

// --- Async block upload ---

void SliceWriter::SubmitBlockUpload(uint64_t slice_id, uint32_t block_index,
                                    BlockData* block_data) {
  auto span =
      vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::SubmitBlockUpload");

  BlockKey key(slice_id, block_index, block_data->Len());
  BlockHandle handle(context_.fs_id, key);

  PutReq req;
  req.handle = handle;
  req.data = block_data->ToIOBuffer();

  bool writeback = FLAGS_vfs_data_writeback;
  if (!writeback) {
    writeback = vfs_hub_->GetFileSuffixWatcher()->ShouldWriteback(context_.ino);
  }
  req.write_back = writeback;

  VLOG(4) << fmt::format("{} SubmitBlockUpload key={}, block_index={}, len={}",
                         UUID(), key.StoreKey(), block_index,
                         block_data->Len());

  auto barrier = flush_barrier_;
  auto* cb_executor = vfs_hub_->GetCBExecutor();
  std::string uuid = UUID();
  std::string store_key = key.StoreKey();

  // VFSHub drains BlockStore before CBExecutor. Upload callbacks retain only
  // the barrier, immutable diagnostics, and BlockData, never SliceWriter.
  auto on_done = [barrier = std::move(barrier), block_data, span, cb_executor,
                  uuid = std::move(uuid),
                  store_key = std::move(store_key)](Status s) mutable {
    SpanScope::End(span);
    cb_executor->Execute(
        [barrier = std::move(barrier), block_data, uuid = std::move(uuid),
         store_key = std::move(store_key), s = std::move(s)]() mutable {
          HandleUploadCompletion(std::move(barrier), block_data,
                                 std::move(uuid), std::move(store_key),
                                 std::move(s));
        });
  };

  vfs_hub_->GetBlockStore()->PutAsync(SpanScope::GetContext(span), req,
                                      std::move(on_done));
}

void SliceWriter::HandleUploadCompletion(std::shared_ptr<FlushBarrier> barrier,
                                         BlockData* block_data,
                                         std::string uuid,
                                         std::string store_key, Status status) {
  std::unique_ptr<BlockData> owner(block_data);
  uint32_t block_index = owner->BlockIndex();

  if (!status.ok()) {
    LOG(WARNING) << fmt::format("{} Block upload key={}, index={} failed: {}",
                                uuid, store_key, block_index,
                                status.ToString());
  } else {
    VLOG(4) << fmt::format("{} Block upload key={}, index={} succeeded", uuid,
                           store_key, block_index);
  }

  barrier->FinishUpload(block_index, status);
}

// --- FlushAsync / DoFlush ---

void SliceWriter::FlushAsync(StatusCallback cb) {
  VLOG(4) << fmt::format("{} FlushAsync Start", UUID());

  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    CHECK(!flush_requested_) << fmt::format(
        "{} FlushAsync already requested, unexpected state", UUID());
    flush_requested_ = true;
  }

  auto self = shared_from_this();
  vfs_hub_->GetFlushExecutor()->Execute(
      [self = std::move(self), cb = std::move(cb)]() mutable {
        self->DoFlush(std::move(cb));
      });
}

StatusCallback SliceWriter::MakeFlushCompletion(StatusCallback cb) {
  auto completion_state = flush_completion_state_;
  auto* cb_executor = vfs_hub_->GetCBExecutor();
  std::string uuid = UUID();
  return [completion_state = std::move(completion_state), cb_executor,
          uuid = std::move(uuid), cb = std::move(cb)](Status status) mutable {
    cb_executor->Execute([completion_state = std::move(completion_state),
                          uuid = std::move(uuid), cb = std::move(cb),
                          status = std::move(status)]() mutable {
      VLOG(4) << fmt::format("{} CompleteFlush status: {}", uuid,
                             status.ToString());
      completion_state->MarkCompleted();
      cb(std::move(status));
    });
  };
}

Status SliceWriter::GetOrAllocateSliceId(ContextSPtr ctx, uint64_t* slice_id) {
  *slice_id = slice_id_state_->Get();
  if (*slice_id != 0) {
    return Status::OK();
  }

  VLOG(2) << fmt::format(
      "{} DoFlush: prepared slice id not ready, allocating synchronously",
      UUID());
  Status status =
      vfs_hub_->GetMetaSystem()->NewSliceId(ctx, context_.ino, slice_id);
  if (!status.ok()) {
    return status;
  }
  CHECK_GT(*slice_id, 0);

  // The asynchronous preparation may publish between the initial read and
  // this allocation. Keep the first published id and abandon the other one.
  if (!slice_id_state_->Publish(*slice_id)) {
    uint64_t winner = slice_id_state_->Get();
    // INFO on purpose: see the matching abandon log in PrepareSliceId.
    LOG(INFO) << fmt::format(
        "{} DoFlush fallback raced PrepareSliceId, abandoning id {} "
        "(winner {})",
        UUID(), *slice_id, winner);
    *slice_id = winner;
  }
  return Status::OK();
}

std::map<uint32_t, BlockDataUPtr> SliceWriter::TakeRemainingBlocks() {
  std::lock_guard<std::mutex> lock(write_flush_mutex_);
  return std::move(block_datas_);
}

std::vector<FlushBarrier::BlockInfo> SliceWriter::DescribeUploads(
    const std::map<uint32_t, BlockDataUPtr>& blocks) {
  std::vector<FlushBarrier::BlockInfo> uploads;
  uploads.reserve(blocks.size());
  for (const auto& [index, block_data] : blocks) {
    uploads.push_back(FlushBarrier::BlockInfo{
        .index = index,
        .size = static_cast<uint32_t>(block_data->Len()),
        .start_us = static_cast<uint64_t>(butil::cpuwide_time_us())});
  }
  return uploads;
}

void SliceWriter::SubmitUploads(uint64_t slice_id,
                                std::map<uint32_t, BlockDataUPtr> blocks) {
  for (auto& [index, block_data] : blocks) {
    SubmitBlockUpload(slice_id, index, block_data.release());
  }
}

void SliceWriter::DoFlush(StatusCallback cb) {
  auto span = vfs_hub_->GetTraceManager()->StartSpan("SliceWriter::DoFlush");
  auto ctx = SpanScope::GetContext(span);
  uint64_t start_us = butil::cpuwide_time_us();

  VLOG(2) << fmt::format("{} DoFlush start", UUID());

  StatusCallback done = MakeFlushCompletion(std::move(cb));

  // === Step 1: Check existing upload errors (fast fail) ===
  Status existing_error = flush_barrier_->FirstError();
  if (!existing_error.ok()) {
    LOG(WARNING) << fmt::format("{} DoFlush early fail: {}", UUID(),
                                existing_error.ToString());
    flush_barrier_->AbortFlush(existing_error, std::move(done));
    return;
  }

  // === Step 2: Ensure slice_id ===
  uint64_t sid = 0;
  Status id_status = GetOrAllocateSliceId(ctx, &sid);
  if (!id_status.ok()) {
    LOG(ERROR) << fmt::format("{} DoFlush NewSliceId failed: {}", UUID(),
                              id_status.ToString());
    flush_barrier_->AbortFlush(id_status, std::move(done));
    return;
  }

  // === Step 3: Take remaining blocks (partial + any not yet streamed) ===
  auto remaining = TakeRemainingBlocks();

  VLOG(2) << fmt::format(
      "{} DoFlush merging remaining={}, streaming_inflight={}", UUID(),
      remaining.size(), flush_barrier_->Inflight());

  auto remaining_uploads = DescribeUploads(remaining);

  // === Step 4: Atomically register every final upload. ===
  if (!flush_barrier_->RegisterFinalUploads(remaining_uploads,
                                            std::move(done))) {
    return;
  }

  // === Step 5: Submit remaining blocks without waiting for streaming. ===
  SubmitUploads(sid, std::move(remaining));

  VLOG(2) << fmt::format("{} DoFlush submitted remaining={}, elapsed_us={}",
                         UUID(), remaining_uploads.size(),
                         butil::cpuwide_time_us() - start_us);
}

Slice SliceWriter::GetCommitSlice() {
  int32_t len = 0;
  {
    std::lock_guard<std::mutex> lg(write_flush_mutex_);
    len = len_;
  }

  uint64_t sid = slice_id_state_->Get();
  // Caller (ChunkFlushTask) must only call this after a successful flush,
  // at which point either PrepareSliceId or DoFlush's sync fallback has
  // published a valid id via CAS. A zero id here means contract violation.
  CHECK_GT(sid, 0) << fmt::format(
      "{} GetCommitSlice called before slice_id is published "
      "(flush not done or failed?)",
      UUID());

  Slice slice{
      .id = sid, .size = len, .off = 0, .len = len, .pos = chunk_offset_};

  VLOG(4) << fmt::format(
      "{} GetCommitSlices completed, slice: {}, slice_data: {}", UUID(),
      Slice2Str(slice), ToString());
  return slice;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
