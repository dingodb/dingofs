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

#include "client/vfs/data/writer/chunk_writer.h"

#include <glog/logging.h>
#include <sys/ioctl.h>

#include <boost/range/algorithm/sort.hpp>
#include <cstdint>
#include <memory>

#include "client/const.h"
#include "client/meta/vfs_meta.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/callback.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("ChunkWriter::" + std::string(__FUNCTION__))

// protected by mutex_
ChunkWriter::ChunkWriter(VFSHub* hub, uint64_t fh, uint64_t ino, uint64_t index)
    : hub_(hub),
      fh_(fh),
      chunk_(hub->GetFsInfo().id, ino, index, hub->GetFsInfo().chunk_size,
             hub->GetFsInfo().block_size, hub->GetPageSize()) {}

ChunkWriter::~ChunkWriter() {
  VLOG(4) << fmt::format("{} Destroy Chunk addr: {}", UUID(),
                         static_cast<const void*>(this));
}

Status ChunkWriter::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                          uint64_t chunk_offset) {
  auto* tracer = hub_->GetTracer();
  auto span = tracer->StartSpanWithContext(kVFSDataMoudule, METHOD_NAME(), ctx);

  uint64_t write_file_offset = chunk_.chunk_start + chunk_offset;
  ChunkWriteInfo info(buf, size, chunk_offset, write_file_offset);
  Writer writer;
  writer.write_info = &info;

  VLOG(4) << fmt::format("{} BufferWrite Start writer: {}", UUID(),
                         writer.ToString());
  CHECK_GE(chunk_.chunk_end, info.end_file_offset);

  // TODO: check mem ratio, sleep when mem is near full
  bool has_full = false;

  {
    std::unique_lock<std::mutex> lg(mutex_);
    writers_.push_back(&writer);

    while (!writer.done && &writer != writers_.front()) {
      VLOG(4) << fmt::format("{} BufferWrite Wait writer: {} ", UUID(),
                             writer.ToString());
      writer.cv.wait(lg);
    }

    if (writer.done) {
      VLOG(4) << fmt::format(
          "{} BufferWrite End, writer already done, writer: {}", UUID(),
          writer.ToString());
      return writer.status;
    }

    Writer* last_writer = &writer;

    std::vector<ChunkWriteInfo*> write_batch;
    CHECK(!writers_.empty());

    VLOG(4) << fmt::format("{} BufferWrite Get write_batch size: {}", UUID(),
                           writers_.size());

    auto iter = writers_.begin();

    while (iter != writers_.end()) {
      Writer* writer = *iter;
      CHECK_NOTNULL(writer);

      write_batch.push_back(writer->write_info);

      last_writer = writer;

      ++iter;
    }

    // TODO: merge write_batch by chunk_offset
    boost::range::sort(write_batch,
                       [](const ChunkWriteInfo* a, const ChunkWriteInfo* b) {
                         return a->chunk_offset < b->chunk_offset;
                       });

    for (const ChunkWriteInfo* write_info : write_batch) {
      VLOG(4) << fmt::format("{} BufferWrite batch write_info: {}", UUID(),
                             write_info->ToString());

      std::unique_ptr<SliceData> writing_slice =
          GetSliceUnlocked(write_info->chunk_offset, write_info->size);

      lg.unlock();

      CHECK_NOTNULL(writing_slice);

      Status s =
          writing_slice->Write(span->GetContext(), write_info->buf,
                               write_info->size, write_info->chunk_offset);
      CHECK(s.ok());

      if (writing_slice->Len() == chunk_.chunk_size) {
        has_full = true;
        VLOG(4) << fmt::format("{} Found full slice_data: {}", UUID(),
                               writing_slice->ToString());
      }

      lg.lock();

      PutSliceUnlocked(std::move(writing_slice));
    }

    while (true) {
      Writer* ready = writers_.front();
      writers_.pop_front();
      if (ready != &writer) {
        // TOOD: we need member every writer status when slice write can be fail
        ready->status = Status::OK();
        ready->done = true;
        ready->cv.notify_all();
      }
      if (ready == last_writer) break;
    }

    if (!writers_.empty()) {
      writers_.front()->cv.notify_all();
    }
  }

  if (has_full) {
    TriggerFlush();
  }

  return Status::OK();
}

void ChunkWriter::PutSliceUnlocked(std::unique_ptr<SliceData> slice_data) {
  slices_.emplace(slice_data->Seq(), std::move(slice_data));
}

std::unique_ptr<SliceData> ChunkWriter::GetSliceUnlocked(uint64_t chunk_pos,
                                                         uint64_t size) {
  std::unique_ptr<SliceData> slice = FindWritableSliceUnLocked(chunk_pos, size);
  if (slice == nullptr) {
    slice = CreateSliceUnlocked(chunk_pos);
    VLOG(4) << fmt::format("{} Created new slice_data: {}", UUID(),
                           slice->ToString());
  }
  DCHECK_NOTNULL(slice);
  return slice;
}

// TODO: maybe this algorithm is not good enough
std::unique_ptr<SliceData> ChunkWriter::FindWritableSliceUnLocked(
    uint64_t chunk_pos, uint64_t size) {
  uint64_t end_in_chunk = chunk_pos + size;

  //   from new to old
  for (auto it = slices_.rbegin(); it != slices_.rend(); ++it) {
    uint64_t seq = it->first;
    SliceData* slice_data = it->second.get();
    DCHECK_NOTNULL(slice_data);

    VLOG(6) << fmt::format(
        "{} FindWritableSliceUnLocked for chunk_range: "
        "[{}-{}], size: {}, seq: {}, check slice_data: {}",
        UUID(), chunk_pos, end_in_chunk, size, seq, slice_data->ToString());

    // if overlap with slice, then use new slice
    if (chunk_pos < slice_data->End() &&
        end_in_chunk > slice_data->ChunkOffset()) {
      return nullptr;
    }

    if (chunk_pos == slice_data->End() ||
        end_in_chunk == slice_data->ChunkOffset()) {
      std::unique_ptr<SliceData> to_return = std::move(it->second);
      CHECK_EQ(slices_.erase(seq), 1);
      return to_return;
    }
  }

  return nullptr;
}

std::unique_ptr<SliceData> ChunkWriter::CreateSliceUnlocked(
    uint64_t chunk_pos) {
  // Use static because chunk with same index may be deleted and recreated
  uint64_t seq = slice_seq_id_gen.fetch_add(1, std::memory_order_relaxed);
  SliceDataContext ctx(chunk_.fs_id, chunk_.ino, chunk_.index, seq,
                       chunk_.chunk_size, chunk_.block_size, chunk_.page_size);
  return std::make_unique<SliceData>(ctx, hub_, chunk_pos);
}

Status ChunkWriter::WriteToBlockCache(const cache::BlockKey& key,
                                      const cache::Block& block,
                                      cache::PutOption option) {
  return hub_->GetBlockCache()->Put(cache::NewContext(), key, block, option);
}

Status ChunkWriter::CommitSlices(ContextSPtr ctx,
                                 const std::vector<Slice>& slices) {
  return hub_->GetMetaSystem()->WriteSlice(ctx, chunk_.ino, chunk_.index, fh_,
                                           slices);
}

void ChunkWriter::AsyncCommitSlices(ContextSPtr ctx,
                                    const std::vector<Slice>& slices,
                                    StatusCallback cb) {
  Status s = hub_->GetMetaSystem()->AsyncWriteSlice(
      ctx, chunk_.ino, chunk_.index, fh_, slices, cb);
  CHECK(s.ok());
}

ChunkWriter::FlushTask ChunkWriter::fake_header_;

void ChunkWriter::FlushTaskDone(FlushTask* flush_task, Status s) {
  // TODO: get ctx from parent
  auto span = hub_->GetTracer()->StartSpan(kVFSDataMoudule, __func__);
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "{} FlushTaskDone Failed chunk_flush_task: {}, status: {}", UUID(),
        flush_task->chunk_flush_task->ToString(), s.ToString());
  }

  {
    std::lock_guard<std::mutex> lg(mutex_);
    flush_task->status = s;
    flush_task->done = true;

    CHECK(!flush_queue_.empty());

    auto* header = flush_queue_.front();
    if (flush_task != header) {
      VLOG(4) << fmt::format(
          "{} FlushTaskDone return because flush_chunk_task: {} is not the "
          "header of the flush_queue_, flush_queue size : {}, "
          "header_task_addr: {}",
          UUID(), flush_task->ToString(), flush_queue_.size(),
          static_cast<const void*>(&fake_header_));
      return;
    } else {
      VLOG(4) << fmt::format(
          "{} FlushTaskDone become header_task: {} of the flush_queue_, "
          "flush_task_addr: {}, flush_queue size: {}, insert fake_header: {}",
          UUID(), flush_task->ToString(), static_cast<const void*>(flush_task),
          flush_queue_.size(), static_cast<const void*>(&fake_header_));

      flush_queue_.push_front(&fake_header_);
    }
  }

  // only the first flush task in the queue can be processed
  // may use another commit thread or make meta is async

  VLOG(4) << fmt::format(
      "{} FlushTaskDone header_task: {} will commit flush_tasks", UUID(),
      flush_task->UUID());

  CommitFlushTasks(span->GetContext());
}

void ChunkWriter::CommitFlushTasks(ContextSPtr ctx) {
  uint64_t commit_seq =
      commit_seq_id_gen.fetch_add(1, std::memory_order_relaxed);

  // this will be delete in SlicesCommited
  std::vector<FlushTask*> to_commit;

  {
    std::lock_guard<std::mutex> lg(mutex_);
    CHECK_GT(flush_queue_.size(), 0);

    auto it = flush_queue_.begin();
    CHECK(*it == &fake_header_)
        << "fake header must be the first, it: "
        << static_cast<const void*>(*it)
        << ", fake_header: " << static_cast<const void*>(&fake_header_);

    ++it;  // from second element, first is fake header

    while (it != flush_queue_.end()) {
      // sequence iterate the flush queue, only pick the flush task which is
      // done util meet the first not done task
      if ((*it)->done) {
        to_commit.push_back(*it);
        // erase current element, and move to next
        it = flush_queue_.erase(it);
      } else {
        break;
      }
    };

    if (to_commit.empty()) {
      VLOG(4) << fmt::format(
          "{} FlushTaskDone commit_seq: {} will return because has no "
          "flush_task to commit, fake header is removed, remain "
          "flush_queue_size: {}",
          UUID(), commit_seq, flush_queue_.size());
      flush_queue_.pop_front();
      return;
    }

  }  //  end lock_guard

  CHECK(!to_commit.empty());

  std::vector<Slice> batch_commit_slices;
  for (FlushTask* task : to_commit) {
    if (task->status.ok()) {
      std::vector<Slice> slices;
      task->chunk_flush_task->GetCommitSlices(slices);
      VLOG(4) << fmt::format(
          "{} FlushTaskDone commit_seq: {} commit chunk_flush_task: {}, "
          "slices_count: {}",
          UUID(), commit_seq, task->ToString(), slices.size());

      if (!slices.empty()) {
        std::move(slices.begin(), slices.end(),
                  std::back_inserter(batch_commit_slices));
      }

    } else {
      LOG(WARNING) << fmt::format(
          "{} FlushTaskDone commit_seq: {} skip commit fail "
          "chunk_flush_task: {}",
          UUID(), commit_seq, task->ToString());

      MarkErrorStatus(task->status);
    }
  }  // end  for to_commit

  // TODO: if we found some flush task fail, no need commit the slices

  VLOG(4) << fmt::format(
      "{} FlushTaskDone commit_seq: {} will commit flush_task_count: {} "
      "batch_slices_count: {}",
      UUID(), commit_seq, to_commit.size(), batch_commit_slices.size());

  // this will be delete in SlicesCommited
  auto* commit_context = new CommmitContext();
  commit_context->commit_seq = commit_seq;
  commit_context->flush_tasks = std::move(to_commit);
  commit_context->commit_slices = std::move(batch_commit_slices);

  if (!commit_context->commit_slices.empty()) {
    AsyncCommitSlices(
        ctx, commit_context->commit_slices,
        [self = shared_from_this(), ctx, commit_context](auto&& ph1) {
          self->SlicesCommited(ctx, commit_context,
                               std::forward<decltype(ph1)>(ph1));
        });
  } else {
    SlicesCommited(ctx, commit_context, GetErrorStatus());
  }
}

void ChunkWriter::SlicesCommited(ContextSPtr ctx, CommmitContext* commit_ctx,
                                 Status s) {
  VLOG(4) << fmt::format(
      "{} SlicesCommited commit_seq: {}, flush_task_count: {} "
      "batch_slices_count: {} commit_status: {}",
      UUID(), commit_ctx->commit_seq, commit_ctx->flush_tasks.size(),
      commit_ctx->commit_slices.size(), s.ToString());

  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "{} SlicesCommited commit_seq: {} fail commit, flush_task_count: {} "
        "batch_slices_count: {} commit_status: {}",
        UUID(), commit_ctx->commit_seq, commit_ctx->flush_tasks.size(),
        commit_ctx->commit_slices.size(), s.ToString());

    MarkErrorStatus(s);
  }

  for (FlushTask* task : commit_ctx->flush_tasks) {
    // if one task fail, all task fail
    task->cb(GetErrorStatus());
    VLOG(4) << fmt::format("{} SlicesCommited delete chunk_flush_task: {}",
                           UUID(), task->ToString());
    delete task;
  }

  VLOG(4) << fmt::format(
      "{} SlicesCommited commit_seq: {} end, delete commit_ctx", UUID(),
      commit_ctx->commit_seq);
  delete commit_ctx;

  // continue batch commit next flush task util all flush task are committed
  CommitFlushTasks(ctx);
}

void ChunkWriter::DoFlushAsync(StatusCallback cb, uint64_t chunk_flush_id) {
  VLOG(4) << fmt::format("{} Start FlushAsync chunk_flush_id: {}", UUID(),
                         chunk_flush_id);

  FlushTask* flush_task{nullptr};
  Status error_status;
  uint64_t slice_count = 0;

  {
    std::lock_guard<std::mutex> lg(mutex_);
    slice_count = slices_.size();
    error_status = error_status_;

    if (error_status.ok()) {
      flush_queue_.emplace_back(new FlushTask{
          .chunk_flush_id = chunk_flush_id,
          .status = Status::OK(),
          .cb = std::move(cb),
          .chunk = shared_from_this(),
      });

      flush_task = flush_queue_.back();
      flush_task->chunk_flush_task = std::make_unique<ChunkFlushTask>(
          chunk_.ino, chunk_.index, chunk_flush_id, std::move(slices_));
    }  // end if error_status.ok()
    //  not ok pass throuth
  }

  if (!error_status.ok()) {
    LOG(WARNING) << fmt::format(
        "{} End FlushAsync because error already happend, chunk_flush_id: "
        "{}, status: {}",
        UUID(), chunk_flush_id, error_status.ToString());

    cb(error_status);
    return;
  }

  CHECK_NOTNULL(flush_task);

  VLOG(1) << fmt::format(
      "{} FlushAsync will run chunk_flush_task: {} slice_count: {}, "
      "flush_task_addr: {}",
      UUID(), flush_task->ToString(), slice_count,
      static_cast<const void*>(flush_task));

  flush_task->chunk_flush_task->RunAsync([this, flush_task](auto&& ph1) {
    FlushTaskDone(flush_task, std::forward<decltype(ph1)>(ph1));
  });

  VLOG(4) << fmt::format("End FlushAsync chunk_flush_id: {}", chunk_flush_id);
}

void ChunkWriter::FlushAsync(StatusCallback cb) {
  uint64_t chunk_flush_id =
      chunk_flush_id_gen.fetch_add(1, std::memory_order_relaxed);
  DoFlushAsync(cb, chunk_flush_id);
}

void ChunkWriter::TriggerFlush() {
  uint64_t chunk_flush_id =
      chunk_flush_id_gen.fetch_add(1, std::memory_order_relaxed);
  VLOG(4) << fmt::format("{} TriggerFlush Start chunk_flush_id: {}", UUID(),
                         chunk_flush_id);

  std::string uuid = UUID();
  DoFlushAsync(
      [chunk_flush_id, uuid](Status s) {
        if (!s.ok()) {
          LOG(WARNING) << fmt::format(
              "{} TriggerFlush Fail chunk_flush_id: {} status: {}", uuid,
              chunk_flush_id, s.ToString());
        } else {
          VLOG(4) << fmt::format(
              "{} TriggerFlush End successfully, chunk_flush_id: {}", uuid,
              chunk_flush_id);
        }
      },
      chunk_flush_id);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
