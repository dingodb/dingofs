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

#include <absl/cleanup/cleanup.h>
#include <fmt/base.h>
#include <fmt/format.h>
#include <glog/logging.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <utility>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "client/common/utils.h"
#include "client/vfs/common/config.h"
#include "client/vfs/data/background/chunk_flush_task.h"
#include "client/vfs/data/common.h"
#include "client/vfs/data/data_utils.h"
#include "client/vfs/data/slice/common.h"
#include "client/vfs/data/slice/slice_data.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/vfs_meta.h"
#include "common/callback.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::atomic<uint64_t> slice_seq_id_gen{1};
static std::atomic<uint64_t> chunk_flush_id_gen{1};

// proteted by mutex_
struct Chunk::FlushTask {
  const uint64_t chunk_flush_id{0};
  bool done{false};
  Status status;
  const StatusCallback cb{nullptr};
  std::unique_ptr<ChunkFlushTask> chunk_flush_task;

  std::string UUID() const {
    CHECK_NOTNULL(chunk_flush_task);
    return chunk_flush_task->UUID();
  }

  std::string ToString() const {
    std::ostringstream oss;
    oss << "{ chunk_flush_id: " << chunk_flush_id
        << ", done: " << (done ? "true" : "false")
        << ", status: " << status.ToString() << ", task: "
        << (chunk_flush_task ? chunk_flush_task->ToString() : "nullptr")
        << " }";
    return oss.str();
  }
};

static Chunk::FlushTask kFakeHeader;

// protected by mutex_
Chunk::Chunk(VFSHub* hub, uint64_t ino, uint64_t index)
    : hub_(hub),
      ino_(ino),
      index_(index),
      fs_id_(hub->GetFsInfo().id),
      chunk_size_(hub->GetFsInfo().chunk_size),
      block_size_(hub->GetFsInfo().block_size),
      page_size_(hub->GetPageSize()),
      chunk_start_(index * chunk_size_),
      chunk_end_(chunk_start_ + chunk_size_) {}

Status Chunk::WriteToBlockCache(const cache::BlockKey& key,
                                const cache::Block& block,
                                cache::PutOption option) {
  return hub_->GetBlockCache()->Put(key, block, option);
}

Status Chunk::AllockChunkId(uint64_t* chunk_id) {
  return hub_->GetMetaSystem()->NewSliceId(ino_, chunk_id);
}

Status Chunk::CommitSlices(const std::vector<Slice>& slices) {
  return hub_->GetMetaSystem()->WriteSlice(ino_, index_, slices);
}

Status Chunk::DirectWrite(const char* buf, uint64_t size,
                          uint64_t chunk_offset) {
  uint64_t write_file_offset = chunk_start_ + chunk_offset;
  uint64_t end_write_file_offset = write_file_offset + size;

  uint64_t end_write_chunk_offset = chunk_offset + size;

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) Start DirectWrite buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_write_chunk_offset,
      write_file_offset, end_write_file_offset);

  CHECK_GE(chunk_end_, end_write_file_offset);

  const char* buf_pos = buf;

  // TODO: refact this code
  uint64_t chunk_id;
  DINGOFS_RETURN_NOT_OK(AllockChunkId(&chunk_id));

  uint64_t block_offset = chunk_offset % block_size_;
  uint64_t block_index = chunk_offset / block_size_;

  uint64_t remain_len = size;

  while (remain_len > 0) {
    uint64_t write_size = std::min(remain_len, block_size_ - block_offset);
    cache::BlockKey key(fs_id_, ino_, chunk_id, block_index, 0);
    cache::Block block(buf_pos, write_size);

    VLOG(4) << "ChunkWrite ino: " << ino_ << ", index: " << index_
            << ", block_key: " << key.StoreKey()
            << ", buf: " << Char2Addr(buf_pos)
            << ", write_size: " << write_size;
    WriteToBlockCache(key, block,
                      cache::PutOption());  // TODO: consider writeback

    remain_len -= write_size;
    buf_pos += write_size;
    block_offset = 0;
    ++block_index;
  }

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) End DirectWrite buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_write_chunk_offset,
      write_file_offset, end_write_file_offset);

  Slice slice{chunk_id, (chunk_start_ + chunk_offset), size, 0, false, size};
  VLOG(4) << "DirectWrite ino: " << ino_ << ", index: " << index_
          << ", slice: " << Slice2Str(slice);

  std::vector<Slice> slices;
  slices.push_back(slice);

  return CommitSlices(slices);
}

// TODO: maybe this algorithm is not good enough
SliceData* Chunk::FindWritableSliceUnLocked(uint64_t chunk_pos, uint64_t size) {
  uint64_t end_in_chunk = chunk_pos + size;

  //   from new to old
  for (auto it = slices_.rbegin(); it != slices_.rend(); ++it) {
    uint64_t seq = it->first;
    SliceData* slice = it->second.get();
    DCHECK_NOTNULL(slice);

    VLOG(6) << fmt::format(
        "(ino:{}, chunk_index:{}) FindWritableSliceUnLocked for chunk_range: "
        "[{}-{}], size: {}, seq: {}, check slice: {}",
        ino_, index_, chunk_pos, end_in_chunk, size, seq, slice->ToString());

    // if overlap with slice, then use new slice
    if (chunk_pos < slice->End() && end_in_chunk > slice->ChunkOffset()) {
      VLOG(6) << fmt::format(
          "(ino:{}, chunk_index:{}) End FindWritableSliceUnLocked because "
          "slice overlaps with chunk_range: "
          "[{}-{}], size: {}, seq: {}, slice: {}",
          ino_, index_, chunk_pos, end_in_chunk, size, seq, slice->ToString());

      return nullptr;
    }

    if (chunk_pos == slice->End() || end_in_chunk == slice->ChunkOffset()) {
      VLOG(6) << fmt::format(
          "(ino:{}, chunk_index:{}) Found slice for chunk_range: "
          "[{}-{}], size: {}, seq: {}, slice: {}",
          ino_, index_, chunk_pos, end_in_chunk, size, seq, slice->ToString());
      return slice;
    }
  }

  return nullptr;
}

SliceData* Chunk::CreateSliceUnlocked(uint64_t chunk_pos) {
  // Use static because chunk with same index may be deleted and recreated
  uint64_t seq = slice_seq_id_gen.fetch_add(1, std::memory_order_relaxed);
  SliceDataContext ctx(fs_id_, ino_, index_, seq, chunk_size_, block_size_,
                       page_size_);

  slices_.emplace(seq, std::make_unique<SliceData>(ctx, hub_, chunk_pos));

  SliceData* data = slices_[seq].get();

  VLOG(4) << fmt::format("Created new slice with seq: {}, slice: {}", seq,
                         data->ToString());

  return data;
}

SliceData* Chunk::FindOrCreateSliceUnlocked(uint64_t chunk_pos, uint64_t size) {
  SliceData* slice = FindWritableSliceUnLocked(chunk_pos, size);
  if (slice == nullptr) {
    slice = CreateSliceUnlocked(chunk_pos);
  }
  DCHECK_NOTNULL(slice);
  return slice;
}

Status Chunk::BufferWrite(const char* buf, uint64_t size,
                          uint64_t chunk_offset) {
  uint64_t write_file_offset = chunk_start_ + chunk_offset;
  uint64_t end_write_file_offset = write_file_offset + size;

  uint64_t end_write_chunk_offset = chunk_offset + size;

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) Start BufferWrite buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_write_chunk_offset,
      write_file_offset, end_write_file_offset);

  CHECK_GE(chunk_end_, end_write_file_offset);

  Status s;

  bool is_full = false;
  {
    std::lock_guard<std::mutex> lg(mutex_);

    SliceData* slice = FindOrCreateSliceUnlocked(chunk_offset, size);
    s = slice->Write(buf, size, chunk_offset);

    if (slice->Len() == chunk_size_) {
      is_full = true;
    }
  }

  if (is_full) {
    VLOG(4) << fmt::format(
        "(ino:{}, chunk_index:{}) Slice is full, triggering flush", ino_,
        index_);
    TriggerFlush();
  }

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) End BufferWrite buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_write_chunk_offset,
      write_file_offset, end_write_file_offset);

  return s;
}

Status Chunk::Write(const char* buf, uint64_t size, uint64_t chunk_offset) {
  {
    Status tmp;
    {
      std::lock_guard<std::mutex> lg(mutex_);
      if (!error_status_.ok()) {
        tmp = error_status_;
      }
    }
    if (!tmp.ok()) {
      LOG(WARNING) << fmt::format(
          "Chunk::Write failed, ino: {}, index: {}, status: {}", ino_, index_,
          tmp.ToString());
      return tmp;
    }
  }

  if (FLAGS_data_use_direct_write) {
    return DirectWrite(buf, size, chunk_offset);
  } else {
    return BufferWrite(buf, size, chunk_offset);
  }
}

Status Chunk::Read(char* buf, uint64_t size, uint64_t chunk_offset) {
  {
    Status tmp;
    {
      std::lock_guard<std::mutex> lg(mutex_);
      if (!error_status_.ok()) {
        tmp = error_status_;
      }
    }
    if (!tmp.ok()) {
      LOG(WARNING) << fmt::format(
          "Chunk::Write failed, ino: {}, index: {}, status: {}", ino_, index_,
          tmp.ToString());
      return tmp;
    }
  }

  uint64_t read_file_offset = chunk_start_ + chunk_offset;
  uint64_t end_read_file_offset = read_file_offset + size;

  uint64_t end_read_chunk_offet = chunk_offset + size;

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) Start ChunkRead buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_read_chunk_offet,
      read_file_offset, end_read_file_offset);

  CHECK_GE(chunk_end_, end_read_file_offset);

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
    cache::BlockKey key(fs_id_, ino_, block_req.block.slice_id,
                        block_req.block.index, block_req.block.version);

    char* buf_pos = buf + (block_req.block.file_offset +
                           block_req.block_offset - read_file_offset);

    VLOG(4) << "ChunkRead ino: " << ino_ << ", chunk_index: " << index_
            << ", block_key: " << key.StoreKey()
            << ", block_offset: " << block_req.block_offset
            << ", read_size: " << block_req.len
            << ", buf: " << Char2Addr(buf_pos);

    IOBuffer buffer;
    cache::RangeOption option;
    option.retrive = true;
    option.block_size = block_req.block.block_len;

    DINGOFS_RETURN_NOT_OK(hub_->GetBlockCache()->Range(
        key, block_req.block_offset, block_req.len, &buffer, option));
    buffer.CopyTo(buf_pos);
  }

  VLOG(4) << fmt::format(
      "(ino:{}, chunk_index:{}) End ChunkRead buf: {}, size: {}"
      ", chunk_range: [{}-{}], file_range: [{}-{}]",
      ino_, index_, Char2Addr(buf), size, chunk_offset, end_read_chunk_offet,
      read_file_offset, end_read_file_offset);

  return Status::OK();
}

void Chunk::FlushTaskDone(FlushTask* flush_task, Status s) {
  if (!s.ok()) {
    LOG(WARNING) << fmt::format(
        "Chunk::FlushTaskDone Failed task: {}, status: {}",
        flush_task->chunk_flush_task->ToString(), s.ToString());
  }

  {
    std::lock_guard<std::mutex> lg(mutex_);
    flush_task->status = s;
    flush_task->done = true;

    if (flush_queue_.front() != flush_task) {
      VLOG(4) << fmt::format(
          "Chunk::FlushTaskDone task: {} is not the header of flush_queue_, "
          "end directly",
          flush_task->ToString());

      return;
    } else {
      VLOG(4) << fmt::format(
          "Chunk::FlushTaskDone header_task: {} of the flush_queue_, "
          "flush_queue size: {}, insert fake header: {}, ",
          flush_task->ToString(), flush_queue_.size(),
          static_cast<const void*>(&kFakeHeader));

      flush_queue_.push_front(&kFakeHeader);
    }
  }

  // only the first flush task in the queue can be processed
  // may use another commit thread or make meta is async

  std::vector<FlushTask*> to_destroy;

  auto defer_destory = ::absl::MakeCleanup([&]() {
    for (FlushTask* task : to_destroy) {
      VLOG(4) << fmt::format("Chunk::FlushTaskDone delete task: {}",
                             task->ToString());
      delete task;
    }
  });

  while (true) {
    std::vector<FlushTask*> to_commit;

    {
      std::lock_guard<std::mutex> lg(mutex_);
      CHECK_GT(flush_queue_.size(), 0);

      auto it = flush_queue_.begin();
      ++it;  // from second element, first is fake header

      while (it != flush_queue_.end()) {
        VLOG(4) << fmt::format(
            "Chunk::FlushTaskDone header_task: {} try to commit task: {}",
            flush_task->UUID(), (*it)->ToString());

        // sequence iterate the flush queue, only pick the flush task which is
        // done
        if ((*it)->done) {
          to_commit.push_back(*it);
          // erase current element, and move to next
          it = flush_queue_.erase(it);
        } else {
          break;
        }
      };

      if (to_commit.empty()) {
        flush_queue_.pop_front();
        VLOG(4) << fmt::format(
            "Chunk::FlushTaskDone header_task: {} will return because has no "
            "flush_task to commit, fake header is removed, remain "
            "flush_queue_size: {}",
            flush_task->UUID(), flush_queue_.size());
        return;
      }
    }  //  end lock_guard

    CHECK(!to_commit.empty());

    for (FlushTask* task : to_commit) {
      VLOG(4) << fmt::format(
          "Chunk::FlushTaskDone header_task: {} commit flush_task: {}",
          flush_task->UUID(), task->ToString());

      if (task->status.ok()) {
        std::vector<Slice> slices;
        task->chunk_flush_task->GetCommitSlices(slices);

        // TODO: maybe use batch commit
        Status status = hub_->GetMetaSystem()->WriteSlice(ino_, index_, slices);
        if (!status.ok()) {
          LOG(WARNING) << fmt::format(
              "Chunk::FlushTaskDone header_task: {} fail commit task: {}, "
              "commit_status: {}",
              flush_task->UUID(), task->ToString(), status.ToString());

          MarkErrorStatus(status);
        }
      } else {
        LOG(WARNING) << fmt::format(
            "Chunk::FlushTaskDone header_task: {} skip commit flush fail "
            "task: {}",
            flush_task->UUID(), task->ToString());

        MarkErrorStatus(task->status);
      }

      // if some error happend before
      task->cb(GetErrorStatus());

      to_destroy.push_back(task);
    }  // end  for to_commit
  }  // end while(true)
}

void Chunk::DoFlushAsync(StatusCallback cb, uint64_t chunk_flush_id) {
  VLOG(4) << fmt::format(
      "Chunk::FlushAsync start chunk_flush_id: {}, ino: {}, index: {}",
      chunk_flush_id, ino_, index_);

  FlushTask* flush_task{nullptr};
  Status error_status;
  uint64_t slice_count = 0;

  {
    std::lock_guard<std::mutex> lg(mutex_);
    error_status = error_status_;
    if (error_status.ok()) {
      flush_queue_.emplace_back(new FlushTask{
          .chunk_flush_id = chunk_flush_id,
          .status = Status::OK(),
          .cb = std::move(cb),
      });

      flush_task = flush_queue_.back();

      slice_count = slices_.size();

      flush_task->chunk_flush_task = std::make_unique<ChunkFlushTask>(
          ino_, index_, chunk_flush_id, std::move(slices_));
    }  // end if error_status.ok()

    //  not ok pass throuth
  }

  if (!error_status.ok()) {
    LOG(WARNING) << fmt::format(
        "Chunk::FlushAsync end because error already happend, chunk_flush_id: "
        "{}, ino: {}, "
        "index: {}, status: {}",
        chunk_flush_id, ino_, index_, error_status.ToString());

    cb(error_status);
    return;
  }

  VLOG(1) << fmt::format(
      "Chunk::FlushAsync will run flush_task: {} ino: {}, "
      "index: {}, slice_count: {}",
      flush_task->ToString(), ino_, index_, slice_count);

  CHECK_NOTNULL(flush_task);

  flush_task->chunk_flush_task->RunAsync([this, flush_task](auto&& ph1) {
    FlushTaskDone(flush_task, std::forward<decltype(ph1)>(ph1));
  });

  VLOG(4) << fmt::format("Chunk::FlushAsync end chunk_flush_id: {}",
                         chunk_flush_id);
}

void Chunk::FlushAsync(StatusCallback cb) {
  uint64_t chunk_flush_id =
      chunk_flush_id_gen.fetch_add(1, std::memory_order_relaxed);
  DoFlushAsync(cb, chunk_flush_id);
}

void Chunk::TriggerFlush() {
  uint64_t chunk_flush_id =
      chunk_flush_id_gen.fetch_add(1, std::memory_order_relaxed);
  VLOG(4) << fmt::format(
      "Chunk::TriggerFlush start chunk_flush_id: {}, ino: {}, index: {}",
      chunk_flush_id, ino_, index_);

  DoFlushAsync(
      [chunk_flush_id](Status s) {
        if (!s.ok()) {
          LOG(WARNING) << fmt::format(
              "Chunk::TriggerFlush fail chunk_flush_id: {} status: {}",
              chunk_flush_id, s.ToString());
        } else {
          VLOG(4) << fmt::format(
              "Chunk::TriggerFlush end successfully, chunk_flush_id: {}",
              chunk_flush_id);
        }
      },
      chunk_flush_id);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs