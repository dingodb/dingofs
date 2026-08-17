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

#include "client/vfs/data/writer/file_writer.h"

#include <bvar/reducer.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/common/async_util.h"
#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/writemempool/write_mem_pool.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("FileWriter::" + std::string(__FUNCTION__))

static std::atomic<uint64_t> file_flush_id_gen{1};

namespace {

bvar::Adder<int64_t> g_close_after_flush_failure_num(
    "vfs_file_writer_close_after_flush_failure_num");

size_t PageNeed(uint64_t offset, uint64_t size, uint64_t page_size) {
  CHECK_GT(page_size, 0);
  return static_cast<size_t>(((offset % page_size) + size + page_size - 1) /
                             page_size);
}

}  // namespace

FileWriter::~FileWriter() { Close(); }

Status FileWriter::Open() {
  VLOG(9) << fmt::format("{} FileWriter opened", uuid_);
  SchedulePeriodicFlush();
  return Status::OK();
}

void FileWriter::Close() {
  std::unique_lock<std::mutex> lg(mutex_);
  if (closed_) {
    LOG(INFO) << fmt::format("{} FileWriter already closed", uuid_);
    return;
  }

  closed_ = true;

  VLOG(9) << fmt::format("{} FileWriter closed ", uuid_);

  while (writers_count_ > 0 || !inflight_flush_tasks_.empty()) {
    VLOG(1) << fmt::format(
        "{} Close waiting, writers_count_: {}, inflight_flush_task_count_: {}",
        uuid_, writers_count_, inflight_flush_tasks_.size());
    cv_.wait(lg);
  }

  Status flush_status = GetStatus();
  if (flush_status.ok()) {
    CHECK_EQ(write_generation_, flushed_generation_)
        << fmt::format("{} Close found unflushed data", uuid_);
  } else {
    g_close_after_flush_failure_num << 1;
    LOG(ERROR) << fmt::format(
        "{} Close after flush failure, write_generation: {}, "
        "flushed_generation: {}, status: {}",
        uuid_, write_generation_, flushed_generation_, flush_status.ToString());
  }

  for (auto& pair : chunk_writers_) {
    ChunkWriter* chunk_writer = pair.second;
    chunk_writer->Stop();
    delete chunk_writer;
  }
}

void FileWriter::AcquireRef() {
  int64_t orgin = refs_.fetch_add(1);
  VLOG(12) << fmt::format("{} AcquireRef origin refs: {}", uuid_, orgin);
  CHECK_GE(orgin, 0);
}

void FileWriter::ReleaseRef() {
  std::string uuid = uuid_;
  int64_t orgin = refs_.fetch_sub(1);
  VLOG(12) << fmt::format("{} ReleaseRef origin refs: {}", uuid, orgin);
  CHECK_GT(orgin, 0);
  if (orgin == 1) {
    delete this;
  }
}

Status FileWriter::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                         uint64_t offset, uint64_t* out_wsize) {
  // Define the out-param on every path.
  *out_wsize = 0;
  DINGOFS_RETURN_NOT_OK(GetStatus());

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      return Status::BadFd("file already closed");
    } else {
      writers_count_++;
    }
  }

  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("FileWriter::Write",
                                                          ctx->GetTraceSpan());

  int32_t chunk_size = GetChunkSize();
  CHECK(chunk_size > 0) << "chunk size not allow 0";

  int64_t chunk_index = offset / chunk_size;
  int32_t chunk_offset = static_cast<int32_t>(offset % chunk_size);

  VLOG(3) << "File::Write, ino: " << ino_ << ", buf: " << Helper::Char2Addr(buf)
          << ", size: " << size << ", offset: " << offset
          << ", chunk_size: " << chunk_size;

  const char* pos = buf;

  Status s;
  uint64_t written_size = 0;
  WriteMemPool* pool = vfs_hub_->GetWriteMemPool();

  while (size > 0) {
    const int32_t write_size = static_cast<int32_t>(
        std::min(size, static_cast<uint64_t>(chunk_size - chunk_offset)));

    // Capacity admission is the only blocking point and happens before any
    // ChunkWriter/SliceWriter lock. One extra page covers worst-case
    // unaligned boundaries; unused pages return through the lease.
    WritePageLease lease;
    s = pool->Acquire(PageNeed(static_cast<uint64_t>(chunk_offset),
                               static_cast<uint64_t>(write_size),
                               static_cast<uint64_t>(pool->GetPageSize())),
                      &lease);
    if (!s.ok()) break;

    // Acquire may have waited while flush or shutdown changed writer state.
    s = GetStatus();
    if (!s.ok()) break;
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (closed_) {
        s = Status::BadFd("file already closed");
      }
    }
    if (!s.ok()) break;

    ChunkWriter* chunk = GetOrCreateChunkWriter(chunk_index);
    chunk->Write(SpanScope::GetContext(span), pos, write_size, chunk_offset,
                 &lease);

    // Publish every independently flushable chunk before the next Acquire can
    // block. Pressure flush uses these generations as its dirty predicate.
    {
      std::lock_guard<std::mutex> lock(mutex_);
      ++write_generation_;
    }
    pool->NotifyDirtyPublished();

    pos += write_size;
    size -= write_size;

    written_size += write_size;

    offset += write_size;
    chunk_index = static_cast<int64_t>(offset / chunk_size);
    chunk_offset = static_cast<int32_t>(offset % chunk_size);
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    writers_count_--;
    if (writers_count_ == 0) {
      cv_.notify_all();
    }
  }

  *out_wsize = written_size;

  // Partial progress is a POSIX short write, not a failure. A zero-byte result
  // surfaces the admission, sticky writer, or shutdown status.
  if (written_size > 0) {
    return Status::OK();
  }
  return s;
}

int32_t FileWriter::GetChunkSize() const {
  return vfs_hub_->GetFsInfo().chunk_size;
}

ChunkWriter* FileWriter::GetOrCreateChunkWriter(int64_t chunk_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto iter = chunk_writers_.find(chunk_index);
  if (iter != chunk_writers_.end()) {
    return iter->second;
  } else {
    auto* chunk_writer = new ChunkWriter(vfs_hub_, ino_, chunk_index);
    chunk_writers_[chunk_index] = chunk_writer;
    return chunk_writer;
  }
}

void FileWriter::FileFlushTaskDone(uint64_t file_flush_id,
                                   uint64_t target_generation,
                                   StatusCallback cb, Status status) {
  if (!status.ok()) {
    SetStatusIfBroken(status);
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    auto iter = inflight_flush_tasks_.find(file_flush_id);
    CHECK(iter != inflight_flush_tasks_.end());
    if (!status.ok()) {
      LOG(WARNING) << "File::AsyncFlush Failed, ino: " << ino_
                   << ", file_flush_id: " << file_flush_id
                   << ", flush_task: " << iter->second->ToString()
                   << ", status: " << status.ToString();
    }

    if (status.ok()) {
      flushed_generation_ = std::max(flushed_generation_, target_generation);
    }

    inflight_flush_tasks_.erase(iter);

    if (inflight_flush_tasks_.empty()) {
      cv_.notify_all();
    }
  }

  cb(status);
}

void FileWriter::AsyncFlush(StatusCallback cb) {
  uint64_t file_flush_id = file_flush_id_gen.fetch_add(1);
  VLOG(3) << "File::AsyncFlush start ino: " << ino_
          << ", file_flush_id: " << file_flush_id;

  FileFlushTask* flush_task{nullptr};
  uint64_t chunk_count = 0;
  uint64_t target_generation = 0;
  bool closed = false;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    target_generation = write_generation_;
    if (closed_) {
      closed = true;
      LOG(INFO) << fmt::format(
          "{} File::AsyncFlush skip becaue file already closed", uuid_);
    } else {
      chunk_count = chunk_writers_.size();
      if (chunk_count > 0) {
        // TODO: maybe we only need chunk index
        // copy chunk_writers_
        auto flush_task_unique_ptr = std::make_unique<FileFlushTask>(
            ino_, file_flush_id, chunk_writers_);
        flush_task = flush_task_unique_ptr.get();

        CHECK(inflight_flush_tasks_
                  .emplace(file_flush_id, std::move(flush_task_unique_ptr))
                  .second);
      }
    }
  }

  if (closed) {
    cb(Status::BadFd("file already closed"));
    return;
  }

  if (flush_task == nullptr) {
    VLOG(3) << fmt::format(
        "{} File::AsyncFlush end file_flush_id: {}, chunk_count: {} calling "
        "callback directly",

        uuid_, file_flush_id, chunk_count);
    // chunk_writers_ only grows after a successful write reaches a chunk.
    // Therefore an empty writer has no generation that needs flushing.
    DCHECK_EQ(target_generation, 0);
    cb(Status::OK());
    return;
  }

  CHECK_NOTNULL(flush_task);

  AcquireRef();
  flush_task->RunAsync([this, file_flush_id, target_generation,
                        rcb = std::move(cb)](Status status) {
    VLOG(3) << "File::AsyncFlush end ino: " << ino_
            << ", file_flush_id: " << file_flush_id
            << ", status: " << status.ToString();
    FileFlushTaskDone(file_flush_id, target_generation, rcb, std::move(status));
    ReleaseRef();
  });
}
void FileWriter::FlushDirtyAsync(StatusCallback cb) {
  bool dirty = false;
  bool closed = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    closed = closed_;
    dirty = write_generation_ > flushed_generation_;
  }
  if (closed) {
    cb(Status::BadFd("file already closed"));
  } else if (!dirty) {
    cb(Status::OK());
  } else {
    AsyncFlush(std::move(cb));
  }
}

Status FileWriter::Flush() {
  Status s;
  Synchronizer sync;
  AsyncFlush(sync.AsStatusCallBack(s));
  sync.Wait();
  return s;
}

Status FileWriter::GetStatus() const {
  std::lock_guard<std::mutex> lg(status_mutex_);
  return file_status_;
}

void FileWriter::SetStatusIfBroken(const Status& s) {
  if (s.ok()) return;
  std::lock_guard<std::mutex> lg(status_mutex_);
  if (file_status_.ok()) {
    file_status_ = s;
  }
}

void FileWriter::SchedulePeriodicFlush() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      LOG(INFO) << fmt::format("{} ScheduleFlush skipped because closed",
                               uuid_);
      return;
    }
  }

  AcquireRef();

  vfs_hub_->GetWriteBackgroundExecutor()->Schedule(
      [this] {
        RunPeriodicFlush();
        ReleaseRef();
      },
      FLAGS_vfs_periodic_flush_interval_ms);
}

void FileWriter::RunPeriodicFlush() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
      LOG(INFO) << fmt::format("{} RunPeriodicFlush skipped because closed",
                               uuid_);
      return;
    }
  }

  AsyncFlush([this](Status s) {
    if (!s.ok()) {
      LOG(ERROR) << fmt::format("{} RunPeriodicFlush failed, status: {}", uuid_,
                                s.ToString());
    }
  });

  SchedulePeriodicFlush();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
