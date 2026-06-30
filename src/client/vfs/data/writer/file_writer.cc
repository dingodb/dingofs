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
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>

#include "client/vfs/common/async_util.h"
#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/writemempool/write_mem_pool.h"

DEFINE_int64(vfs_stale_write_sleep_us, 10, "sleep us when detect memory low");

DEFINE_double(vfs_write_throttle_soft_ratio, 0.85,
              "write buffer usage ratio at which to start soft throttle");
DEFINE_double(vfs_write_throttle_hard_ratio, 0.95,
              "write buffer usage ratio at which to keep throttling until "
              "the flush worker drains it back below");
DEFINE_int64(vfs_write_throttle_budget_ms, 5000,
             "max time to block in hard throttle before returning NoSpace; "
             "bounds the wait so a stalled flush can't hang the FUSE syscall");

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("FileWriter::" + std::string(__FUNCTION__))

static std::atomic<uint64_t> file_flush_id_gen{1};

namespace {

// Throttle counters (lock-free per-thread Adder). Only bumped on the slow path
// (usage already past soft_ratio), so the normal write path never touches them.
bvar::Adder<int64_t> g_throttle_num("vfs_write_throttle_num");
bvar::Adder<int64_t> g_throttle_timeout_num("vfs_write_throttle_timeout_num");

// Memory-pressure throttle (pool-backed WriteMemPool, usage is hard-capped at
// 100% so the old `used > total` / `> 2*total` thresholds never fire). Now
// ratio-based with a soft pre-throttle:
//   usage < soft_ratio  -> pass;
//   usage >= soft_ratio -> sleep once to slow the writer;
//   usage >= hard_ratio -> keep sleeping (bounded by budget_ms) until the
//                          flush worker drains it back below.
// The soft pre-throttle keeps the pool from truly filling, which would make
// Allocate's bounded-acquire time out into ENOSPC. The wait is bounded so a
// stalled flush (S3 down) can't D-state the FUSE syscall forever -- on timeout
// we return NoSpace (-> ENOSPC) instead of blocking indefinitely.
Status ApplyWriteBufferThrottle(WriteMemPool* bm) {
  if (!bm->IsHighPressure(FLAGS_vfs_write_throttle_soft_ratio)) {
    return Status::OK();
  }

  g_throttle_num << 1;  // soft limit hit (slow path only)
  VLOG(1) << "WriteMemPool soft pressure, usage=" << bm->GetUsageRatio();
  std::this_thread::sleep_for(
      std::chrono::microseconds(FLAGS_vfs_stale_write_sleep_us));

  auto deadline = std::chrono::steady_clock::now() +
                  std::chrono::milliseconds(FLAGS_vfs_write_throttle_budget_ms);
  while (bm->IsHighPressure(FLAGS_vfs_write_throttle_hard_ratio)) {
    if (std::chrono::steady_clock::now() >= deadline) {
      g_throttle_timeout_num << 1;
      LOG(WARNING) << "WriteMemPool throttle timeout, usage="
                   << bm->GetUsageRatio() << " used=" << bm->GetUsedBytes()
                   << " total=" << bm->GetTotalBytes();
      return Status::NoSpace("write buffer throttle timeout");
    }
    std::this_thread::sleep_for(
        std::chrono::microseconds(10 * FLAGS_vfs_stale_write_sleep_us));
  }
  return Status::OK();
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

  if (!chunk_writers_.empty()) {
    uint64_t file_flush_id = file_flush_id_gen.fetch_add(1);
    auto flush_task =
        std::make_unique<FileFlushTask>(ino_, file_flush_id, chunk_writers_);

    Status s;
    Synchronizer sync;
    flush_task->RunAsync(sync.AsStatusCallBack(s));
    sync.Wait();

    if (!s.ok()) {
      LOG(ERROR) << fmt::format("{} Failed to close file, flush error: {}",
                                uuid_, s.ToString());
    }
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
  // Define the out-param on every path: the early-return error paths below
  // (sticky status / throttle timeout / closed) leave it 0 instead of relying
  // on the caller to pre-zero.
  *out_wsize = 0;

  // Sticky-broken fast-fail.
  DINGOFS_RETURN_NOT_OK(GetStatus());

  // Memory-pressure throttle (soft pre-throttle; bounded hard wait). On timeout
  // returns NoSpace -> ENOSPC instead of blocking the FUSE syscall forever.
  DINGOFS_RETURN_NOT_OK(ApplyWriteBufferThrottle(vfs_hub_->GetWriteMemPool()));

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

  while (size > 0) {
    int32_t write_size = static_cast<int32_t>(
        std::min(size, static_cast<uint64_t>(chunk_size - chunk_offset)));

    ChunkWriter* chunk = GetOrCreateChunkWriter(chunk_index);
    s = chunk->Write(SpanScope::GetContext(span), pos, write_size,
                     chunk_offset);
    if (!s.ok()) {
      // Detail-only: this chunk stopped the write (surfaces as a short write if
      // written_size > 0, else a hard error, decided after the loop). Don't
      // WARN per chunk -- it floods under back-pressure and misreads a
      // successful short write; the outcome is recorded uniformly in
      // VFSWrapper's access log (status + out_wsize), the pool-pressure
      // locality in SliceWriter, and the failure rate in the
      // vfs_write_pool_alloc_fail_num metric.
      VLOG(3) << "stop write at chunk, ino: " << ino_
              << ", chunk_index: " << chunk_index
              << ", chunk_offset: " << chunk_offset
              << ", write_size: " << write_size << ", status: " << s.ToString();
      break;
    }

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

  // Partial progress is a POSIX short write, not a failure. SliceWriter::Write
  // is atomic per chunk, so written_size is exactly the prefix of fully-written
  // chunks; report OK + that prefix so the caller advances and re-issues the
  // rest (which hits the throttle / backpressure). Only a zero-byte write
  // surfaces the error (e.g. the very first chunk got ENOSPC).
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

void FileWriter::FileFlushTaskDone(uint64_t file_flush_id, StatusCallback cb,
                                   Status status) {
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

  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (closed_) {
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

  if (flush_task == nullptr) {
    VLOG(3) << fmt::format(
        "{} File::AsyncFlush end file_flush_id: {}, chunk_count: {} calling "
        "callback directly",
        uuid_, file_flush_id, chunk_count);
    cb(Status::OK());
    return;
  }

  CHECK_NOTNULL(flush_task);

  AcquireRef();
  flush_task->RunAsync(
      [this, file_flush_id, rcb = std::move(cb)](Status status) {
        VLOG(3) << "File::AsyncFlush end ino: " << ino_
                << ", file_flush_id: " << file_flush_id
                << ", status: " << status.ToString();
        FileFlushTaskDone(file_flush_id, rcb, std::move(status));
        ReleaseRef();
      });
}

Status FileWriter::Flush() {
  Status s;
  Synchronizer sync;
  AsyncFlush(sync.AsStatusCallBack(s));
  sync.Wait();
  if (!s.ok()) {
    SetStatusIfBroken(s);
  }
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
