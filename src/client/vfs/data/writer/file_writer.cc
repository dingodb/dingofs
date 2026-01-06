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

#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "client/vfs/data/common/async_util.h"
#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

#define METHOD_NAME() ("FileWriter::" + std::string(__FUNCTION__))

static std::atomic<uint64_t> file_flush_id_gen{1};

FileWriter::~FileWriter() { Close(); }

void FileWriter::Close() {
  std::unique_lock<std::mutex> lg(mutex_);
  if (closed_) {
    return;
  }

  closed_ = true;

  while (writers_count_ > 0 || !inflight_flush_tasks_.empty()) {
    VLOG(1) << fmt::format(
        "{} File::Close waiting, ino: {}, writers_count_: {}, "
        "inflight_flush_task_count_: {}",
        uuid_, ino_, writers_count_, inflight_flush_tasks_.size());
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
      LOG(ERROR) << fmt::format(
          "{} Failed to close file, fh: {}, flush error: {}", uuid_, fh_,
          s.ToString());
    }
  }

  for (auto& pair : chunk_writers_) {
    ChunkWriter* chunk_writer = pair.second;
    chunk_writer->Stop();
    delete chunk_writer;
  }
}

Status FileWriter::Write(ContextSPtr ctx, const char* buf, uint64_t size,
                         uint64_t offset, uint64_t* out_wsize) {
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

  uint64_t chunk_size = GetChunkSize();
  CHECK(chunk_size > 0) << "chunk size not allow 0";

  uint64_t chunk_index = offset / chunk_size;
  uint64_t chunk_offset = offset % chunk_size;

  VLOG(3) << "File::Write, ino: " << ino_ << ", buf: " << Helper::Char2Addr(buf)
          << ", size: " << size << ", offset: " << offset
          << ", chunk_size: " << chunk_size;

  const char* pos = buf;

  Status s;
  uint64_t written_size = 0;

  while (size > 0) {
    uint64_t write_size = std::min(size, chunk_size - chunk_offset);

    ChunkWriter* chunk = GetOrCreateChunkWriter(chunk_index);
    s = chunk->Write(ctx, pos, write_size, chunk_offset);
    if (!s.ok()) {
      LOG(WARNING) << "Fail write chunk, ino: " << ino_
                   << ", chunk_index: " << chunk_index
                   << ", chunk_offset: " << chunk_offset
                   << ", write_size: " << write_size;
      break;
    }

    pos += write_size;
    size -= write_size;

    written_size += write_size;

    offset += write_size;
    chunk_index = offset / chunk_size;
    chunk_offset = offset % chunk_size;
  }

  {
    std::lock_guard<std::mutex> lock(mutex_);
    writers_count_--;
    if (writers_count_ == 0) {
      cv_.notify_all();
    }
  }

  *out_wsize = written_size;
  return s;
}

uint64_t FileWriter::GetChunkSize() const {
  return vfs_hub_->GetFsInfo().chunk_size;
}

ChunkWriter* FileWriter::GetOrCreateChunkWriter(uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto iter = chunk_writers_.find(chunk_index);
  if (iter != chunk_writers_.end()) {
    return iter->second;
  } else {
    auto* chunk_writer = new ChunkWriter(vfs_hub_, fh_, ino_, chunk_index);
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

  flush_task->RunAsync(
      [this, file_flush_id, rcb = std::move(cb)](Status status) {
        VLOG(3) << "File::AsyncFlush end ino: " << ino_
                << ", file_flush_id: " << file_flush_id
                << ", status: " << status.ToString();
        FileFlushTaskDone(file_flush_id, rcb, std::move(status));
      });
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
