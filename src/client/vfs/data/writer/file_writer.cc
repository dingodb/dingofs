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

#include <glog/logging.h>

#include "client/common/utils.h"
#include "client/vfs/data/writer/chunk_writer.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

static std::atomic<uint64_t> file_flush_id_gen{1};

Status FileWriter::Write(const char* buf, uint64_t size, uint64_t offset,
                         uint64_t* out_wsize) {
  uint64_t chunk_size = GetChunkSize();
  CHECK(chunk_size > 0) << "chunk size not allow 0";

  uint64_t chunk_index = offset / chunk_size;
  uint64_t chunk_offset = offset % chunk_size;

  VLOG(3) << "File::Write, ino: " << ino_ << ", buf: " << Char2Addr(buf)
          << ", size: " << size << ", offset: " << offset
          << ", chunk_size: " << chunk_size;

  const char* pos = buf;

  Status s;
  uint64_t written_size = 0;

  while (size > 0) {
    uint64_t write_size = std::min(size, chunk_size - chunk_offset);

    ChunkWriter* chunk = GetOrCreateChunkWriter(chunk_index);
    s = chunk->Write(pos, write_size, chunk_offset);
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
    return iter->second.get();
  } else {
    auto chunk_writer =
        std::make_shared<ChunkWriter>(vfs_hub_, ino_, chunk_index);
    chunk_writers_[chunk_index] = std::move(chunk_writer);
    return chunk_writers_[chunk_index].get();
  }
}

void FileWriter::AsyncFlush(StatusCallback cb) {
  uint64_t file_flush_id = file_flush_id_gen.fetch_add(1);
  VLOG(3) << "File::AsyncFlush start ino: " << ino_
          << ", file_flush_id: " << file_flush_id;

  FileFlushTask* flush_task{nullptr};
  bool is_empty = false;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    uint64_t chunk_count = chunk_writers_.size();
    if (chunk_count == 0) {
      is_empty = true;
    } else {
      // TODO: maybe we only need chunk index
      // copy chunk_writers_
      auto flush_task_unique_ptr =
          std::make_unique<FileFlushTask>(ino_, file_flush_id, chunk_writers_);
      flush_task = flush_task_unique_ptr.get();

      CHECK(inflight_flush_tasks_
                .emplace(file_flush_id, std::move(flush_task_unique_ptr))
                .second);
    }
  }

  if (is_empty) {
    VLOG(1) << "File::AsyncFlush end ino: " << ino_
            << ", file_flush_id: " << file_flush_id
            << ", no chunks to flush, calling callback directly";
    cb(Status::OK());
    return;
  }

  CHECK_NOTNULL(flush_task);
  flush_task->RunAsync(cb);

  VLOG(3) << "File::AsyncFlush end ino: " << ino_
          << ", file_flush_id: " << file_flush_id;
}
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
