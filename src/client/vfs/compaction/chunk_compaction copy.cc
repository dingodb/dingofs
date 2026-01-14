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

#include "client/vfs/compaction/chunk_compaction.h"

namespace dingofs {
namespace client {
namespace vfs {

Status CompactionManager::CompactChunk(Ino ino, uint64_t chunk_index) {
  uint64_t fh = 0;
  std::vector<Slice> slices;
  uint64_t version;

  Status s = vfs_hub_->GetMetaSystem()->ReadSlice(nullptr, ino, chunk_index, fh,
                                                  &slices, version);
  if (!s.ok()) {
    return s;
  }

  if (slices.size() <= 1) {
    return Status::OK();
  }

  FileRange frange{
      .offset =
          static_cast<int64_t>(chunk_index * vfs_hub_->GetFsInfo().chunk_size),
      .len = static_cast<int64_t>(vfs_hub_->GetFsInfo().chunk_size)};

  ChunkReq req{
      .ino = ino, .index = (int64_t)chunk_index, .offset = 0, .frange = frange};

  ChunkReader reader(vfs_hub_, fh, req);

  absl::BlockingCounter bc_read(1);
  Status read_status;
  reader.ReadAsync(nullptr, [&](Status s) {
    read_status = s;
    bc_read.DecrementCount();
  });
  bc_read.Wait();

  if (!read_status.ok()) {
    return read_status;
  }

  IOBuffer data = reader.GetDataBuffer();
  if (data.Size() == 0) {
    return Status::OK();
  }

  // use slice data
  auto fs_info = vfs_hub_->GetFsInfo();
  auto page_size = vfs_hub_->GetWriteBufferManager()->GetPageSize();

  SliceDataContext ctx(fs_info.id, ino, chunk_index, fs_info.chunk_size,
                       fs_info.block_size, page_size);

  auto slice_data = std::make_unique<SliceData>(ctx, vfs_hub_, 0);

  auto iovs = data.Fetch();
  uint64_t write_offset = 0;

  for (const auto& iov : iovs) {
    Status write_s = slice_data->Write(nullptr, (const char*)iov.iov_base,
                                       iov.iov_len, write_offset);
    if (!write_s.ok()) {
      return write_s;
    }
    write_offset += iov.iov_len;
  }

  absl::BlockingCounter bc_flush(1);
  Status flush_status;
  slice_data->FlushAsync([&](Status s) {
    flush_status = s;
    bc_flush.DecrementCount();
  });
  bc_flush.Wait();

  if (!flush_status.ok()) {
    return flush_status;
  }

  Slice new_slice = slice_data->GetCommitSlice();
  std::vector<Slice> new_slices = {new_slice};

  s = vfs_hub_->GetMetaSystem()->WriteSlice(nullptr, ino, chunk_index, fh,
                                            new_slices);
  return s;
}

void CompactionManager::CompactChunkAsync(Ino ino, uint64_t chunk_index,
                                          StatusCallback cb) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (inflight_tasks_.count({ino, chunk_index}) > 0) {
      if (cb) {
        cb(Status::OK());
      }
      return;
    }
    inflight_tasks_.insert({ino, chunk_index});
  }

  CompactionManager::CompactionTask task;
  task.ino = ino;
  task.chunk_index = chunk_index;
  task.cb = cb;

  if (bthread::execution_queue_execute(compaction_queue_, task) != 0) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      inflight_tasks_.erase({ino, chunk_index});
    }
    if (cb) {
      cb(Status::Internal("Failed to execute compaction task"));
    }
  }
}

int CompactionManager::HandleCompactionTask(
    void* meta,
    bthread::TaskIterator<CompactionManager::CompactionTask>& iter) {
  CompactionManager* self = static_cast<CompactionManager*>(meta);
  if (iter.is_queue_stopped()) {
    return 0;
  }

  for (; iter; ++iter) {
    CompactionManager::CompactionTask& task = *iter;

    Status s = self->CompactChunk(task.ino, task.chunk_index);
    if (!s.ok()) {
      LOG(WARNING) << "CompactChunk failed ino: " << task.ino
                   << " index: " << task.chunk_index
                   << " status: " << s.ToString();
    } else {
      LOG(INFO) << "CompactChunk success ino: " << task.ino
                << " index: " << task.chunk_index;
    }

    {
      std::lock_guard<std::mutex> lock(self->mutex_);
      self->inflight_tasks_.erase({task.ino, task.chunk_index});
    }

    if (task.cb) {
      task.cb(s);
    }
  }
  return 0;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
