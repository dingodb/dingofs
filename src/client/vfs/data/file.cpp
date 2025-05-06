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

#include "client/vfs/data/file.h"

#include <cstdint>

#include "common/status.h"
#include "client/common/utils.h"
#include "client/vfs/data/chunk.h"
#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {

uint64_t File::GetChunkSize() const { return vfs_hub_->GetFsInfo().chunk_size; }

Chunk* File::GetOrCreateChunk(uint64_t chunk_index) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto iter = chunks_.find(chunk_index);
  if (iter != chunks_.end()) {
    return iter->second.get();
  } else {
    auto chunk_writer = std::make_unique<Chunk>(vfs_hub_, ino_, chunk_index);
    chunks_[chunk_index] = std::move(chunk_writer);
    return chunks_[chunk_index].get();
  }
}

Status File::Write(const char* buf, uint64_t size, uint64_t offset,
                   uint64_t* out_wsize) {
  uint64_t chunk_size = GetChunkSize();

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

    Chunk* chunk = GetOrCreateChunk(chunk_index);
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

// TODO : concurrent read
Status File::Read(char* buf, uint64_t size, uint64_t offset,
                  uint64_t* out_rsize) {
  Attr attr;
  vfs_hub_->GetMetaSystem()->GetAttr(ino_, &attr);

  if (attr.length <= offset) {
    *out_rsize = 0;
    return Status::OK();
  }

  uint64_t chunk_size = GetChunkSize();

  uint64_t chunk_index = offset / chunk_size;
  uint64_t chunk_offset = offset % chunk_size;

  uint64_t total_read_size = std::min(size, attr.length - offset);
  uint64_t has_read = 0;

  while (total_read_size > 0) {
    uint64_t read_size = std::min(total_read_size, chunk_size - chunk_offset);

    Chunk* chunk = GetOrCreateChunk(chunk_index);
    DINGOFS_RETURN_NOT_OK(chunk->Read(buf, read_size, chunk_offset));

    buf += read_size;
    total_read_size -= read_size;

    has_read += read_size;

    offset += read_size;
    chunk_index = offset / chunk_size;
    chunk_offset = offset % chunk_size;
  }

  *out_rsize = has_read;
  return Status::OK();
}

}  // namespace vfs

}  // namespace client

}  // namespace dingofs