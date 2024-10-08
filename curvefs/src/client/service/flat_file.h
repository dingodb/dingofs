// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CURVEFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_
#define CURVEFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_

#include <cstdint>
#include <iomanip>
#include <map>
#include <string>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/blockcache/cache_store.h"
#include "glog/logging.h"

namespace curvefs {
namespace client {

using ::curvefs::metaserver::S3ChunkInfo;

class FlatFile;

struct FlatFileSlice {
  uint64_t file_offset;
  uint64_t len;
  uint64_t chunk_id;

  std::string ToString() const {
    return "(file_offset:" + std::to_string(file_offset) +
           ", len:" + std::to_string(len) +
           ", chunk_id:" + std::to_string(chunk_id) + ")";
  }
};

class FlatFileChunk {
 public:
  FlatFileChunk() = default;

  ~FlatFileChunk() = default;

  void InsertChunkInfo(const FlatFileSlice& new_slice);

  std::string ToString() const {
    std::ostringstream os;
    for (const auto& slice : file_offset_slice_) {
      os << "file_offset: " << slice.first << slice.second.ToString() << "\n";
    }
    return os.str();
  }

  const std::map<uint64_t, FlatFileSlice>& GetFileOffsetSlice() const {
    return file_offset_slice_;
  }

 private:
  friend class FlatFile;
  std::map<uint64_t, FlatFileSlice> file_offset_slice_;
};

class FlatFile {
 public:
  FlatFile(uint64_t fs_id, uint64_t ino) : fs_id_(fs_id), ino_(ino) {}

  ~FlatFile() = default;

  void InsertChunkInfo(uint64_t chunk_index, const S3ChunkInfo& chunk_info) {
    chunk_id_to_chunk_info_[chunk_info.chunkid()] = chunk_info;

    FlatFileSlice slice = {chunk_info.offset(), chunk_info.len(),
                           chunk_info.chunkid()};
    chunk_index_flat_file_chunk_[chunk_index].InsertChunkInfo(slice);
  }

  std::string ToString() const {
    std::ostringstream os;
    os << "chunk_id_to_chunk_info_:\n";
    for (const auto& chunk : chunk_id_to_chunk_info_) {
      os << "chunk_id: " << chunk.first
         << ", chunk_info: " << chunk.second.DebugString();
    }

    os << "\n chunk_index_flat_file_chunk_:\n";
    for (const auto& chunk : chunk_index_flat_file_chunk_) {
      os << "chunk_index: " << chunk.first << chunk.second.ToString();
    }
    return os.str();
  }

  std::string FormatStringWithHeader() const {
    std::ostringstream os;
    os << std::left << std::setw(20) << "file_offset" << std::setw(20) << "len"
       << std::setw(20) << "block_offset" << std::setw(40) << "block_name"
       << std::setw(20) << "block_len" << std::setw(20) << "block_size"
       << std::setw(4) << "zero" << "\n";

    for (const auto& flat_file_chunk_iter : chunk_index_flat_file_chunk_) {
      uint64_t chunk_index = flat_file_chunk_iter.first;
      const FlatFileChunk& flat_file_chunk = flat_file_chunk_iter.second;

      for (const auto& flat_file_slice_iter :
           flat_file_chunk.file_offset_slice_) {
        const FlatFileSlice& flat_file_slice = flat_file_slice_iter.second;

        auto iter = chunk_id_to_chunk_info_.find(flat_file_slice.chunk_id);
        CHECK(iter != chunk_id_to_chunk_info_.end())
            << "chunk_id: " << flat_file_slice.chunk_id << " not found";

        const S3ChunkInfo& chunk_info = iter->second;

        uint64_t block_offset =
            flat_file_slice.file_offset - chunk_info.offset();
        blockcache::BlockKey key(fs_id_, ino_, chunk_info.chunkid(),
                                 chunk_index, chunk_info.compaction());

        os << std::left << std::setw(20) << flat_file_slice.file_offset
           << std::setw(20) << flat_file_slice.len << std::setw(20)
           << block_offset << std::setw(40) << key.StoreKey() << std::setw(20)
           << chunk_info.len() << std::setw(20) << chunk_info.size()
           << std::setw(4) << (chunk_info.zero() ? "true" : "false") << "\n";
      }
    }

    return os.str();
  }

 private:
  uint64_t fs_id_;  // filesystem id
  uint64_t ino_;    // inode id
  std::map<uint64_t, S3ChunkInfo> chunk_id_to_chunk_info_;
  std::map<uint64_t, FlatFileChunk> chunk_index_flat_file_chunk_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_