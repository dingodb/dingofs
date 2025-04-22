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

#ifndef DINGOFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_
#define DINGOFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_

#include <cstdint>
#include <iomanip>
#include <map>
#include <sstream>
#include <string>

#include "cache/blockcache/cache_store.h"
#include "dingofs/metaserver.pb.h"
#include "glog/logging.h"

namespace dingofs {
namespace client {

class FlatFile;

using cache::blockcache::BlockKey;

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
      os << slice.second.ToString() << "\n";
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

struct BlockObj {
  uint64_t file_offset;
  uint64_t obj_len;
  bool zero;
  uint64_t version;
  uint64_t chunk_id;
  uint64_t block_index;

  std::string ToString() const {
    std::ostringstream os;
    os << "[file_offset: " << file_offset << ", obj_len: " << obj_len
       << ", zero: " << zero << ", version: " << version
       << ", chunk_id: " << chunk_id << ", block_index: " << block_index << "]";
    return os.str();
  }
};

struct BlockObjSlice {
  uint64_t file_offset;  // slice start offset
  uint64_t len;          // slice length
  BlockObj obj;          // block object
};

class S3ChunkHoler {
 public:
  S3ChunkHoler(const pb::metaserver::S3ChunkInfo& chunk_info,
               uint64_t chunk_size, uint64_t block_size)
      : chunk_info_(chunk_info),
        chunk_size_(chunk_size),
        block_size_(block_size) {
    Init();
  }

  ~S3ChunkHoler() = default;

  std::string ToString() const {
    std::ostringstream os;
    os << "S3ChunkInfo :" << chunk_info_.ShortDebugString() << "\n";

    for (const auto& block : offset_to_block_) {
      os << block.second.ToString() << "\n";
    }
    return os.str();
  }

  std::vector<BlockObj> GetBlockObj(const FlatFileSlice& slice) const {
    CHECK_EQ(slice.chunk_id, chunk_info_.chunkid());
    CHECK_GE(slice.file_offset, chunk_info_.offset());
    CHECK_LE((slice.file_offset + slice.len),
             (chunk_info_.offset() + chunk_info_.len()));

    std::vector<BlockObj> overlap_block_objs;

    auto start_iter = offset_to_block_.lower_bound(slice.file_offset);
    if (start_iter != offset_to_block_.begin()) {
      start_iter--;
      const BlockObj& block_obj = start_iter->second;
      overlap_block_objs.push_back(block_obj);
      start_iter++;
    }

    while (start_iter != offset_to_block_.end() &&
           start_iter->first < (slice.file_offset + slice.len)) {
      const BlockObj& block_obj = start_iter->second;
      overlap_block_objs.push_back(block_obj);
      start_iter++;
    }

    CHECK(!overlap_block_objs.empty());

    return overlap_block_objs;
  }

  std::vector<BlockObjSlice> GetBlockObjSlice(
      const FlatFileSlice& slice) const {
    CHECK_EQ(slice.chunk_id, chunk_info_.chunkid());
    CHECK_GE(slice.file_offset, chunk_info_.offset());
    CHECK_LE(slice.len, chunk_info_.len());

    std::vector<BlockObj> overlap_block_objs = GetBlockObj(slice);

    std::vector<BlockObjSlice> obj_slices;

    for (const auto& block_obj : overlap_block_objs) {
      uint64_t start_offset =
          std::max(slice.file_offset, block_obj.file_offset);
      uint64_t end_offset = std::min(slice.file_offset + slice.len,
                                     block_obj.file_offset + block_obj.obj_len);
      uint64_t len = end_offset - start_offset;

      BlockObjSlice obj_slice = {start_offset, len, block_obj};
      obj_slices.push_back(obj_slice);
    }

    return obj_slices;
  }

  const std::map<uint64_t, BlockObj>& GetOffsetToBlock() const {
    return offset_to_block_;
  }

 private:
  void Init() {
    uint64_t offset_in_chunk = chunk_info_.offset() % chunk_size_;
    uint64_t block_index_in_chunk = offset_in_chunk / block_size_;

    uint64_t chunk_end = chunk_info_.len() + chunk_info_.offset();
    uint64_t offset = chunk_info_.offset();

    uint64_t block_id = chunk_info_.offset() / block_size_;
    while (offset < chunk_end) {
      uint64_t block_boundary = (block_id + 1) * block_size_;
      uint64_t obj_len = std::min(block_boundary, chunk_end) - offset;

      BlockObj block_obj = {offset,
                            obj_len,
                            chunk_info_.zero(),
                            chunk_info_.compaction(),
                            chunk_info_.chunkid(),
                            block_index_in_chunk};

      CHECK(offset_to_block_.insert({offset, block_obj}).second);

      block_id++;
      offset += obj_len;

      block_index_in_chunk++;
    }
  }

  pb::metaserver::S3ChunkInfo chunk_info_;
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};
  std::map<uint64_t, BlockObj> offset_to_block_;
};

class FlatFile {
 public:
  FlatFile(uint64_t fs_id, uint64_t ino, uint64_t chunk_size,
           uint64_t block_size)
      : fs_id_(fs_id),
        ino_(ino),
        chunk_size_(chunk_size),
        block_size_(block_size) {}

  ~FlatFile() = default;

  void InsertFlatFileSlice(uint64_t chunk_index, const FlatFileSlice& slice) {
    chunk_index_flat_file_chunk_[chunk_index].InsertChunkInfo(slice);
  }

  void InsertChunkInfo(uint64_t chunk_index,
                       const pb::metaserver::S3ChunkInfo& chunk_info) {
    CHECK(chunk_id_to_s3_chunk_holer_
              .insert({chunk_info.chunkid(),
                       S3ChunkHoler(chunk_info, chunk_size_, block_size_)})
              .second);

    FlatFileSlice slice = {chunk_info.offset(), chunk_info.len(),
                           chunk_info.chunkid()};
    InsertFlatFileSlice(chunk_index, slice);
  }

  std::vector<BlockObj> GetBlockObj(uint64_t offset, uint64_t length) const;

  void DumpToString() const {
    {
      std::ostringstream os;
      for (const auto& chunk_holder : chunk_id_to_s3_chunk_holer_) {
        os << "chunk_id: " << chunk_holder.first
           << chunk_holder.second.ToString() << "\n";
      }
      LOG(INFO) << "chunk_id_to_chunk_info_:\n " << os.str();
    }

    {
      std::ostringstream os;
      os << "chunk_index_flat_file_chunk_:\n";
      for (const auto& chunk : chunk_index_flat_file_chunk_) {
        os << "chunk_index: " << chunk.first << chunk.second.ToString();
      }
      LOG(INFO) << "chunk_index_flat_file_chunk_:\n " << os.str();
    }
  }

  std::string FormatStringWithHeader(bool use_delimiter = false) const {
    std::ostringstream os;
    if (use_delimiter) {
      os << std::left << "file_offset"
         << "|"
         << "len"
         << "|"
         << "block_offset"
         << "|"
         << "block_name"
         << "|"
         << "block_len"
         << "|"
         << "zero"
         << "\n";
    } else {
      os << std::left << std::setw(20) << "file_offset" << std::setw(15)
         << "len" << std::setw(15) << "block_offset" << std::setw(100)
         << "block_name" << std::setw(15) << "block_len" << std::setw(10)
         << "zero"
         << "\n";
    }

    for (const auto& flat_file_chunk_iter : chunk_index_flat_file_chunk_) {
      const FlatFileChunk& flat_file_chunk = flat_file_chunk_iter.second;

      for (const auto& flat_file_slice_iter :
           flat_file_chunk.file_offset_slice_) {
        const FlatFileSlice& flat_file_slice = flat_file_slice_iter.second;

        auto iter = chunk_id_to_s3_chunk_holer_.find(flat_file_slice.chunk_id);
        CHECK(iter != chunk_id_to_s3_chunk_holer_.end())
            << "chunk_id: " << flat_file_slice.chunk_id << " not found";

        const S3ChunkHoler& chunk_holder = iter->second;

        std::vector<BlockObjSlice> obj_slices =
            chunk_holder.GetBlockObjSlice(flat_file_slice);

        for (const auto& obj_slice : obj_slices) {
          BlockKey key(fs_id_, ino_, obj_slice.obj.chunk_id,
                       obj_slice.obj.block_index, obj_slice.obj.version);

          uint64_t block_offset =
              obj_slice.file_offset - obj_slice.obj.file_offset;

          if (use_delimiter) {
            os << std::left << obj_slice.file_offset << "|" << obj_slice.len
               << "|" << block_offset << "|" << key.StoreKey() << "|"
               << obj_slice.obj.obj_len << "|"
               << (obj_slice.obj.zero ? "true" : "false") << "\n";
          } else {
            os << std::left << std::setw(20) << obj_slice.file_offset
               << std::setw(15) << obj_slice.len << std::setw(15)
               << block_offset << std::setw(100) << key.StoreKey()
               << std::setw(15) << obj_slice.obj.obj_len << std::setw(10)
               << (obj_slice.obj.zero ? "true" : "false") << "\n";
          }
        }
      }
    }

    return os.str();
  }

 private:
  uint64_t fs_id_;  // filesystem id
  uint64_t ino_;    // inode id
  uint64_t chunk_size_;
  uint64_t block_size_;
  // chunk id to s3 chunk holder
  std::map<uint64_t, S3ChunkHoler> chunk_id_to_s3_chunk_holer_;
  //  chunk index to flat file chunk
  std::map<uint64_t, FlatFileChunk> chunk_index_flat_file_chunk_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_SERVICE_FLAT_FILE_H_