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

#ifndef DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_
#define DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "client/vfs/data/common.h"
#include "client/vfs/data/flat/common.h"
#include "client/vfs/data/flat/slice_holder.h"
#include "client/meta/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class SliceHolder;

class FlatFileChunk {
 public:
  static std::shared_ptr<FlatFileChunk> Create(
      uint64_t fs_id, uint64_t ino, uint64_t index, uint64_t chunk_size,
      uint64_t block_size, const std::vector<Slice>& slices);

  ~FlatFileChunk() = default;

  void PrepareReadReq(const FileRange& file_range,
                      std::vector<BlockReadReq>& block_read_reqs);

  std::string ToString() const {
    std::ostringstream os;
    for (const auto& slice : file_offset_slice_) {
      os << slice.second.ToString() << "\n";
    }
    return os.str();
  }

 private:
  FlatFileChunk(uint64_t fs_id, uint64_t ino, uint64_t index,
                uint64_t chunk_size, uint64_t block_size)
      : fs_id_(fs_id),
        ino_(ino),
        index_(index),
        chunk_size_(chunk_size),
        block_size_(block_size) {}

  void InsertSlice(const Slice& new_slice);

  void GenerateFileSlice();

  void InsertFileOffsetSliceMap(const SliceRange& new_slice);

  std::vector<SliceRange> FindOverlappingSlices(
      const SliceRange& new_slice_range);

  void RemoveOverlappingSlices(const std::vector<SliceRange>& overlap_slices);

  void ProcessOverlapSlices(const std::vector<SliceRange>& overlap_slices,
                            const SliceRange& new_slice_range);

  void InsertNewSlice(const SliceRange& new_slice_range);

  uint64_t fs_id_{0};
  uint64_t ino_{0};
  uint64_t index_{0};
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  // TOOD: file_offset_slice_ and slice_id_to_slice_holder_ maybe no need to
  // keep in memory
  std::map<uint64_t, SliceRange> file_offset_slice_;
  // slice_id_to_slice_holder_ is used to store the slice holder
  std::unordered_map<uint64_t, std::unique_ptr<SliceHolder>>
      slice_id_to_slice_holder_;

  std::map<uint64_t, FileSlice> file_offset_file_slice_map_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGODB_CLIENT_VFS_DATA_FLAT_CHUNK_H_
