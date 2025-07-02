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

#ifndef DINGOFS_CLIENT_VFS_DATA_FLAT_SLICE_HOLDER_H_
#define DINGOFS_CLIENT_VFS_DATA_FLAT_SLICE_HOLDER_H_

#include <glog/logging.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/data/common.h"
#include "client/vfs/data/flat/common.h"
#include "client/meta/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

class SliceHolder {
 public:
  static std::unique_ptr<SliceHolder> Create(uint64_t fs_id, uint64_t ino,
                                             uint64_t chunk_size,
                                             uint64_t block_size,
                                             const Slice& slice);

  ~SliceHolder() = default;

  void GetFileSlices(const SliceRange& slice_range,
                     std::vector<FileSlice> file_slices) const;

  std::string ToString() const;

 private:
  SliceHolder(uint64_t fs_id, uint64_t ino, uint64_t chunk_size,
              uint64_t block_size, const Slice& slice)
      : fs_id_(fs_id),
        ino_(ino),
        chunk_size_(chunk_size),
        block_size_(block_size),
        slice_(slice) {}

  void Init();

  void GetBlockDesc(const SliceRange& slice_range,
                    std::vector<BlockDesc> blocks) const;

  uint64_t fs_id_{0};
  uint64_t ino_{0};
  Slice slice_;
  uint64_t chunk_size_{0};
  uint64_t block_size_{0};

  std::map<uint64_t, BlockDesc> file_offset_to_block_;
};

}  // namespace vfs

}  // namespace client

}  // namespace dingofs
#endif  // DINGOFS_CLIENT_VFS_DATA_FLAT_SLICE_HOLDER_H_