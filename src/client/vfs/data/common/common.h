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

#ifndef DINGODB_CLIENT_VFS_DATA_COMMON_H_
#define DINGODB_CLIENT_VFS_DATA_COMMON_H_

#include <cstdint>
#include <optional>
#include <string>

#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

struct FileRange {
  uint64_t offset;
  uint64_t len;

  uint64_t End() const { return offset + len; }
  std::string ToString() const;
};

struct SliceReadReq {
  uint64_t file_offset;
  uint64_t len;
  std::optional<Slice> slice;

  uint64_t End() const { return file_offset + len; }
  std::string ToString() const;
};

struct BlockDesc {
  uint64_t file_offset;
  uint64_t block_len;  // the len of the block
  bool zero;
  uint64_t version;
  uint64_t slice_id;
  uint64_t index;  // block index in the chunk

  uint64_t End() const { return file_offset + block_len; }
  std::string ToString() const;
};

struct BlockReadReq {
  uint64_t block_offset;
  uint64_t len;
  BlockDesc block;

  // Note: this is the offset in the block, not the file offset.
  uint64_t End() const { return block_offset + len; }
  std::string ToString() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_COMMON_H_