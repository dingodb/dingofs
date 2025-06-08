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

#ifndef DINGODB_CLIENT_VFS_DATA_FLAT_COMMON_H_
#define DINGODB_CLIENT_VFS_DATA_FLAT_COMMON_H_

#include <cstdint>
#include <string>

#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace client {
namespace vfs {

struct SliceRange {
  uint64_t file_offset;
  uint64_t len;
  uint64_t slice_id;

  uint64_t End() const { return file_offset + len; }

  std::string ToString() const;
};

struct FileSlice {
  uint64_t file_offset;       // file offset
  uint64_t len;               // slice length
  uint64_t block_offset;      // block offset
  cache::BlockKey block_key;  // block key
  uint64_t block_len;         // block length
  bool zero;                  // is zero or not

  std::string ToString() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGODB_CLIENT_VFS_DATA_FLAT_COMMON_H_