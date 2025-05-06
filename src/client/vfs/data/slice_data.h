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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>

#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

struct PageData {
  uint64_t index;
  std::unique_ptr<char*> data;
};

// page_index -> PageData
using PageDataMap = std::map<uint64_t, PageData>;

class Chunk;
// writing -> freezed -> sync
class SliceData {
 public:
  explicit SliceData(uint64_t chunk_index, uint64_t chunk_offset,
                     uint64_t length)
      : chunk_index_(chunk_index),
        chunk_offset_(chunk_offset),
        length_(length) {}

  ~SliceData();

  Status Write(const char* buf, uint64_t size, uint64_t slice_offset);

 private:
  friend class Chunk;

  uint64_t chunk_index_;
  uint64_t chunk_offset_;
  uint64_t length_;
  std::atomic_bool writing_{true};
  std::atomic_bool freezed_{false};
  std::atomic_bool sync_{false};
  // block_index -> PageData vet
  std::map<uint64_t, PageDataMap> block_pages_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_