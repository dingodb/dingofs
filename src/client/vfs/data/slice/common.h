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

#ifndef DINGOFS_CLIENT_VFS_DATA_SLICE_COMMON_H_
#define DINGOFS_CLIENT_VFS_DATA_SLICE_COMMON_H_

#include <cstdint>

namespace dingofs {
namespace client {
namespace vfs {

struct SliceDataContext {
  uint64_t fs_id{0};
  uint64_t ino{0};
  uint64_t chunk_index{0};
  uint64_t seq{0};  // unique id for slice in current chunk, valid from 1
  uint64_t chunk_size{0};
  uint64_t block_size{0};
  uint64_t page_size{0};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_SLICE_DATA_H_