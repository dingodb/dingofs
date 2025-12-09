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

#ifndef DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_
#define DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_

#include <cstdint>
#include <string>

#include "client/vfs/data/common/common.h"

namespace dingofs {
namespace client {
namespace vfs {

struct ChunkReadReq {
  const uint64_t req_id{0};  // request id
  const uint64_t ino{0};     // ino
  const int64_t index{0};    // chunk index
  const int64_t offset{0};   // offset in the chunk
  const FileRange frange;

  std::string ToString() const;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_DATA_READER_READER_COMMON_H_