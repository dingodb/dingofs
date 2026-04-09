// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_TEST_CLIENT_VFS_DATA_TEST_DATA_UTILS_COMMON_H
#define DINGOFS_TEST_CLIENT_VFS_DATA_TEST_DATA_UTILS_COMMON_H

#include <gtest/gtest.h>

#include <cstdint>

#include "client/vfs/data/common/common.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

// Create a Slice with the new fields: pos, size, off, len.
// For simple cases where off=0 and len=size, only pos/size/id are needed.
static Slice CreateSlice(uint64_t id, int32_t pos, int32_t size,
                         int32_t off = 0, int32_t len = -1) {
  if (len < 0) {
    len = size - off;
  }
  return Slice{
      .id = id,
      .size = size,
      .off = off,
      .len = len,
      .pos = pos,
  };
}

static FileRange CreateFileRange(int64_t offset, int64_t len) {
  return FileRange{
      .offset = offset,
      .len = len,
  };
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_VFS_DATA_TEST_DATA_UTILS_COMMON_H