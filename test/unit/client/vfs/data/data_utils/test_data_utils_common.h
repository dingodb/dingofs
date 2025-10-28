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

static const bool kDefaltZero = false;
static const uint64_t kDefaultCompaction = 1;

static Slice CreateSlice(uint64_t offset, uint64_t length, uint64_t id,
                         bool is_zero = kDefaltZero,
                         uint64_t compaction = kDefaultCompaction) {
  return Slice{
      .id = id,
      .offset = offset,
      .length = length,
      .compaction = compaction,
      .is_zero = is_zero,
  };
}

static FileRange CreateFileRange(uint64_t offset, uint64_t len) {
  return FileRange{
      .offset = offset,
      .len = len,
  };
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_VFS_DATA_TEST_DATA_UTILS_COMMON_H