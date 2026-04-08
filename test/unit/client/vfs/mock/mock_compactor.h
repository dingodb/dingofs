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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_COMPACTOR_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_COMPACTOR_H_

#include <gmock/gmock.h>

#include "client/vfs/compaction/compactor.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class MockCompactor : public Compactor {
 public:
  MOCK_METHOD(Status, Start, (), (override));
  MOCK_METHOD(Status, Stop, (), (override));
  MOCK_METHOD(Status, Compact,
              (ContextSPtr ctx, Ino ino, int64_t chunk_index,
               const std::vector<Slice>& slices, std::vector<Slice>& out_slices),
              (override));
  MOCK_METHOD(Status, ForceCompact,
              (ContextSPtr ctx, Ino ino, int64_t chunk_index,
               const std::vector<Slice>& slices, std::vector<Slice>& out_slices),
              (override));
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_COMPACTOR_H_
