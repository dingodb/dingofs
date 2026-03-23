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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_IFILE_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_IFILE_H_

#include <gmock/gmock.h>

#include "client/vfs/data/ifile.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class MockIFile : public IFile {
 public:
  MOCK_METHOD(Status, Open, (), (override));
  MOCK_METHOD(void, Close, (), (override));
  MOCK_METHOD(Status, Write,
              (ContextSPtr ctx, const char* buf, uint64_t size, uint64_t offset,
               uint64_t* out_wsize),
              (override));
  MOCK_METHOD(Status, Read,
              (ContextSPtr ctx, DataBuffer* data_buffer, uint64_t size,
               uint64_t offset, uint64_t* out_rsize),
              (override));
  MOCK_METHOD(void, Invalidate, (int64_t offset, int64_t size), (override));
  MOCK_METHOD(Status, Flush, (), (override));
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_IFILE_H_
