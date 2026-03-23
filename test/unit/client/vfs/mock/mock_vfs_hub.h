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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_VFS_HUB_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_VFS_HUB_H_

#include <gmock/gmock.h>

#include "client/vfs/hub/vfs_hub.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class MockVFSHub : public VFSHub {
 public:
  MOCK_METHOD(Status, Start, (bool skip_mount), (override));
  MOCK_METHOD(Status, Stop, (bool skip_unmount), (override));
  MOCK_METHOD(ClientId, GetClientId, (), (override));
  MOCK_METHOD(MetaWrapper*, GetMetaSystem, (), (override));
  MOCK_METHOD(HandleManager*, GetHandleManager, (), (override));
  MOCK_METHOD(BlockStore*, GetBlockStore, (), (override));
  MOCK_METHOD(blockaccess::BlockAccesser*, GetBlockAccesser, (), (override));
  MOCK_METHOD(Executor*, GetReadExecutor, (), (override));
  MOCK_METHOD(Executor*, GetBGExecutor, (), (override));
  MOCK_METHOD(Executor*, GetFlushExecutor, (), (override));
  MOCK_METHOD(Executor*, GetCBExecutor, (), (override));
  MOCK_METHOD(WriteBufferManager*, GetWriteBufferManager, (), (override));
  MOCK_METHOD(ReadBufferManager*, GetReadBufferManager, (), (override));
  MOCK_METHOD(FileSuffixWatcher*, GetFileSuffixWatcher, (), (override));
  MOCK_METHOD(PrefetchManager*, GetPrefetchManager, (), (override));
  MOCK_METHOD(WarmupManager*, GetWarmupManager, (), (override));
  MOCK_METHOD(Compactor*, GetCompactor, (), (override));
  MOCK_METHOD(TraceManager*, GetTraceManager, (), (override));
  MOCK_METHOD(FsInfo, GetFsInfo, (), (override));
  MOCK_METHOD(blockaccess::BlockAccessOptions, GetBlockAccesserOptions, (),
              (override));
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_VFS_HUB_H_
