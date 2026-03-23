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

#ifndef DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_BLOCK_STORE_H_
#define DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_BLOCK_STORE_H_

#include <gmock/gmock.h>

#include "client/vfs/blockstore/block_store.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

class MockBlockStore : public BlockStore {
 public:
  MOCK_METHOD(Status, Start, (), (override));
  MOCK_METHOD(void, Shutdown, (), (override));
  MOCK_METHOD(void, RangeAsync,
              (ContextSPtr ctx, RangeReq req, StatusCallback callback),
              (override));
  MOCK_METHOD(void, PutAsync,
              (ContextSPtr ctx, PutReq req, StatusCallback callback),
              (override));
  MOCK_METHOD(void, PrefetchAsync,
              (ContextSPtr ctx, PrefetchReq req, StatusCallback callback),
              (override));
  MOCK_METHOD(bool, EnableCache, (), (const, override));
  MOCK_METHOD(cache::BlockCache*, GetBlockCache, (), (const, override));
};

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CLIENT_VFS_MOCK_BLOCK_STORE_H_
