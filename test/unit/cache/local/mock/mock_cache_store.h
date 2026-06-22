/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2026-06-21
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_CACHE_BLOCKCACHE_MOCK_CACHE_STORE_H_
#define DINGOFS_TEST_UNIT_CACHE_BLOCKCACHE_MOCK_CACHE_STORE_H_

#include <gmock/gmock.h>

#include "cache/local/cache_store.h"

namespace dingofs {
namespace cache {

class MockCacheStore : public CacheStore {
 public:
  MockCacheStore() = default;
  ~MockCacheStore() override = default;

  MOCK_METHOD(Status, Start, (UploadFunc uploader), (override));
  MOCK_METHOD(Status, Shutdown, (), (override));
  MOCK_METHOD(Status, Stage, (BlockHandle handle, IOBuffer block, StageOption),
              (override));
  MOCK_METHOD(Status, RemoveStage, (BlockHandle handle, RemoveStageOption),
              (override));
  MOCK_METHOD(Status, Cache, (BlockHandle handle, IOBuffer block, CacheOption),
              (override));
  MOCK_METHOD(Status, Load,
              (BlockHandle handle, off_t offset, size_t length,
               IOBuffer* buffer, LoadOption),
              (override));
  MOCK_METHOD(std::string, Id, (), (const, override));
  MOCK_METHOD(bool, IsRunning, (), (const, override));
  MOCK_METHOD(bool, IsCached, (const BlockHandle& handle), (const, override));
  MOCK_METHOD(bool, IsFull, (const BlockHandle& handle), (const, override));
  MOCK_METHOD(bool, Dump, (Json::Value & value), (const, override));
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CACHE_BLOCKCACHE_MOCK_CACHE_STORE_H_
