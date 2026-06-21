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

#ifndef DINGOFS_TEST_UNIT_CACHE_COMMON_MOCK_STORAGE_CLIENT_POOL_H_
#define DINGOFS_TEST_UNIT_CACHE_COMMON_MOCK_STORAGE_CLIENT_POOL_H_

#include <gmock/gmock.h>

#include "cache/common/storage_client_pool.h"

namespace dingofs {
namespace cache {

class MockStorageClientPool : public StorageClientPool {
 public:
  MockStorageClientPool() = default;
  ~MockStorageClientPool() override = default;

  MOCK_METHOD(Status, GetStorageClient,
              (uint32_t fs_id, StorageClient** storage_client), (override));
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CACHE_COMMON_MOCK_STORAGE_CLIENT_POOL_H_
