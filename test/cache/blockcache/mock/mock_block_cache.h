/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: Dingofs
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CACHE_MOCK_BLOCKCACHE_H_
#define DINGOFS_TEST_CACHE_MOCK_BLOCKCACHE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"
#include "cache/common/common.h"
#include "gmock/gmock.h"

namespace dingofs {
namespace cache {
namespace blockcache {

class MockBlockCache : public BlockCache {
 public:
  MockBlockCache() = default;

  ~MockBlockCache() override = default;

  MOCK_METHOD0(Init, Status());

  MOCK_METHOD0(Shutdown, Status());

  MOCK_METHOD3(Put, Status(const BlockKey& key, const Block& block,
                           BlockContext ctx));

  MOCK_METHOD5(Range, Status(const BlockKey& key, off_t offset, size_t size,
                             char* buffer, bool retrive));

  MOCK_METHOD2(SubmitPrefetch, void(const BlockKey& key, size_t length));

  MOCK_METHOD2(Cache, Status(const BlockKey& key, const Block& block));

  MOCK_METHOD1(Flush, Status(uint64_t ino));

  MOCK_METHOD(void, AsyncPut,
              (PutOption option, const BlockKey& key, const Block& block,
               AsyncCallback callback),
              (override));

  MOCK_METHOD(void, AsyncRange,
              (RangeOption option, const BlockKey& key, off_t offset,
               size_t length, IOBuffer* buffer, AsyncCallback callback),
              (override));

  MOCK_METHOD(void, AsyncCache,
              (CacheOption option, const BlockKey& key, const Block& block,
               AsyncCallback callback),
              (override));

  MOCK_METHOD(void, AsyncPrefetch,
              (PrefetchOption option, const BlockKey& key, size_t length,
               AsyncCallback callback),
              (override));

  MOCK_METHOD1(IsCached, bool(const BlockKey& key));

  MOCK_METHOD0(GetStoreType, StoreType());
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_CACHE_MOCK_BLOCKCACHE_H_
