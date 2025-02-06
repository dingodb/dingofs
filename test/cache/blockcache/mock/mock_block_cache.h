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
 * Project: Curve
 * Created Date: 2024-09-08
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_TEST_CLIENT_MOCK_BLOCKCACHE_H_
#define DINGOFS_TEST_CLIENT_MOCK_BLOCKCACHE_H_

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "cache/blockcache/block_cache.h"
#include "cache/blockcache/cache_store.h"

namespace dingofs {
namespace cache {
namespace blockcache {

using ::dingofs::client::blockcache::Errno;

class MockBlockCache : public BlockCache {
 public:
  MockBlockCache() = default;

  ~MockBlockCache() override = default;

  MOCK_METHOD0(Init, Errno());

  MOCK_METHOD0(Shutdown, Errno());

  MOCK_METHOD3(Put, Errno(const BlockKey& key, const Block& block,
                          BlockContext ctx));

  MOCK_METHOD5(Range, Errno(const BlockKey& key, off_t offset, size_t size,
                            char* buffer, bool retrive));

  MOCK_METHOD2(Cache, Errno(const BlockKey& key, const Block& block));

  MOCK_METHOD1(Flush, Errno(uint64_t ino));

  MOCK_METHOD1(IsCached, bool(const BlockKey& key));

  MOCK_METHOD0(GetStoreType, StoreType());
};

}  // namespace blockcache
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_CLIENT_MOCK_BLOCKCACHE_H_
