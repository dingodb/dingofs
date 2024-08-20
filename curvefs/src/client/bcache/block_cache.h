/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-05
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_BLOCK_CACHE_H_
#define CURVEFS_SRC_CLIENT_BCACHE_BLOCK_CACHE_H_

#include <memory>
#include <string>

#include "curvefs/src/client/bcache/cache_store.h"
#include "curvefs/src/client/common/config.h"
#include "curvefs/src/client/bcache/error.h"

namespace curvefs {
namespace client {
namespace bcache {

using ::curvefs::client::common::BlockCacheOption;

class BlockCache {
 public:
    virtual BCACHE_ERROR Init() = 0;

    virtual BCACHE_ERROR Shutdown() = 0;

    virtual BCACHE_ERROR Put(const BlockKey& key, const Block& block) = 0;

    virtual BCACHE_ERROR Get(const BlockKey& key, Block* block) = 0;

  	virtual BCACHE_ERROR Range(const BlockKey& key,
                              off_t offset,
                              size_t size,
                              char* buffer) = 0;

  	virtual BCACHE_ERROR Flush(uint64_t fh) = 0;
};

class BlockCacheImpl : public BlockCache {
 public:
    explicit BlockCacheImpl(BlockCacheOption option);

    ~BlockCacheImpl() = default;

    BCACHE_ERROR Init() override;

    BCACHE_ERROR Shutdown() override;

    BCACHE_ERROR Put(const BlockKey& key, const Block& block) override;

    BCACHE_ERROR Get(const BlockKey& key, Block* block) override;

  	BCACHE_ERROR Range(const BlockKey& key,
                       off_t offset,
                       size_t size,
                       char* buffer) override;

  	BCACHE_ERROR Flush(uint64_t fh) override;

 private:
    BlockCacheOption option_;
    std::unique_ptr<CacheStore> store_;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_BLOCK_CACHE_H_
