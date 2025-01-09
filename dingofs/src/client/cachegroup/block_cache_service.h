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
 * Created Date: 2025-01-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_CACHEGROUP_BLOCK_CACHE_SERVICE_H_
#define DINGOFS_SRC_CLIENT_CACHEGROUP_BLOCK_CACHE_SERVICE_H_

namespace dingofs {
namespace client {
namespace cachegroup {

class BlockCacheService {
 public:
  virtual ~BlockCacheService() = default;
};

// Range Cache
class BlockCacheServiceImpl : public BlockCacheService {
 public:
};

}  // namespace cachegroup
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_CACHEGROUP_BLOCK_CACHE_SERVICE_H_
