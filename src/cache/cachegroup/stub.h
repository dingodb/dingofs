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

/*
 * Project: DingoFS
 * Created Date: 2025-07-24
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_CACHEGROUP_STUB_H_
#define DINGOFS_SRC_CACHE_CACHEGROUP_STUB_H_

#include "cache/storage/storage_pool.h"

namespace dingofs {
namespace cache {

// FIXME: So ugly implemention, pls remove ASAP.
GetStorageInfoFunc NewV1GetStorageInfoFunc(
    std::shared_ptr<stub::rpcclient::MdsClient> mds_client);

GetStorageInfoFunc NewV2GetStorageInfoFunc(const std::string& mds_addr);

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_CACHEGROUP_STUB_H_
