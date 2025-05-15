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
 * Created Date: 2025-05-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_PROTO_H_
#define DINGOFS_SRC_CACHE_COMMON_PROTO_H_

#include "common/status.h"
#include "dingofs/blockcache.pb.h"
#include "dingofs/cachegroup.pb.h"
#include "dingofs/mds.pb.h"

namespace dingofs {
namespace cache {

using dingofs::pb::cache::blockcache::BlockCacheErrCode;       // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrCode_Name;  // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrFailure;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrIOError;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrNotFound;   // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheErrUnknown;    // NOLINT
using dingofs::pb::cache::blockcache::BlockCacheOk;            // NOLINT

using dingofs::pb::mds::FSStatusCode;                        // NOLINT
using dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;  // NOLINT
using dingofs::pb::mds::cachegroup::CacheGroupMember;        // NOLINT
using dingofs::pb::mds::cachegroup::CacheGroupOk;            // NOLINT

inline BlockCacheErrCode PbErr(Status status) {
  if (status.ok()) {
    return BlockCacheOk;
  } else if (status.IsIoError()) {
    return BlockCacheErrIOError;
  } else if (status.IsNotFound()) {
    return BlockCacheErrNotFound;
  }
  return BlockCacheErrFailure;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_PROTO_H_
