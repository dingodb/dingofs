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

using PB_FSStatusCode = pb::mds::FSStatusCode;
using PB_FsInfo = pb::mds::FsInfo;

using PB_CacheGroupMember = pb::mds::cachegroup::CacheGroupMember;
using PB_CacheGroupNodeMetadata = pb::mds::cachegroup::CacheGroupNodeMetadata;
using PB_CacheGroupErrCode = pb::mds::cachegroup::CacheGroupErrCode;
using PB_Statistic = pb::mds::cachegroup::HeartbeatRequest::Statistic;
using PB_CacheGroupMemberStatus = pb::mds::cachegroup::CacheGroupMemberStatus;
using PB_CacheGroupMembers = std::vector<PB_CacheGroupMember>;

using PB_BlockCacheErrCode = pb::cache::blockcache::BlockCacheErrCode;
using PB_BlockCacheService_Stub = pb::cache::blockcache::BlockCacheService_Stub;
using PB_PutRequest = pb::cache::blockcache::PutRequest;
using PB_PutResponse = pb::cache::blockcache::PutResponse;
using PB_RangeRequest = pb::cache::blockcache::RangeRequest;
using PB_RangeResponse = pb::cache::blockcache::RangeResponse;
using PB_CacheRequest = pb::cache::blockcache::CacheRequest;
using PB_CacheResponse = pb::cache::blockcache::CacheResponse;
using PB_PrefetchRequest = pb::cache::blockcache::PrefetchRequest;
using PB_PrefetchResponse = pb::cache::blockcache::PrefetchResponse;
using PB_BlockCacheService = pb::cache::blockcache::BlockCacheService;

inline PB_BlockCacheErrCode PBErr(Status status) {
  if (status.ok()) {
    return PB_BlockCacheErrCode::BlockCacheOk;
  } else if (status.IsInvalidParam()) {
    return PB_BlockCacheErrCode::BlockCacheErrInvalidParam;
  } else if (status.IsNotFound()) {
    return PB_BlockCacheErrCode::BlockCacheErrNotFound;
  } else if (status.IsInternal()) {
    return PB_BlockCacheErrCode::BlockCacheErrFailure;
  } else if (status.IsIoError()) {
    return PB_BlockCacheErrCode::BlockCacheErrIOError;
  }

  return PB_BlockCacheErrCode::BlockCacheErrUnknown;
}

inline bool operator==(const PB_CacheGroupMember& lhs,
                       const PB_CacheGroupMember& rhs) {
  return lhs.id() == rhs.id() && lhs.ip() == rhs.ip() &&
         lhs.port() == rhs.port() && lhs.weight() == rhs.weight();
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_PROTO_H_
