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
#include "dingofs/cache.pb.h"
#include "dingofs/cachegroup.pb.h"
#include "dingofs/common.pb.h"
#include "dingofs/mds.pb.h"
#include "dingofs/mdsv2.pb.h"

namespace dingofs {
namespace cache {

using PBFSStatusCode = pb::mds::FSStatusCode;
using PBFsInfo = pb::mds::FsInfo;
using PBFsInfoV2 = pb::mdsv2::FsInfo;
using PBStorageInfo = pb::common::StorageInfo;
using PBS3Info = pb::common::S3Info;
using PBRadosInfo = pb::common::RadosInfo;
using PBS3InfoV2 = pb::mdsv2::S3Info;
using PBRadosInfoV2 = pb::mdsv2::RadosInfo;

using PBCacheGroupMember = pb::mds::cachegroup::CacheGroupMember;
using PBCacheGroupErrCode = pb::mds::cachegroup::CacheGroupErrCode;
using PBCacheGroupMemberState = pb::mds::cachegroup::CacheGroupMemberState;
using PBCacheGroupMembers = std::vector<PBCacheGroupMember>;

using PBBlockKey = pb::cache::blockcache::BlockKey;
using PBBlockCacheErrCode = pb::cache::blockcache::BlockCacheErrCode;
using PBBlockCacheService_Stub = pb::cache::blockcache::BlockCacheService_Stub;
using PBBlockCacheService = pb::cache::blockcache::BlockCacheService;
using PBPutRequest = pb::cache::blockcache::PutRequest;
using PBPutResponse = pb::cache::blockcache::PutResponse;
using PBRangeRequest = pb::cache::blockcache::RangeRequest;
using PBRangeResponse = pb::cache::blockcache::RangeResponse;
using PBCacheRequest = pb::cache::blockcache::CacheRequest;
using PBCacheResponse = pb::cache::blockcache::CacheResponse;
using PBPrefetchRequest = pb::cache::blockcache::PrefetchRequest;
using PBPrefetchResponse = pb::cache::blockcache::PrefetchResponse;
using PBPingRequest = pb::cache::blockcache::PingRequest;
using PBPingResponse = pb::cache::blockcache::PingResponse;

using PBCacheService = pb::cache::cache;

inline PBBlockCacheErrCode PBErr(Status status) {
  if (status.ok()) {
    return PBBlockCacheErrCode::BlockCacheOk;
  } else if (status.IsInvalidParam()) {
    return PBBlockCacheErrCode::BlockCacheErrInvalidParam;
  } else if (status.IsNotFound()) {
    return PBBlockCacheErrCode::BlockCacheErrNotFound;
  } else if (status.IsInternal()) {
    return PBBlockCacheErrCode::BlockCacheErrFailure;
  } else if (status.IsIoError()) {
    return PBBlockCacheErrCode::BlockCacheErrIOError;
  }

  return PBBlockCacheErrCode::BlockCacheErrUnknown;
}

inline Status ToStatus(const PBBlockCacheErrCode& code) {
  if (code == PBBlockCacheErrCode::BlockCacheOk) {
    return Status::OK();
  } else if (code == PBBlockCacheErrCode::BlockCacheErrInvalidParam) {
    return Status::InvalidParam("Invalid parameter");
  } else if (code == PBBlockCacheErrCode::BlockCacheErrNotFound) {
    return Status::NotFound("Not found");
  } else if (code == PBBlockCacheErrCode::BlockCacheErrFailure) {
    return Status::Internal("Internal error");
  } else if (code == PBBlockCacheErrCode::BlockCacheErrIOError) {
    return Status::IoError("IO error");
  }
  return Status::Unknown("unknown error");
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_PROTO_H_
