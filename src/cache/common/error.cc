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
 * Created Date: 2026-01-19
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/error.h"

namespace dingofs {
namespace cache {

static std::unordered_map<pb::cache::BlockCacheErrCode, Status> kErrCodes{
    {pb::cache::BlockCacheOk, Status::OK()},
    {pb::cache::BlockCacheErrInvalidParam, Status::InvalidParam("")},
    {pb::cache::BlockCacheErrNotFound, Status::NotFound("")},
    {pb::cache::BlockCacheErrFailure, Status::Internal("")},
    {pb::cache::BlockCacheErrIOError, Status::IoError("")},
};

static std::unordered_map<int32_t, pb::cache::BlockCacheErrCode> kSysCodes;

static bool initialized = [] {
  for (const auto& pair : kErrCodes) {
    kSysCodes[pair.second.ToSysErrNo()] = pair.first;
  }
  return true;
}();

pb::cache::BlockCacheErrCode ToPBErr(Status status) {
  auto it = kSysCodes.find(status.ToSysErrNo());
  if (it != kSysCodes.end()) {
    return it->second;
  }
  return pb::cache::BlockCacheErrUnknown;
}

Status ToStatus(pb::cache::BlockCacheErrCode errcode) {
  auto it = kErrCodes.find(errcode);
  if (it != kErrCodes.end()) {
    return it->second;
  }
  return Status::Internal("Unknown error code");
}

}  // namespace cache
}  // namespace dingofs
