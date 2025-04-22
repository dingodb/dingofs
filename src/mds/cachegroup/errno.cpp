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
 * Created Date: 2025-02-08
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/errno.h"

#include <string>
#include <unordered_map>

#include "dingofs/cachegroup.pb.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

using ::dingofs::pb::mds::cachegroup::CacheGroupErrCode_Name;
using ::dingofs::pb::mds::cachegroup::CacheGroupErrFailure;
using ::dingofs::pb::mds::cachegroup::CacheGroupErrInvalidGroupName;
using ::dingofs::pb::mds::cachegroup::CacheGroupErrInvalidMemberId;
using ::dingofs::pb::mds::cachegroup::CacheGroupErrNotFound;
using ::dingofs::pb::mds::cachegroup::CacheGroupErrUnknown;
using ::dingofs::pb::mds::cachegroup::CacheGroupOk;

static const std::unordered_map<Errno, CacheGroupErrCode> kErrnos = {
    {Errno::kOk, CacheGroupOk},
    {Errno::kFail, CacheGroupErrFailure},
    {Errno::kNotFound, CacheGroupErrNotFound},
    {Errno::kInvalidMemberId, CacheGroupErrInvalidMemberId},
    {Errno::kInvalidGroupName, CacheGroupErrInvalidGroupName},
};

std::string StrErr(Errno code) {
  auto it = kErrnos.find(code);
  if (it != kErrnos.end()) {
    return CacheGroupErrCode_Name(it->second);
  }
  return "unknown";
}

CacheGroupErrCode PbErr(Errno code) {
  auto it = kErrnos.find(code);
  if (it != kErrnos.end()) {
    return it->second;
  }
  return CacheGroupErrUnknown;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
