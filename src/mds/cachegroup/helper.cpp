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
 * Created Date: 2025-08-03
 * Author: Jingli Chen (Wine93)
 */

#include "mds/cachegroup/helper.h"

#include <absl/strings/str_format.h>

#include <chrono>

#include "mds/cachegroup/common.h"

namespace dingofs {
namespace mds {
namespace cachegroup {

int64_t Helper::TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

int64_t Helper::Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
}

PBCacheGroupErrCode Helper::PBErr(Status status) {
  if (status.ok()) {
    return PBCacheGroupErrCode::CacheGroupOk;
  } else if (status.IsNotFound()) {
    return PBCacheGroupErrCode::CacheGroupErrNotFound;
  } else if (status.IsExist()) {
    return PBCacheGroupErrCode::CacheGroupErrAlreadyExist;
  } else if (status.IsInvalidParam()) {
    return PBCacheGroupErrCode::CacheGroupErrInvalidParam;
  }
  return PBCacheGroupErrCode::CacheGroupErrFailure;
}

}  // namespace cachegroup
}  // namespace mds
}  // namespace dingofs
