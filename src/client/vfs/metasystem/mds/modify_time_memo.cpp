// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "client/vfs/metasystem/mds/modify_time_memo.h"

#include "mds/common/helper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

void ModifyTimeMemo::Remember(Ino ino) {
  utils::WriteLockGuard guard(lock_);

  modify_time_map_[ino] = mds::Helper::TimestampNs();
}

void ModifyTimeMemo::Forget(Ino ino) {
  utils::WriteLockGuard guard(lock_);

  modify_time_map_.erase(ino);
}

void ModifyTimeMemo::ForgetExpired(uint64_t expire_timestamp_ns) {
  utils::WriteLockGuard guard(lock_);

  for (auto it = modify_time_map_.begin(); it != modify_time_map_.end();) {
    if (it->second < expire_timestamp_ns) {
      it = modify_time_map_.erase(it);
    } else {
      ++it;
    }
  }
}

uint64_t ModifyTimeMemo::Get(Ino ino) {
  utils::ReadLockGuard guard(lock_);

  auto it = modify_time_map_.find(ino);
  return (it != modify_time_map_.end()) ? it->second : 0;
}

bool ModifyTimeMemo::ModifiedSince(Ino ino, uint64_t timestamp) {
  return Get(ino) > timestamp;
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs