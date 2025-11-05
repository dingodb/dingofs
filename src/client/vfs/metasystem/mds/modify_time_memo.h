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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_MODIFY_TIME_MEMO_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_MODIFY_TIME_MEMO_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <vector>

#include "client/vfs/vfs_meta.h"
#include "json/value.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class ModifyTimeMemo {
 public:
  ModifyTimeMemo() = default;
  ~ModifyTimeMemo() = default;

  void Remember(Ino ino);
  void Forget(Ino ino);
  void ForgetExpired(uint64_t expire_timestamp_ns);

  uint64_t Get(Ino ino);
  bool ModifiedSince(Ino ino, uint64_t timestamp);

 private:
  utils::RWLock lock_;
  std::map<Ino, uint64_t> modify_time_map_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_MODIFY_TIME_MEMO_H_
