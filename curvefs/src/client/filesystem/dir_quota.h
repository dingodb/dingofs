// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_H_
#define CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_H_

#include <atomic>
#include <cstdint>

#include "curvefs/proto/metaserver.pb.h"
#include "curvefs/src/client/filesystem/dir_parent_watcher.h"
#include "curvefs/src/client/filesystem/meta.h"
#include "src/common/concurrent/concurrent.h"

namespace curvefs {
namespace client {
namespace filesystem {

using ::curve::common::RWLock;
using ::curvefs::metaserver::Quota;
using ::curvefs::metaserver::Usage;

class DirQuota {
 public:
  DirQuota(Ino ino, Quota quota) : ino_(ino), quota_(std::move(quota)) {}

  Ino GetIno() const { return ino_; }

  void UpdateUsage(int64_t new_space, int64_t new_inodes);

  bool CheckQuota(int64_t new_space, int64_t new_inodes);

  void Refresh(Quota quota);

  Usage GetUsage();

  Quota GetQuota();

  std::string ToString();

 private:
  const Ino ino_;
  std::atomic<int64_t> new_space_{0};
  std::atomic<int64_t> new_inodes_{0};

  RWLock rwlock_;
  Quota quota_;
};

}  // namespace filesystem
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_FILE_SYSTEM_DIR_QUOTA_H_