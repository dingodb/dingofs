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

#ifndef DINGOFS_MDSV2_BACKGROUND_CLEAN_INODE_H_
#define DINGOFS_MDSV2_BACKGROUND_CLEAN_INODE_H_

#include "mdsv2/common/distribution_lock.h"
#include "mdsv2/common/status.h"
#include "mdsv2/filesystem/filesystem.h"

namespace dingofs {
namespace mdsv2 {

class CleanInode {
 public:
  CleanInode() = default;
  ~CleanInode() = default;

  void Clean();

 private:
  std::atomic<bool> is_running_{false};

  FileSystemSetSPtr fs_set_;

  DistributionLockPtr dist_lock_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_BACKGROUND_CLEAN_INODE_H_