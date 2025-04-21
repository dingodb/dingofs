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

#ifndef DINGOFS_SRC_MDSV2_FS_UTILS_H_
#define DINGOFS_SRC_MDSV2_FS_UTILS_H_

#include <string>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {

struct FsTreeNode {
  bool is_orphan{true};
  pb::mdsv2::Dentry dentry;
  pb::mdsv2::Inode inode;

  std::vector<FsTreeNode*> children;
};

void FreeFsTree(FsTreeNode* root);

class FsUtils {
 public:
  FsUtils(KVStorageSPtr kv_storage) : kv_storage_(kv_storage) {}

  FsTreeNode* GenFsTree(uint32_t fs_id);
  std::string GenFsTreeJsonString(uint32_t fs_id);

 private:
  KVStorageSPtr kv_storage_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDSV2_FS_UTILS_H_
