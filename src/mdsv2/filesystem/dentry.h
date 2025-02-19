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

#ifndef DINGOFS_MDV2_FILESYSTEM_DENTRY_H_
#define DINGOFS_MDV2_FILESYSTEM_DENTRY_H_

#include <sys/types.h>

#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "mdsv2/filesystem/inode.h"
#include "utils/concurrent/concurrent.h"
#include "utils/lru_cache.h"

namespace dingofs {
namespace mdsv2 {

class DentrySet;
using DentrySetPtr = std::shared_ptr<DentrySet>;

// represent a file or directory entry
class Dentry {
 public:
  Dentry() = default;
  Dentry(uint32_t fs_id, const std::string& name, uint64_t parent_ino, uint64_t ino, pb::mdsv2::FileType type,
         uint32_t flag, InodePtr inode = nullptr);
  Dentry(const pb::mdsv2::Dentry& dentry, InodePtr inode = nullptr);
  Dentry(const Dentry& dentry, InodePtr inode);
  ~Dentry();

  // Dentry(const Dentry& other);
  // Dentry& operator=(const Dentry& other);

  const std::string& Name() const { return name_; }
  uint32_t FsId() const { return fs_id_; }
  uint64_t Ino() const { return ino_; }
  uint64_t ParentIno() const { return parent_ino_; }
  pb::mdsv2::FileType Type() const { return type_; }
  uint32_t Flag() const { return flag_; }

  InodePtr Inode() const { return inode_; }

  pb::mdsv2::Dentry CopyTo();

 private:
  std::string name_;
  uint32_t fs_id_;
  uint64_t ino_;
  uint64_t parent_ino_;
  pb::mdsv2::FileType type_;
  uint32_t flag_;

  // maybe null, just inode shortcut
  InodePtr inode_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_DENTRY_H_