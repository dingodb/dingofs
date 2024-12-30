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

#include "bthread/types.h"
#include "dingofs/src/mdsv2/filesystem/inode.h"

namespace dingofs {
namespace mdsv2 {

class Dentry;
using DentryPtr = std::shared_ptr<Dentry>;

class Dentry {
 public:
  Dentry(uint32_t fs_id, const std::string& name);
  ~Dentry();

  static DentryPtr New(uint32_t fs_id, const std::string& name) { return std::make_shared<Dentry>(fs_id, name); }

  const std::string& GetName() const { return name_; }
  uint32_t GetFsId() const { return fs_id_; }

  uint64_t GetIno() const { return ino_; }
  void SetIno(uint64_t ino) { ino_ = ino; }

  uint64_t GetParentIno() const { return parent_ino_; }
  void SetParentIno(uint64_t parent_ino) { parent_ino_ = parent_ino; }

  pb::mdsv2::FileType GetType() const { return type_; }
  void SetType(pb::mdsv2::FileType type) { type_ = type; }

  uint32_t GetFlag() const { return flag_; }
  void SetFlag(uint32_t flag) { flag_ = flag; }

  InodePtr GetInode() const { return inode_; }
  void SetInode(InodePtr inode) { inode_ = inode; }

  std::string SerializeAsString();

  void CopyTo(pb::mdsv2::Dentry* dentry);
  void CopyTo(pb::mdsv2::Dentry& dentry);

  DentryPtr GetChildDentry(const std::string& name);
  std::vector<DentryPtr> GetChildDentries(const std::string& start_name, uint32_t limit, bool is_only_dir);

 private:
  bthread_mutex_t mutex_;

  const std::string name_;
  uint32_t fs_id_;
  uint64_t ino_;         // inode id
  uint64_t parent_ino_;  // parent inode id
  pb::mdsv2::FileType type_;
  uint32_t flag_;

  InodePtr inode_{nullptr};

  std::map<std::string, DentryPtr> child_dentries_;
};

class DentryMap {
 public:
  DentryMap();
  ~DentryMap();

  DentryPtr GetDentry(uint64_t ino);
  DentryPtr GetDentry(const std::string& name);
  void AddDentry(DentryPtr dentry);
  void DeleteDentry(const std::string& name);

 private:
  bthread_mutex_t mutex_;

  // ino: dentry
  std::map<uint64_t, DentryPtr> dentry_map_;

  // name: ino
  std::map<std::string, uint64_t> name_ino_map_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_FILESYSTEM_DENTRY_H_