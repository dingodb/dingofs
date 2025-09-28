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

#include "mds/filesystem/dentry.h"

#include <cstdint>

namespace dingofs {
namespace mds {

Dentry::Dentry(uint32_t fs_id, const std::string& name, Ino parent, Ino ino, pb::mds::FileType type, uint32_t flag,
               InodeSPtr inode)
    : fs_id_(fs_id), name_(name), parent_(parent), ino_(ino), type_(type), flag_(flag), inode_(inode) {}

Dentry::Dentry(const pb::mds::Dentry& dentry, InodeSPtr inode)
    : name_(dentry.name()),
      fs_id_(dentry.fs_id()),
      ino_(dentry.ino()),
      parent_(dentry.parent()),
      type_(dentry.type()),
      flag_(dentry.flag()),
      inode_(inode) {}

Dentry::Dentry(const Dentry& dentry, InodeSPtr inode)
    : name_(dentry.Name()),
      fs_id_(dentry.FsId()),
      ino_(dentry.INo()),
      parent_(dentry.ParentIno()),
      type_(dentry.Type()),
      flag_(dentry.Flag()),
      inode_(inode) {}

Dentry::~Dentry() {}  // NOLINT

DentryEntry Dentry::Copy() const {
  DentryEntry dentry;

  dentry.set_fs_id(fs_id_);
  dentry.set_ino(ino_);
  dentry.set_parent(parent_);
  dentry.set_name(name_);
  dentry.set_type(type_);
  dentry.set_flag(flag_);

  return dentry;
}

}  // namespace mds
}  // namespace dingofs