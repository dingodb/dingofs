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

#include "client/vfs/meta/meta_system_wrapper.h"

#include <absl/strings/str_format.h>

#include "client/common/status.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/meta/dummy/dummy_filesystem.h"
#include "client/vfs/meta/meta_log.h"
#include "client/vfs/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

Status MetaSystemWrapper::Init() {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("init %s", s.ToString()); });

  meta_system_ = std::make_unique<v2::DummyFileSystem>();
  s = meta_system_->Init();
  return s;
}

void MetaSystemWrapper::UnInit() {
  MetaLogGuard log_guard([&]() { return "uninit"; });
  meta_system_->UnInit();
}

Status MetaSystemWrapper::Lookup(Ino parent, const std::string& name,
                                 Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("lookup (%d,%s): %s %s", parent, name, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->Lookup(parent, name, attr);
  return s;
}

Status MetaSystemWrapper::MkNod(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid, uint32_t mode,
                                uint64_t rdev, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mknod (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->MkNod(parent, name, uid, gid, mode, rdev, attr);
  return s;
}

Status MetaSystemWrapper::Open(Ino ino, int flags, Attr* attr) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("open (%d): %s", ino, s.ToString()); });

  s = meta_system_->Open(ino, flags, attr);
  return s;
}

Status MetaSystemWrapper::Create(Ino parent, const std::string& name,
                                 uint32_t uid, uint32_t gid, uint32_t mode,
                                 int flags, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("create (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  Attr mk_attr;
  s = meta_system_->MkNod(parent, name, uid, gid, mode, 0, &mk_attr);
  s = meta_system_->Open(attr->ino, flags, attr);
  return s;
}

Status MetaSystemWrapper::Close(Ino ino) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("close (%d): %s", ino, s.ToString()); });
  s = meta_system_->Close(ino);
  return s;
}

Status MetaSystemWrapper::ReadSlice(Ino ino, uint64_t index,
                                    std::vector<Slice>* slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_slice (%d,%d): %s", ino, index, s.ToString());
  });

  s = meta_system_->ReadSlice(ino, index, slices);
  return s;
}

Status MetaSystemWrapper::NewSliceId(uint64_t* id) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_slice_id: %d, %s", *id, s.ToString());
  });

  s = meta_system_->NewSliceId(id);
  return s;
}

Status MetaSystemWrapper::WriteSlice(Ino ino, uint64_t index,
                                     const std::vector<Slice>& slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("write_slice (%d,%d): %s", ino, index, s.ToString());
  });

  s = meta_system_->WriteSlice(ino, index, slices);
  return s;
}

Status MetaSystemWrapper::Unlink(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  s = meta_system_->Unlink(parent, name);
  return s;
}

Status MetaSystemWrapper::Rename(Ino old_parent, const std::string& old_name,
                                 Ino new_parent, const std::string& new_name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  s = meta_system_->Rename(old_parent, old_name, new_parent, new_name);
  return s;
}

Status MetaSystemWrapper::Link(Ino ino, Ino new_parent,
                               const std::string& new_name, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  s = meta_system_->Link(ino, new_parent, new_name, attr);
  return s;
}

Status MetaSystemWrapper::Symlink(Ino parent, const std::string& name,
                                  uint32_t uid, uint32_t gid,
                                  const std::string& link, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): (%d,%d) %s %s", parent, name,
                           link, uid, gid, s.ToString(), StrAttr(attr));
  });

  s = meta_system_->Symlink(parent, name, uid, gid, link, attr);
  return s;
}

Status MetaSystemWrapper::ReadLink(Ino ino, std::string* link) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_link (%d): %s %s", ino, s.ToString(), *link);
  });

  s = meta_system_->ReadLink(ino, link);
  return s;
}

Status MetaSystemWrapper::GetAttr(Ino ino, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getattr (%d): %s %s", ino, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->GetAttr(ino, attr);
  return s;
}

Status MetaSystemWrapper::SetAttr(Ino ino, int set, const Attr& in_attr,
                                  Attr* out_attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  s = meta_system_->SetAttr(ino, set, in_attr, out_attr);
  return s;
}

Status MetaSystemWrapper::SetXattr(Ino ino, const std::string& name,
                                   const std::string& value, int flags) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  s = meta_system_->SetXattr(ino, name, value, flags);
  return s;
}

Status MetaSystemWrapper::GetXattr(Ino ino, const std::string& name,
                                   std::string* value) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getxattr (%d,%s): %s %s", ino, name, s.ToString(),
                           *value);
  });

  s = meta_system_->GetXattr(ino, name, value);
  return s;
}

Status MetaSystemWrapper::ListXattr(
    Ino ino, std::map<std::string, std::string>* xattrs) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  s = meta_system_->ListXattr(ino, xattrs);
  return s;
}

Status MetaSystemWrapper::MkDir(Ino parent, const std::string& name,
                                uint32_t uid, uint32_t gid, uint32_t mode,
                                uint64_t rdev, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->MkDir(parent, name, uid, gid, mode, rdev, attr);

  return s;
}

Status MetaSystemWrapper::RmDir(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  s = meta_system_->RmDir(parent, name);
  return s;
}

Status MetaSystemWrapper::OpenDir(Ino ino) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("opendir (%d): %s", ino, s.ToString()); });

  s = meta_system_->OpenDir(ino);
  return s;
}

Status MetaSystemWrapper::NewDirHandler(Ino ino, bool with_attr,
                                        DirHandler** handler) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_dir_handler (%d,%d): %s", ino, with_attr,
                           s.ToString());
  });

  s = meta_system_->NewDirHandler(ino, with_attr, handler);
  return s;
}

Status MetaSystemWrapper::StatFs(Ino ino, FsStat* fs_stat) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = meta_system_->StatFs(ino, fs_stat);
  return s;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
