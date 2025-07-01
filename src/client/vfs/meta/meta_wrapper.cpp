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

#include "client/vfs/meta/meta_wrapper.h"

#include <absl/strings/str_format.h>

#include "client/vfs/common/helper.h"
#include "client/vfs/meta/meta_log.h"
#include "client/vfs_meta.h"

namespace dingofs {
namespace client {
namespace vfs {

Status MetaWrapper::Init() {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("init %s", s.ToString()); });

  s = target_->Init();
  return s;
}

void MetaWrapper::UnInit() {
  MetaLogGuard log_guard([&]() { return "uninit"; });
  target_->UnInit();
}

bool MetaWrapper::Dump(Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "dump"; });
  return target_->Dump(value);
}

bool MetaWrapper::Load(const Json::Value& value) {
  MetaLogGuard log_guard([&]() { return "load"; });
  return target_->Load(value);
}

Status MetaWrapper::Lookup(Ino parent, const std::string& name, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("lookup (%d/%s): %s %s", parent, name, s.ToString(),
                           StrAttr(attr));
  });

  s = target_->Lookup(parent, name, attr);
  return s;
}

Status MetaWrapper::MkNod(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, uint32_t mode, uint64_t rdev,
                          Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mknod (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });
  s = target_->MkNod(parent, name, uid, gid, mode, rdev, attr);
  return s;
}

Status MetaWrapper::Open(Ino ino, int flags, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("open (%d): %s [fh:%d]", ino, s.ToString(), fh);
  });

  s = target_->Open(ino, flags, fh);
  return s;
}

Status MetaWrapper::Create(Ino parent, const std::string& name, uint32_t uid,
                           uint32_t gid, uint32_t mode, int flags, Attr* attr,
                           uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("create (%d,%s,%s:0%04o): (%d,%d) %s %s [fh:%d]",
                           parent, name, StrMode(mode), mode, uid, gid,
                           s.ToString(), StrAttr(attr), fh);
  });
  s = target_->Create(parent, name, uid, gid, mode, flags, attr, fh);
  return s;
}

Status MetaWrapper::Close(Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("close (%d): %s [fh:%d]", ino, s.ToString(), fh);
  });
  s = target_->Close(ino, fh);
  return s;
}

Status MetaWrapper::Unlink(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  s = target_->Unlink(parent, name);
  return s;
}

Status MetaWrapper::Rename(Ino old_parent, const std::string& old_name,
                           Ino new_parent, const std::string& new_name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  s = target_->Rename(old_parent, old_name, new_parent, new_name);
  return s;
}

Status MetaWrapper::Link(Ino ino, Ino new_parent, const std::string& new_name,
                         Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  s = target_->Link(ino, new_parent, new_name, attr);
  return s;
}

Status MetaWrapper::Symlink(Ino parent, const std::string& name, uint32_t uid,
                            uint32_t gid, const std::string& link, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): (%d,%d) %s %s", parent, name,
                           link, uid, gid, s.ToString(), StrAttr(attr));
  });

  s = target_->Symlink(parent, name, uid, gid, link, attr);
  return s;
}

Status MetaWrapper::ReadLink(Ino ino, std::string* link) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_link (%d): %s %s", ino, s.ToString(), *link);
  });

  s = target_->ReadLink(ino, link);
  return s;
}

Status MetaWrapper::GetAttr(Ino ino, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getattr (%d): %s %s", ino, s.ToString(),
                           StrAttr(attr));
  });

  s = target_->GetAttr(ino, attr);
  return s;
}

Status MetaWrapper::SetAttr(Ino ino, int set, const Attr& in_attr,
                            Attr* out_attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  s = target_->SetAttr(ino, set, in_attr, out_attr);
  return s;
}

Status MetaWrapper::SetXattr(Ino ino, const std::string& name,
                             const std::string& value, int flags) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  s = target_->SetXattr(ino, name, value, flags);
  return s;
}

Status MetaWrapper::GetXattr(Ino ino, const std::string& name,
                             std::string* value) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getxattr (%d,%s): %s %s", ino, name, s.ToString(),
                           *value);
  });

  s = target_->GetXattr(ino, name, value);

  return s;
}

Status MetaWrapper::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  s = target_->ListXattr(ino, xattrs);
  return s;
}

Status MetaWrapper::MkDir(Ino parent, const std::string& name, uint32_t uid,
                          uint32_t gid, uint32_t mode, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = target_->MkDir(parent, name, uid, gid, mode, attr);

  return s;
}

Status MetaWrapper::RmDir(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  s = target_->RmDir(parent, name);
  return s;
}

Status MetaWrapper::OpenDir(Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("opendir (%d): %d %s", ino, fh, s.ToString());
  });

  s = target_->OpenDir(ino, fh);
  return s;
}

Status MetaWrapper::ReadDir(Ino ino, uint64_t fh, uint64_t offset,
                            bool with_attr, ReadDirHandler handler) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("readdir (%d): %d %d %d %s", ino, fh, offset,
                           with_attr, s.ToString());
  });

  s = target_->ReadDir(ino, fh, offset, with_attr, handler);
  return s;
}

Status MetaWrapper::ReleaseDir(Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("releasedir (%d): %d %s", ino, fh, s.ToString());
  });

  s = target_->ReleaseDir(ino, fh);
  return s;
}

Status MetaWrapper::NewSliceId(Ino ino, uint64_t* id) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_slice_id: (%d) %d, %s", ino, *id, s.ToString());
  });

  s = target_->NewSliceId(ino, id);
  return s;
}

Status MetaWrapper::ReadSlice(Ino ino, uint64_t index,
                              std::vector<Slice>* slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_slice (%d,%d): %s %d", ino, index,
                           s.ToString(), slices->size());
  });

  s = target_->ReadSlice(ino, index, slices);
  return s;
}

Status MetaWrapper::WriteSlice(Ino ino, uint64_t index,
                               const std::vector<Slice>& slices) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("write_slice (%d,%d): %s %d", ino, index,
                           s.ToString(), slices.size());
  });

  s = target_->WriteSlice(ino, index, slices);
  return s;
}

Status MetaWrapper::StatFs(Ino ino, FsStat* fs_stat) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = target_->StatFs(ino, fs_stat);
  return s;
}

Status MetaWrapper::GetFsInfo(FsInfo* fs_info) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("get_fsinfo %s", s.ToString()); });

  s = target_->GetFsInfo(fs_info);
  return s;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
