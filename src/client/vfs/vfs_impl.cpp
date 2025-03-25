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

#include "client/vfs/vfs_impl.h"

#include <memory>
#include <utility>

#include "client/common/status.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/dir_iterator.h"
#include "client/vfs/meta/dummy/dummy_filesystem.h"
#include "client/vfs/meta/meta_log.h"
#include "client/vfs/meta/v2/filesystem.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "utils/configuration.h"

namespace dingofs {
namespace client {
namespace vfs {

Status VFSImpl::Start(const VFSConfig& vfs_conf) {  // NOLINT
  if (started_.load()) {
    return Status::OK();
  }

  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("init %s", s.ToString()); });

  utils::Configuration conf;
  conf.SetConfigPath(vfs_conf.config_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "load config fail, confPath = " << vfs_conf.config_path;
    return Status::Internal("load config fail");
  }

  if (vfs_conf.fs_type == "vfs_dummy") {
    LOG(INFO) << "use dummy file system.";
    meta_system_ = std::make_unique<dummy::DummyFileSystem>();

  } else if (vfs_conf.fs_type == "vfs_v2") {
    LOG(INFO) << "use mdsv2 file system.";
    std::string mds_addr;
    conf.GetValueFatalIfFail("mds.addr", &mds_addr);
    LOG(INFO) << fmt::format("mds addr: {}.", mds_addr);

    meta_system_ = v2::MDSV2FileSystem::Build(vfs_conf.fs_name, mds_addr,
                                              vfs_conf.mount_point);
  } else {
    LOG(INFO) << fmt::format("not unknown file system {}.", vfs_conf.fs_type);
    return Status::Internal("not unknown file system");
  }

  if (meta_system_ == nullptr) {
    return Status::Internal("build meta system fail");
  }

  s = meta_system_->Init();
  if (!s.ok()) {
    return s;
  }

  handle_manager_ = std::make_unique<HandleManager>();

  started_.store(true);
  return Status::OK();
}

Status VFSImpl::Stop() {
  if (!started_.load()) {
    return Status::OK();
  }

  started_.store(true);

  {
    MetaLogGuard log_guard([&]() { return "uninit"; });
    meta_system_->UnInit();
  }

  return Status::OK();
}

bool VFSImpl::EnableSplice() { return true; }

double VFSImpl::GetAttrTimeout(const FileType& type) { return 1; }  // NOLINT

double VFSImpl::GetEntryTimeout(const FileType& type) { return 1; }  // NOLINT

Status VFSImpl::Lookup(Ino parent, const std::string& name, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("lookup (%d/%s): %s %s", parent, name, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->Lookup(parent, name, attr);
  return s;
}

Status VFSImpl::GetAttr(Ino ino, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getattr (%d): %s %s", ino, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->GetAttr(ino, attr);
  return s;
}

Status VFSImpl::SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setattr (%d,0x%X): %s %s", ino, set, s.ToString(),
                           StrAttr(out_attr));
  });

  s = meta_system_->SetAttr(ino, set, in_attr, out_attr);
  return s;
}

Status VFSImpl::ReadLink(Ino ino, std::string* link) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read_link (%d): %s %s", ino, s.ToString(), *link);
  });

  s = meta_system_->ReadLink(ino, link);
  return s;
}

Status VFSImpl::MkNod(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, uint64_t dev, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mknod (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });
  s = meta_system_->MkNod(parent, name, uid, gid, mode, dev, attr);
  return s;
}

Status VFSImpl::Unlink(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("unlink (%d,%s): %s", parent, name, s.ToString());
  });

  s = meta_system_->Unlink(parent, name);
  return s;
}

Status VFSImpl::Symlink(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, const std::string& link, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("symlink (%d,%s,%s): (%d,%d) %s %s", parent, name,
                           link, uid, gid, s.ToString(), StrAttr(attr));
  });

  s = meta_system_->Symlink(parent, name, uid, gid, link, attr);
  return s;
}

Status VFSImpl::Rename(Ino old_parent, const std::string& old_name,
                       Ino new_parent, const std::string& new_name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rename (%d,%s,%d,%s): %s", old_parent, old_name,
                           new_parent, new_name, s.ToString());
  });

  s = meta_system_->Rename(old_parent, old_name, new_parent, new_name);
  return s;
}

Status VFSImpl::Link(Ino ino, Ino new_parent, const std::string& new_name,
                     Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("link (%d,%d,%s): %s %s", ino, new_parent, new_name,
                           s.ToString(), StrAttr(attr));
  });

  s = meta_system_->Link(ino, new_parent, new_name, attr);
  return s;
}

Status VFSImpl::Open(Ino ino, int flags, uint64_t* fh) {  // NOLINT
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("open (%d): %s", ino, s.ToString()); });

  s = meta_system_->Open(ino, flags);
  return s;
}

Status VFSImpl::Create(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                       Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("create (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->Create(parent, name, uid, gid, mode, flags, attr);
  return s;
}

Status VFSImpl::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                     uint64_t fh, uint64_t* out_rsize) {
  return Status::NotSupport("no implement");
}

Status VFSImpl::Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
                      uint64_t fh, uint64_t* out_wsize) {
  return Status::NotSupport("no implement");
}

Status VFSImpl::Flush(Ino ino, uint64_t fh) { return Status::OK(); }

Status VFSImpl::Release(Ino ino, uint64_t fh) { return Status::OK(); }

Status VFSImpl::Fsync(Ino ino, int datasync, uint64_t fh) {
  return Status::OK();
}

Status VFSImpl::SetXattr(Ino ino, const std::string& name,
                         const std::string& value, int flags) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("setxattr (%d,%s): %s", ino, name, s.ToString());
  });

  s = meta_system_->SetXattr(ino, name, value, flags);
  return s;
}

Status VFSImpl::GetXattr(Ino ino, const std::string& name, std::string* value) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("getxattr (%d,%s): %s %s", ino, name, s.ToString(),
                           *value);
  });

  s = meta_system_->GetXattr(ino, name, value);

  return s;
}

Status VFSImpl::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("listxattr (%d): %s %d", ino, s.ToString(),
                           xattrs->size());
  });

  s = meta_system_->ListXattr(ino, xattrs);
  return s;
}

Status VFSImpl::MkDir(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("mkdir (%d,%s,%s:0%04o): (%d,%d) %s %s", parent,
                           name, StrMode(mode), mode, uid, gid, s.ToString(),
                           StrAttr(attr));
  });

  s = meta_system_->MkDir(parent, name, uid, gid, mode, attr);

  return s;
}

Status VFSImpl::OpenDir(Ino ino, uint64_t* fh) {
  Status s;
  {
    MetaLogGuard log_guard([&]() {
      return absl::StrFormat("opendir (%d): %s", ino, s.ToString());
    });
    s = meta_system_->OpenDir(ino);
  }

  if (s.ok()) {
    DirIteratorUPtr dir_iterator(meta_system_->NewDirIterator(ino));
    dir_iterator->Seek();
    auto handle = handle_manager_->NewHandle(ino, std::move(dir_iterator));
    *fh = handle->fh;
  }

  return s;
}

Status VFSImpl::ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                        ReadDirHandler handler) {
  auto handle = handle_manager_->FindHandler(fh);
  if (handle == nullptr) {
    return Status::BadFd(fmt::format("bad  fh:{}", fh));
  }

  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("readdir (%d,%d): %s (%d)", ino, fh, s.ToString(),
                           offset);
  });

  auto& dir_iterator = handle->dir_iterator;
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  while (dir_iterator->Valid()) {
    DirEntry entry = dir_iterator->GetValue(with_attr);

    if (!handler(entry)) {
      LOG(INFO) << "read dir break by handler next_offset: " << offset;
      break;
    }

    dir_iterator->Next();
  }

  LOG(INFO) << fmt::format("read dir ino({}) fh({}) offset({})", ino, fh,
                           offset);
  return s;
}

Status VFSImpl::ReleaseDir(Ino ino, uint64_t fh) {  // NOLINT
  handle_manager_->ReleaseHandler(fh);
  return Status::OK();
}

Status VFSImpl::RmDir(Ino parent, const std::string& name) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("rmdir (%d,%s): %s", parent, name, s.ToString());
  });

  s = meta_system_->RmDir(parent, name);
  return s;
}

Status VFSImpl::StatFs(Ino ino, FsStat* fs_stat) {
  Status s;
  MetaLogGuard log_guard(
      [&]() { return absl::StrFormat("statfs (%d): %s", ino, s.ToString()); });

  s = meta_system_->StatFs(ino, fs_stat);
  return s;
}

uint64_t VFSImpl::GetFsId() { return 10; }

uint64_t VFSImpl::GetMaxNameLength() { return 255; }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
