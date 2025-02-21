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

#include <fmt/format.h>
#include <glog/logging.h>

#include <memory>

#include "client/common/status.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/dir_handler.h"
#include "client/vfs/meta/dummy/dummy_filesystem.h"
#include "client/vfs/meta/meta_log.h"

namespace dingofs {
namespace client {
namespace vfs {

Status VFSImpl::Start(const VFSConfig& vfs_conf) {
  if (started_.load()) {
    return Status::OK();
  }

  {
    meta_system_ = std::make_unique<v2::DummyFileSystem>();

    Status s;
    MetaLogGuard log_guard(
        [&]() { return absl::StrFormat("init %s", s.ToString()); });

    s = meta_system_->Init();
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

double VFSImpl::GetAttrTimeout(const FileType& type) { return 1; }

double VFSImpl::GetEntryTimeout(const FileType& type) { return 1; }

Status VFSImpl::Lookup(Ino parent, const std::string& name, Attr* attr) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("lookup (%d,%s): %s %s", parent, name, s.ToString(),
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

Status VFSImpl::Open(Ino ino, int flags, uint64_t* fh) {
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
    auto* handle = handle_manager_->NewHandle(ino);
    *fh = handle->fh;
  }

  return s;
}

Status VFSImpl::NewDirHandler(Ino ino, bool with_attr, DirHandler** handler) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("new_dir_handler (%d,%d): %s", ino, with_attr,
                           s.ToString());
  });

  s = meta_system_->NewDirHandler(ino, with_attr, handler);
  return s;
}

Status VFSImpl::ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                        ReadDirHandler handler) {
  auto* handle = handle_manager_->FindHandler(fh);
  if (handle == nullptr) {
    return Status::BadFd(fmt::format("bad  fh:{}", fh));
  }

  if (handle->dir_handler == nullptr) {
    // Init dir handler
    DirHandler* dir_handler;
    DINGOFS_RETURN_NOT_OK(NewDirHandler(ino, with_attr, &dir_handler));
    handle->dir_handler.reset(dir_handler);
  }

  CHECK_NOTNULL(handle->dir_handler);

  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("readdir (%d,%d): %s (%d)", ino, fh, s.ToString(),
                           offset);
  });

  auto* dir_handler = handle->dir_handler.get();

  if (dir_handler->Offset() != offset) {
    s = dir_handler->Seek(offset);
    if (!s.ok()) {
      LOG(WARNING) << "Fail Seek in ReadDir, inodeId=" << ino << ", fh: " << fh
                   << ", offset: " << offset << ", status: " << s.ToString();
      return s;
    }
  }

  while (dir_handler->HasNext()) {
    DirEntry entry;
    s = dir_handler->Next(&entry);
    if (!s.ok()) {
      LOG(WARNING) << "Fail Next in ReadDir, inodeId=" << ino << ", fh: " << fh
                   << ", offset: " << offset << ", status: " << s.ToString();
      return s;
    }

    uint64_t next_offset = dir_handler->Offset();
    if (!handler(entry, next_offset)) {
      LOG(INFO) << "ReadDir break by handler next_offset: " << next_offset;
      break;
    }
  }

  LOG(INFO) << "ReadDir inodeId=" << ino << ", fh: " << fh
            << ", offset: " << offset
            << " success, next_offset: " << dir_handler->Offset();
  return s;
}

Status VFSImpl::ReleaseDir(Ino ino, uint64_t fh) {
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
