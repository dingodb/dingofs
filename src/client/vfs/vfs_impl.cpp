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
#include "client/vfs/dir_handler.h"
#include "client/vfs/meta/meta_system_wrapper.h"

namespace dingofs {
namespace client {
namespace vfs {

Status VFSImpl::Start(const VFSConfig& vfs_conf) {
  if (started_.load()) {
    return Status::OK();
  }

  meta_system_wrapper_ = std::make_unique<MetaSystemWrapper>();
  DINGOFS_RETURN_NOT_OK(meta_system_wrapper_->Init());

  handle_manager_ = std::make_unique<HandleManager>();

  started_.store(true);
  return Status::OK();
}

Status VFSImpl::Stop() {
  if (!started_.load()) {
    return Status::OK();
  }

  started_.store(true);

  meta_system_wrapper_->UnInit();

  return Status::OK();
}

bool VFSImpl::EnableSplice() { return true; }

double VFSImpl::GetAttrTimeout(const FileType& type) { return 1; }

double VFSImpl::GetEntryTimeout(const FileType& type) { return 1; }

Status VFSImpl::Lookup(Ino parent, const std::string& name, Attr* attr) {
  return meta_system_wrapper_->Lookup(parent, name, attr);
}

Status VFSImpl::GetAttr(Ino ino, Attr* attr) {
  return meta_system_wrapper_->GetAttr(ino, attr);
}

Status VFSImpl::SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr) {
  return meta_system_wrapper_->SetAttr(ino, set, in_attr, out_attr);
}

Status VFSImpl::ReadLink(Ino ino, std::string* link) {
  return meta_system_wrapper_->ReadLink(ino, link);
}

Status VFSImpl::MkNod(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, uint64_t dev, Attr* attr) {
  return Status::OK();
}

Status VFSImpl::Unlink(Ino parent, const std::string& name) {
  return meta_system_wrapper_->Unlink(parent, name);
}

Status VFSImpl::Symlink(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, const std::string& link, Attr* attr) {
  return Status::OK();
}

Status VFSImpl::Rename(Ino old_parent, const std::string& old_name,
                       Ino new_parent, const std::string& new_name) {
  return meta_system_wrapper_->Rename(old_parent, old_name, new_parent,
                                      new_name);
}

Status VFSImpl::Link(Ino ino, Ino new_parent, const std::string& new_name,
                     Attr* attr) {
  return meta_system_wrapper_->Link(ino, new_parent, new_name, attr);
}

Status VFSImpl::Open(Ino ino, int flags, uint64_t* fh, Attr* attr) {
  return meta_system_wrapper_->Open(ino, flags, attr);
}

Status VFSImpl::Create(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                       Attr* attr) {
  return meta_system_wrapper_->Create(parent, name, uid, gid, mode, flags,
                                      attr);
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

Status VFSImpl::SetXAttr(Ino ino, const std::string& name,
                         const std::string& value, int flags) {
  return meta_system_wrapper_->SetXattr(ino, name, value, flags);
}

Status VFSImpl::GetXAttr(Ino ino, const std::string& name, std::string* value) {
  return meta_system_wrapper_->GetXattr(ino, name, value);
}

Status VFSImpl::ListXAttr(Ino ino, std::vector<std::string>* xattrs) {
  return Status::NotSupport("no implement");
}

Status VFSImpl::MkDir(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, Attr* attr) {
  return Status::NotSupport("no implement");
}

Status VFSImpl::OpenDir(Ino ino, uint64_t* fh) {
  DINGOFS_RETURN_NOT_OK(meta_system_wrapper_->OpenDir(ino));

  auto* handle = handle_manager_->NewHandle(ino);
  *fh = handle->fh;
  return Status::OK();
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
    DINGOFS_RETURN_NOT_OK(
        meta_system_wrapper_->NewDirHandler(ino, with_attr, &dir_handler));
    handle->dir_handler.reset(dir_handler);
  }

  CHECK_NOTNULL(handle->dir_handler);

  auto* dir_handler = handle->dir_handler.get();

  if (dir_handler->Offset() != offset) {
    Status s = dir_handler->Seek(offset);
    if (!s.ok()) {
      LOG(WARNING) << "Fail Seek in ReadDir, inodeId=" << ino << ", fh: " << fh
                   << ", offset: " << offset << ", status: " << s.ToString();
      return s;
    }
  }

  while (dir_handler->HasNext()) {
    DirEntry entry;
    Status s = dir_handler->Next(&entry);
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
  return Status::OK();
}

Status VFSImpl::ReleaseDir(Ino ino, uint64_t fh) {
  handle_manager_->ReleaseHandler(fh);
  return Status::OK();
}

Status VFSImpl::RmDir(Ino parent, const std::string& name) {
  return meta_system_wrapper_->RmDir(parent, name);
}

Status VFSImpl::StatFs(Ino ino, FsStat* fs_stat) {
  return meta_system_wrapper_->StatFs(ino, fs_stat);
}

uint64_t VFSImpl::GetFsId() { return 10; }

uint64_t VFSImpl::GetMaxNameLength() { return 255; }

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
