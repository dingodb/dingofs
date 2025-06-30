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

#include <fcntl.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "client/common/client_dummy_server_info.h"
#include "client/vfs/data/file.h"
#include "client/vfs/handle/dir_iterator.h"
#include "client/vfs/hub/vfs_hub.h"
#include "client/vfs/meta/meta_log.h"
#include "common/status.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "options/client/options/common_option.h"
#include "utils/configuration.h"
#include "utils/net_common.h"

#define VFS_CHECK_HANDLE(handle, ino, fh) \
  CHECK((handle) != nullptr)              \
      << "handle is null, ino: " << (ino) << ", fh: " << (fh);

namespace dingofs {
namespace client {
namespace vfs {

Status VFSImpl::Start(const VFSConfig& vfs_conf) {  // NOLINT
  vfs_hub_ = std::make_unique<VFSHubImpl>();
  DINGOFS_RETURN_NOT_OK(vfs_hub_->Start(vfs_conf, vfs_option_));

  meta_system_ = vfs_hub_->GetMetaSystem();
  handle_manager_ = vfs_hub_->GetHandleManager();

  DINGOFS_RETURN_NOT_OK(StartBrpcServer());

  return Status::OK();
}

Status VFSImpl::Stop() { return vfs_hub_->Stop(); }

double VFSImpl::GetAttrTimeout(const FileType& type) { return 1; }  // NOLINT

double VFSImpl::GetEntryTimeout(const FileType& type) { return 1; }  // NOLINT

Status VFSImpl::Lookup(Ino parent, const std::string& name, Attr* attr) {
  return meta_system_->Lookup(parent, name, attr);
}

Status VFSImpl::GetAttr(Ino ino, Attr* attr) {
  return meta_system_->GetAttr(ino, attr);
}

Status VFSImpl::SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr) {
  return meta_system_->SetAttr(ino, set, in_attr, out_attr);
}

Status VFSImpl::ReadLink(Ino ino, std::string* link) {
  return meta_system_->ReadLink(ino, link);
}

Status VFSImpl::MkNod(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, uint64_t dev, Attr* attr) {
  return meta_system_->MkNod(parent, name, uid, gid, mode, dev, attr);
}

Status VFSImpl::Unlink(Ino parent, const std::string& name) {
  return meta_system_->Unlink(parent, name);
}

Status VFSImpl::Symlink(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, const std::string& link, Attr* attr) {
  return meta_system_->Symlink(parent, name, uid, gid, link, attr);
}

Status VFSImpl::Rename(Ino old_parent, const std::string& old_name,
                       Ino new_parent, const std::string& new_name) {
  return meta_system_->Rename(old_parent, old_name, new_parent, new_name);
}

Status VFSImpl::Link(Ino ino, Ino new_parent, const std::string& new_name,
                     Attr* attr) {
  return meta_system_->Link(ino, new_parent, new_name, attr);
}

Status VFSImpl::Open(Ino ino, int flags, uint64_t* fh) {  // NOLINT
  HandleSPtr handle = handle_manager_->NewHandle();

  Status s = meta_system_->Open(ino, flags, handle->fh);
  if (!s.ok()) {
    handle_manager_->ReleaseHandler(handle->fh);
  } else {
    handle->ino = ino;
    handle->flags = flags;
    handle->file = std::make_unique<File>(vfs_hub_.get(), ino);
    *fh = handle->fh;

    // TOOD: if flags is O_RDONLY, no need schedule flush
    vfs_hub_->GetPeriodicFlushManger()->SubmitToFlush(handle);
  }

  return s;
}

Status VFSImpl::Create(Ino parent, const std::string& name, uint32_t uid,
                       uint32_t gid, uint32_t mode, int flags, uint64_t* fh,
                       Attr* attr) {
  auto handle = handle_manager_->NewHandle();

  Status s = meta_system_->Create(parent, name, uid, gid, mode, flags, attr,
                                  handle->fh);
  if (!s.ok()) {
    handle_manager_->ReleaseHandler(handle->fh);
  } else {
    CHECK_GT(attr->ino, 0) << "ino in attr is null";
    Ino ino = attr->ino;

    handle->ino = ino;
    handle->flags = flags;
    handle->file = std::make_unique<File>(vfs_hub_.get(), ino);
    *fh = handle->fh;
  }

  return s;
}

Status VFSImpl::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                     uint64_t fh, uint64_t* out_rsize) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("read (%d,%d,%d,%d): %s", ino, offset, size, fh,
                           s.ToString());
  });

  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));
    return s;
  }

  s = handle->file->Flush();
  if (!s.ok()) {
    return s;
  }

  s = handle->file->Read(buf, size, offset, out_rsize);

  return s;
}

Status VFSImpl::Write(Ino ino, const char* buf, uint64_t size, uint64_t offset,
                      uint64_t fh, uint64_t* out_wsize) {
  static std::atomic<int> write_count{0};
  write_count.fetch_add(1);
  Status s;
  MetaLogGuard log_guard([&]() {
    write_count.fetch_sub(1);
    return absl::StrFormat("write (%d,%d,%d,%d): %s %d %d", ino, offset, size,
                           fh, s.ToString(), *out_wsize, write_count.load());
  });

  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));

  } else {
    s = handle->file->Write(buf, size, offset, out_wsize);
  }

  return s;
}

Status VFSImpl::Flush(Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("flush (%d,%d): %s", ino, fh, s.ToString());
  });

  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));

  } else {
    s = handle->file->Flush();
  }

  return s;
}

Status VFSImpl::Release(Ino ino, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("release (%d,%d): %s", ino, fh, s.ToString());
  });

  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));

  } else {
    // how do we return
    s = meta_system_->Close(ino, fh);
  }

  handle_manager_->ReleaseHandler(fh);

  return s;
}

// TODO: seperate data flush with metadata flush
Status VFSImpl::Fsync(Ino ino, int datasync, uint64_t fh) {
  Status s;
  MetaLogGuard log_guard([&]() {
    return absl::StrFormat("fsync (%d,%d,%d): %s", ino, datasync, fh,
                           s.ToString());
  });

  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));

  } else {
    s = handle->file->Flush();
  }

  return s;
}

Status VFSImpl::SetXattr(Ino ino, const std::string& name,
                         const std::string& value, int flags) {
  return meta_system_->SetXattr(ino, name, value, flags);
}

Status VFSImpl::GetXattr(Ino ino, const std::string& name, std::string* value) {
  return meta_system_->GetXattr(ino, name, value);
}

Status VFSImpl::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  return meta_system_->ListXattr(ino, xattrs);
}

Status VFSImpl::MkDir(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, Attr* attr) {
  return meta_system_->MkDir(parent, name, uid, gid, mode, attr);
}

Status VFSImpl::OpenDir(Ino ino, uint64_t* fh) {
  DINGOFS_RETURN_NOT_OK(meta_system_->OpenDir(ino));

  auto handle = handle_manager_->NewHandle();
  handle->ino = ino;

  DirIteratorUPtr dir_iterator(meta_system_->NewDirIterator(ino));
  dir_iterator->Seek();
  handle->dir_iterator = std::move(dir_iterator);

  *fh = handle->fh;

  return Status::OK();
}

Status VFSImpl::ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                        ReadDirHandler handler) {
  auto handle = handle_manager_->FindHandler(fh);
  if (handle == nullptr) {
    return Status::BadFd(fmt::format("bad  fh:{}", fh));
  }

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

  VLOG(1) << fmt::format("read dir ino({}) fh({}) offset({})", ino, fh, offset);
  return Status::OK();
}

Status VFSImpl::ReleaseDir(Ino ino, uint64_t fh) {  // NOLINT
  handle_manager_->ReleaseHandler(fh);
  return Status::OK();
}

Status VFSImpl::RmDir(Ino parent, const std::string& name) {
  return meta_system_->RmDir(parent, name);
}

Status VFSImpl::StatFs(Ino ino, FsStat* fs_stat) {
  return meta_system_->StatFs(ino, fs_stat);
}

uint64_t VFSImpl::GetFsId() { return 10; }

uint64_t VFSImpl::GetMaxNameLength() {
  return vfs_option_.meta_option.max_name_length;
}

Status VFSImpl::StartBrpcServer() {
  brpc::ServerOptions brpc_server_options;
  if (FLAGS_bthread_worker_num > 0) {
    brpc_server_options.num_threads = FLAGS_bthread_worker_num;
  }

  int rc =
      brpc_server_.Start(vfs_option_.dummy_server_port, &brpc_server_options);
  if (rc != 0) {
    std::string error_msg =
        fmt::format("Start brpc dummy server failed, port = {}, rc = {}",
                    vfs_option_.dummy_server_port, rc);

    LOG(ERROR) << error_msg;
    return Status::InvalidParam(error_msg);
  }

  LOG(INFO) << "Start brpc server success, listen port = "
            << vfs_option_.dummy_server_port;

  std::string local_ip;
  if (!utils::NetCommon::GetLocalIP(&local_ip)) {
    std::string error_msg =
        fmt::format("Get local ip failed, please check network configuration");
    LOG(ERROR) << error_msg;
    return Status::Unknown(error_msg);
  }

  ClientDummyServerInfo::GetInstance().SetPort(vfs_option_.dummy_server_port);
  ClientDummyServerInfo::GetInstance().SetIP(local_ip);

  return Status::OK();
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
