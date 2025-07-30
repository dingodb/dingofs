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

#include "cache/debug/expose.h"
#include "client/common/client_dummy_server_info.h"
#include "client/meta/vfs_fh.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/data/file.h"
#include "client/vfs/hub/vfs_hub.h"
#include "common/define.h"
#include "common/status.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "metrics/metrics_dumper.h"
#include "options/client/common_option.h"
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

bool VFSImpl::Dump(Json::Value& value) {
  CHECK(meta_system_ != nullptr) << "meta_system is null";
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!handle_manager_->Dump(value)) {
    return false;
  }

  return meta_system_->Dump(value);
}

bool VFSImpl::Load(const Json::Value& value) {
  CHECK(meta_system_ != nullptr) << "meta_system is null";
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!handle_manager_->Load(value)) {
    return false;
  }

  return meta_system_->Load(value);
}

double VFSImpl::GetAttrTimeout(const FileType& type) { return 1; }  // NOLINT

double VFSImpl::GetEntryTimeout(const FileType& type) { return 1; }  // NOLINT

Status VFSImpl::Lookup(Ino parent, const std::string& name, Attr* attr) {
  // check if parent is root inode and file name is .stats name
  if (BAIDU_UNLIKELY(parent == ROOTINODEID &&
                     name == STATSNAME)) {  // stats node
    *attr = GenerateVirtualInodeAttr(STATSINODEID);
    return Status::OK();
  }

  return meta_system_->Lookup(parent, name, attr);
}

Status VFSImpl::GetAttr(Ino ino, Attr* attr) {
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    *attr = GenerateVirtualInodeAttr(STATSINODEID);
    return Status::OK();
  }

  return meta_system_->GetAttr(ino, attr);
}

Status VFSImpl::SetAttr(Ino ino, int set, const Attr& in_attr, Attr* out_attr) {
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::OK();
  }

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
  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    LOG(WARNING) << "Can not unlink internal node, parent inodeId=" << parent
                 << ", name: " << name;
    return Status::NoPermitted("Can not unlink internal node");
  }

  return meta_system_->Unlink(parent, name);
}

Status VFSImpl::Symlink(Ino parent, const std::string& name, uint32_t uid,
                        uint32_t gid, const std::string& link, Attr* attr) {
  {
    // internal file name can not allowed for symlink
    // cant't allow  ln -s  .stats  <file>
    if (parent == ROOTINODEID && IsInternalName(name)) {
      LOG(WARNING) << "Can not symlink internal node, parent inodeId=" << parent
                   << ", name: " << name;
      return Status::NoPermitted("Can not symlink internal node");
    }
    // cant't allow  ln -s <file> .stats
    if (parent == ROOTINODEID && IsInternalName(link)) {
      LOG(WARNING) << "Can not symlink to internal node, parent inodeId="
                   << parent << ", link: " << link;
      return Status::NoPermitted("Can not symlink to internal node");
    }
  }

  return meta_system_->Symlink(parent, name, uid, gid, link, attr);
}

Status VFSImpl::Rename(Ino old_parent, const std::string& old_name,
                       Ino new_parent, const std::string& new_name) {
  // internel name can not be rename or rename to
  if ((IsInternalName(old_name) || IsInternalName(new_name)) &&
      old_parent == ROOTINODEID) {
    return Status::NoPermitted("Can not rename internal node");
  }

  return meta_system_->Rename(old_parent, old_name, new_parent, new_name);
}

Status VFSImpl::Link(Ino ino, Ino new_parent, const std::string& new_name,
                     Attr* attr) {
  {
    // cant't allow  ln   <file> .stats
    // cant't allow  ln  .stats  <file>
    if (IsInternalNode(ino) ||
        (new_parent == ROOTINODEID && IsInternalName(new_name))) {
      return Status::NoPermitted("Can not link internal node");
    }
  }

  return meta_system_->Link(ino, new_parent, new_name, attr);
}

Status VFSImpl::Open(Ino ino, int flags, uint64_t* fh) {  // NOLINT
  // check if ino is .stats inode,if true ,get metric data and generate
  // inodeattr information
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    auto handler = handle_manager_->NewHandle();
    *fh = handler->fh;

    MetricsDumper metrics_dumper;
    bvar::DumpOptions opts;
    int ret = bvar::Variable::dump_exposed(&metrics_dumper, &opts);
    std::string contents = metrics_dumper.Contents();

    size_t len = contents.size();
    if (len == 0) {
      return Status::NoData("No data in .stats");
    }

    auto file_data_ptr = std::make_unique<char[]>(len);
    std::memcpy(file_data_ptr.get(), contents.c_str(), len);
    handler->file_buffer.size = len;
    handler->file_buffer.data = std::move(file_data_ptr);

    return Status::OK();
  }

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

    // TOOD: if flags is O_RDONLY, no need schedule flush
    vfs_hub_->GetPeriodicFlushManger()->SubmitToFlush(handle);
  }

  return s;
}

Status VFSImpl::Read(Ino ino, char* buf, uint64_t size, uint64_t offset,
                     uint64_t fh, uint64_t* out_rsize) {
  Status s;
  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  // read .stats file data
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    size_t file_size = handle->file_buffer.size;
    size_t read_size =
        std::min(size, file_size > offset ? file_size - offset : 0);
    if (read_size > 0) {
      std::memcpy(buf, handle->file_buffer.data.get() + offset, read_size);
    }
    *out_rsize = read_size;

    return Status::OK();
  }

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
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::OK();
  }

  Status s;
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
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    handle_manager_->ReleaseHandler(fh);
    return Status::OK();
  }

  Status s;
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
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::OK();
  }

  return meta_system_->SetXattr(ino, name, value, flags);
}

Status VFSImpl::GetXattr(Ino ino, const std::string& name, std::string* value) {
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->GetXattr(ino, name, value);
}

Status VFSImpl::RemoveXattr(Ino ino, const std::string& name) {
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->RemoveXattr(ino, name);
}

Status VFSImpl::ListXattr(Ino ino, std::vector<std::string>* xattrs) {
  if (BAIDU_UNLIKELY(ino == STATSINODEID)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->ListXattr(ino, xattrs);
}

Status VFSImpl::MkDir(Ino parent, const std::string& name, uint32_t uid,
                      uint32_t gid, uint32_t mode, Attr* attr) {
  return meta_system_->MkDir(parent, name, uid, gid, mode, attr);
}

Status VFSImpl::OpenDir(Ino ino, uint64_t* fh) {
  *fh = vfs::FhGenerator::GenFh();

  return meta_system_->OpenDir(ino, *fh);
}

Status VFSImpl::ReadDir(Ino ino, uint64_t fh, uint64_t offset, bool with_attr,
                        ReadDirHandler handler) {
  // root dir(add .stats file)
  if (BAIDU_UNLIKELY(ino == ROOTINODEID) && offset == 0) {
    DirEntry stats_entry{STATSINODEID, STATSNAME,
                         GenerateVirtualInodeAttr(STATSINODEID)};
    handler(stats_entry, 1);  // pos 0 is the offset for .stats entry
  }

  uint64_t to_meta = (offset > 0) ? (offset - 1) : 0;

  return meta_system_->ReadDir(
      ino, fh, to_meta, with_attr,
      [handler](const DirEntry& entry, uint64_t meta_offset) {
        uint64_t return_off = meta_offset + 1;
        return handler(entry, return_off);
      });
}

Status VFSImpl::ReleaseDir(Ino ino, uint64_t fh) {
  return meta_system_->ReleaseDir(ino, fh);
}

Status VFSImpl::RmDir(Ino parent, const std::string& name) {
  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == ROOTINODEID) ||
      parent == RECYCLEINODEID) {
    return Status::NoPermitted("not permit rmdir internal dir");
  }

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
  inode_blocks_service_.Init(vfs_hub_.get());

  int rc = brpc_server_.AddService(&inode_blocks_service_,
                                   brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    std::string error_msg = fmt::format(
        "Add inode blocks service to brpc server failed, rc: {}", rc);
    LOG(ERROR) << error_msg;
    return Status::Internal(error_msg);
  }

  auto status = cache::AddCacheService(&brpc_server_);
  if (!status.ok()) {
    return status;
  }

  brpc::ServerOptions brpc_server_options;
  if (FLAGS_bthread_worker_num > 0) {
    brpc_server_options.num_threads = FLAGS_bthread_worker_num;
  }

  rc = brpc_server_.Start(vfs_option_.dummy_server_port, &brpc_server_options);
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
