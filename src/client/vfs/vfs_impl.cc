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

#include <cstdint>
#include <memory>
#include <string>

#include "cache/metric/cache_status.h"
#include "client/common/const.h"
#include "client/memory/page_allocator.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/file.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_xattr.h"
#include "common/const.h"
#include "common/metrics/metrics_dumper.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "linux/fs.h"
#include "utils/encode.h"
#include "utils/net_common.h"
#include "vfs_meta.h"

#define VFS_CHECK_HANDLE(handle, ino, fh) \
  CHECK((handle) != nullptr)              \
      << "handle is null, ino: " << (ino) << ", fh: " << (fh);

namespace dingofs {
namespace client {
namespace vfs {

Status VFSImpl::Start(const VFSConfig& vfs_conf, bool upgrade) {
  vfs_hub_ = std::make_unique<VFSHubImpl>(client_id_);
  DINGOFS_RETURN_NOT_OK(vfs_hub_->Start(vfs_conf, upgrade));

  meta_system_ = vfs_hub_->GetMetaSystem();
  handle_manager_ = vfs_hub_->GetHandleManager();

  DINGOFS_RETURN_NOT_OK(StartBrpcServer());

  return Status::OK();
}

Status VFSImpl::Stop(bool upgrade) { return vfs_hub_->Stop(upgrade); }

bool VFSImpl::Dump(ContextSPtr ctx, Json::Value& value) {
  CHECK(meta_system_ != nullptr) << "meta_system is null";
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!client_id_.Dump(value)) {
    return false;
  }

  if (!handle_manager_->Dump(value)) {
    return false;
  }

  return meta_system_->Dump(ctx, value);
}

bool VFSImpl::Load(ContextSPtr ctx, const Json::Value& value) {
  CHECK(meta_system_ != nullptr) << "meta_system is null";
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!handle_manager_->Load(value)) {
    return false;
  }

  return meta_system_->Load(ctx, value);
}

double VFSImpl::GetAttrTimeout(const FileType& type) {  // NOLINT
  return FLAGS_fuse_attr_cache_timeout_s;
}

double VFSImpl::GetEntryTimeout(const FileType& type) {  // NOLINT
  return FLAGS_fuse_entry_cache_timeout_s;
}

Status VFSImpl::Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                       Attr* attr) {
  // check if parent is root inode and file name is .stats name
  if (BAIDU_UNLIKELY(parent == kRootIno && name == kStatsName)) {  // stats node
    *attr = GenerateVirtualInodeAttr(kStatsIno);
    return Status::OK();
  }

  Status s = meta_system_->Lookup(ctx, parent, name, attr);
  if (s.ok()) {
    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }
  return s;
}

Status VFSImpl::GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    *attr = GenerateVirtualInodeAttr(kStatsIno);
    return Status::OK();
  }

  return meta_system_->GetAttr(ctx, ino, attr);
}

Status VFSImpl::SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                        Attr* out_attr) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::OK();
  }

  Status s = meta_system_->SetAttr(ctx, ino, set, in_attr, out_attr);

  return s;
}

Status VFSImpl::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  return meta_system_->ReadLink(ctx, ino, link);
}

Status VFSImpl::MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                      uint32_t uid, uint32_t gid, uint32_t mode, uint64_t dev,
                      Attr* attr) {
  Status s = meta_system_->MkNod(ctx, parent, name, uid, gid, mode, dev, attr);
  if (s.ok()) {
    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }
  return s;
}

Status VFSImpl::Unlink(ContextSPtr ctx, Ino parent, const std::string& name) {
  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == kRootIno) || parent == kRecycleIno) {
    LOG(WARNING) << "Can not unlink internal node, parent inodeId=" << parent
                 << ", name: " << name;
    return Status::NoPermitted("Can not unlink internal node");
  }

  return meta_system_->Unlink(ctx, parent, name);
}

Status VFSImpl::Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, const std::string& link,
                        Attr* attr) {
  {
    // internal file name can not allowed for symlink
    // cant't allow  ln -s  .stats  <file>
    if (parent == kRootIno && IsInternalName(name)) {
      LOG(WARNING) << "Can not symlink internal node, parent inodeId=" << parent
                   << ", name: " << name;
      return Status::NoPermitted("Can not symlink internal node");
    }
    // cant't allow  ln -s <file> .stats
    if (parent == kRootIno && IsInternalName(link)) {
      LOG(WARNING) << "Can not symlink to internal node, parent inodeId="
                   << parent << ", link: " << link;
      return Status::NoPermitted("Can not symlink to internal node");
    }
  }

  return meta_system_->Symlink(ctx, parent, name, uid, gid, link, attr);
}

Status VFSImpl::Rename(ContextSPtr ctx, Ino old_parent,
                       const std::string& old_name, Ino new_parent,
                       const std::string& new_name) {
  // internel name can not be rename or rename to
  if ((IsInternalName(old_name) || IsInternalName(new_name)) &&
      old_parent == kRootIno) {
    return Status::NoPermitted("Can not rename internal node");
  }

  // TODO: maybe call file suffix watcher to forget old name?
  return meta_system_->Rename(ctx, old_parent, old_name, new_parent, new_name);
}

Status VFSImpl::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                     const std::string& new_name, Attr* attr) {
  {
    // cant't allow  ln   <file> .stats
    // cant't allow  ln  .stats  <file>
    if (IsInternalNode(ino) ||
        (new_parent == kRootIno && IsInternalName(new_name))) {
      return Status::NoPermitted("Can not link internal node");
    }
  }

  Status s = meta_system_->Link(ctx, ino, new_parent, new_name, attr);
  if (s.ok()) {
    vfs_hub_->GetFileSuffixWatcher()->Forget(ino);
  }
  return s;
}

Status VFSImpl::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t* fh) {
  // check if ino is .stats inode,if true ,get metric data and generate
  // inodeattr information
  uint64_t gfh = vfs::FhGenerator::GenFh();

  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    // uint64_t gfh = vfs::FhGenerator::GenFh();
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

    auto handler = std::make_shared<Handle>();
    handler->fh = gfh;
    handler->ino = kStatsIno;
    handler->file_buffer.size = len;
    handler->file_buffer.data = std::move(file_data_ptr);

    handle_manager_->AddHandle(handler);

    *fh = handler->fh;

    return Status::OK();
  }

  Status s = meta_system_->Open(ctx, ino, flags, gfh);
  if (s.ok()) {
    auto file = std::make_unique<File>(vfs_hub_.get(), gfh, ino);
    DINGOFS_RETURN_NOT_OK(file->Open());

    // TOOD: if flags is O_RDONLY, no need schedule flush
    auto handle = NewHandle(gfh, ino, flags, std::move(file));
    *fh = handle->fh;
  }

  return s;
}

Status VFSImpl::Create(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                       uint64_t* fh, Attr* attr) {
  uint64_t gfh = vfs::FhGenerator::GenFh();
  Status s =
      meta_system_->Create(ctx, parent, name, uid, gid, mode, flags, attr, gfh);
  if (s.ok()) {
    CHECK_GT(attr->ino, 0) << "ino in attr is null";
    Ino ino = attr->ino;

    auto file = std::make_unique<File>(vfs_hub_.get(), gfh, ino);
    DINGOFS_RETURN_NOT_OK(file->Open());

    // TOOD: if flags is O_RDONLY, no need schedule flush
    auto handle = NewHandle(gfh, ino, flags, std::move(file));
    *fh = handle->fh;

    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }

  return s;
}

Status VFSImpl::Read(ContextSPtr ctx, Ino ino, DataBuffer* data_buffer,
                     uint64_t size, uint64_t offset, uint64_t fh,
                     uint64_t* out_rsize) {
  Status s;
  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);
  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("VFSImpl::Read",
                                                          ctx->GetTraceSpan());

  // read .stats file data
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    size_t file_size = handle->file_buffer.size;
    size_t read_size =
        std::min(size, file_size > offset ? file_size - offset : 0);
    if (read_size > 0) {
      data_buffer->RawIOBuffer()->AppendUserData(
          handle->file_buffer.data.get() + offset, read_size, [](void*) {});
    }
    *out_rsize = read_size;

    return Status::OK();
  }

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));
    span->SetStatus(s);
    return s;
  }

  {
    auto flush_span = vfs_hub_->GetTraceManager()->StartChildSpan(
        "VFSImpl::Read.Flush", span);
    s = handle->file->Flush();
    if (!s.ok()) {
      flush_span->SetStatus(s);
      return s;
    }
  }

  s = handle->file->Read(span->GetContext(), data_buffer, size, offset,
                         out_rsize);
  span->SetStatus(s);
  return s;
}

Status VFSImpl::Write(ContextSPtr ctx, Ino ino, const char* buf, uint64_t size,
                      uint64_t offset, uint64_t fh, uint64_t* out_wsize) {
  Status s;
  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("VFSImpl::Write",
                                                          ctx->GetTraceSpan());

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad  fh:{}", fh));
    return s;
  }

  PageAllocatorStat stat = vfs_hub_->GetPageAllocator()->GetStat();
  if ((stat.free_pages / (double)stat.total_pages) <
      FLAGS_vfs_trigger_flush_free_page_ratio) {
    VLOG(1) << "trigger flush because low memory, page stat: "
            << stat.ToString();
    vfs_hub_->GetHandleManager()->TriggerFlushAll();
  }

  s = handle->file->Write(ctx, buf, size, offset, out_wsize);
  if (s.ok()) {
    s = meta_system_->Write(ctx, ino, offset, size, fh);
    handle->file->Invalidate(offset, size);
  }

  return s;
}

Status VFSImpl::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::OK();
  }

  Status s;
  auto handle = handle_manager_->FindHandler(fh);
  VFS_CHECK_HANDLE(handle, ino, fh);

  if (handle->file == nullptr) {
    LOG(ERROR) << "file is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad fh:{}", fh));
    return s;
  }

  s = handle->file->Flush();
  if (!s.ok()) return s;

  s = meta_system_->Flush(ctx, ino, fh);

  return s;
}

Status VFSImpl::Release(ContextSPtr ctx, Ino ino, uint64_t fh) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
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
    s = meta_system_->Close(ctx, ino, fh);
  }

  handle_manager_->ReleaseHandler(fh);

  return s;
}

// TODO: seperate data flush with metadata flush
Status VFSImpl::Fsync(ContextSPtr ctx, Ino ino, int datasync, uint64_t fh) {
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

Status VFSImpl::SetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                         const std::string& value, int flags) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::OK();
  }

  if (IsWarmupXAttr(name)) {
    LOG(INFO) << fmt::format(
        "Set warmup task context: [key: {}, inodes: ({})].", ino, value);
    vfs_hub_->GetWarmupManager()->SubmitTask(WarmupTaskContext{ino, value});

    return Status::OK();
  }

  return meta_system_->SetXattr(ctx, ino, name, value, flags);
}

Status VFSImpl::GetXattr(ContextSPtr ctx, Ino ino, const std::string& name,
                         std::string* value) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoData("No Xattr data in .stats");
  }

  if (IsWarmupXAttr(name)) {
    *value = vfs_hub_->GetWarmupManager()->GetWarmupTaskStatus(ino);
    LOG(INFO) << "Get warmup task status value: " << *value;
    return Status::OK();
  }

  return meta_system_->GetXattr(ctx, ino, name, value);
}

Status VFSImpl::RemoveXattr(ContextSPtr ctx, Ino ino, const std::string& name) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->RemoveXattr(ctx, ino, name);
}

Status VFSImpl::ListXattr(ContextSPtr ctx, Ino ino,
                          std::vector<std::string>* xattrs) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->ListXattr(ctx, ino, xattrs);
}

Status VFSImpl::MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                      uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) {
  return meta_system_->MkDir(ctx, parent, name, uid, gid, mode, attr);
}

Status VFSImpl::OpenDir(ContextSPtr ctx, Ino ino, uint64_t* fh) {
  *fh = vfs::FhGenerator::GenFh();

  return meta_system_->OpenDir(ctx, ino, *fh);
}

Status VFSImpl::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                        bool with_attr, ReadDirHandler handler) {
  // root dir(add .stats file)
  if (BAIDU_UNLIKELY(ino == kRootIno) && offset == 0) {
    DirEntry stats_entry{kStatsIno, kStatsName,
                         GenerateVirtualInodeAttr(kStatsIno)};
    handler(stats_entry, 1);  // pos 0 is the offset for .stats entry
  }

  return meta_system_->ReadDir(ctx, ino, fh, offset, with_attr, handler);
}

Status VFSImpl::ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  return meta_system_->ReleaseDir(ctx, ino, fh);
}

Status VFSImpl::RmDir(ContextSPtr ctx, Ino parent, const std::string& name) {
  // check if node is recycle or recycle time dir or .stats node
  if ((IsInternalName(name) && parent == kRootIno) || parent == kRecycleIno) {
    return Status::NoPermitted("not permit rmdir internal dir");
  }

  return meta_system_->RmDir(ctx, parent, name);
}

Status VFSImpl::StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
  return meta_system_->StatFs(ctx, ino, fs_stat);
}

Status VFSImpl::Ioctl(ContextSPtr ctx, Ino ino, uint32_t uid, unsigned int cmd,
                      unsigned flags, const void* in_buf, size_t in_bufsz,
                      char* out_buf, size_t out_bufsz) {
  (void)flags;
  // For internal inode, ioctl is not supported
  if (BAIDU_UNLIKELY(IsInternalNode(ino))) {
    return Status::NotSupport("Ioctl is not supported for internal inode");
  }

  static const std::unordered_set<unsigned int> kSupportedIoctls = {
      FS_IOC_SETFLAGS, FS_IOC32_SETFLAGS, FS_IOC_GETFLAGS, FS_IOC32_GETFLAGS,
      FS_IOC_FSGETXATTR};

  if (kSupportedIoctls.find(cmd) == kSupportedIoctls.end()) {
    return Status::NotSupport(fmt::format("ioctl cmd({}) not supported", cmd));
  }

  Attr attr;
  Status s = meta_system_->GetAttr(ctx, ino, &attr);
  if (!s.ok()) {
    return s;
  }

  auto op_code = cmd >> 30;
  if (op_code == 1) {  // set
    uint64_t iflag = 0;

    if (in_bufsz == 8) {
      iflag = utils::DecodeNativeEndian64(static_cast<const char*>(in_buf));
    } else if (in_bufsz == 4) {
      iflag = utils::DecodeNativeEndian32(static_cast<const char*>(in_buf));
    } else {
      return Status::InvalidParam(
          fmt::format("out_bufsz({}) not supported", out_bufsz));
    }

    if (uid != 0) {
      return Status::NoPermission("set flags not allowed for non-root user");
    }

    attr.flags = ((iflag & FS_IMMUTABLE_FL) ? (attr.flags | kFlagImmutable)
                                            : (attr.flags & ~kFlagImmutable));
    attr.flags = ((iflag & FS_APPEND_FL) ? (attr.flags | kFlagAppend)
                                         : (attr.flags & ~kFlagAppend));
    attr.flags = ((iflag & FS_NODUMP_FL) ? (attr.flags | kFlagNoDump)
                                         : (attr.flags & ~kFlagNoDump));
    attr.flags = ((iflag & FS_SYNC_FL) ? (attr.flags | kFlagSync)
                                       : (attr.flags & ~kFlagSync));
    // TODO: FS_NOATIME_FL iflag from fuse is 0xffffff80,  does't match
    // FS_NOATIME_FL(0x00000080)

    VLOG(1) << "ioctl ino: " << ino << ", set iflag : " << iflag
            << ", ioctl attr: " << Attr2Str(attr);

    iflag &= ~(FS_IMMUTABLE_FL | FS_APPEND_FL | FS_NODUMP_FL | FS_SYNC_FL);
    if (iflag != 0) {
      return Status::NotSupport(
          fmt::format("ioctl iflag({}) not supported", iflag));
    }

    Attr out_attr;
    return meta_system_->SetAttr(ctx, ino, kSetAttrFlags, attr, &out_attr);
  } else {
    uint64_t iflag = 0;
    if (((cmd >> 8) & 0xFF) == 'f') {  // FS_IOC_GETFLAGS

      iflag |= ((attr.flags & kFlagImmutable) ? FS_IMMUTABLE_FL : 0);
      iflag |= ((attr.flags & kFlagAppend) ? FS_APPEND_FL : 0);
      iflag |= ((attr.flags & kFlagNoDump) ? FS_NODUMP_FL : 0);
      iflag |= ((attr.flags & kFlagSync) ? FS_SYNC_FL : 0);

      if (out_bufsz == 8) {
        utils::EncodeNativeEndian64(out_buf, iflag);
      } else if (out_bufsz == 4) {
        utils::EncodeNativeEndian32(out_buf, static_cast<uint32_t>(iflag));
      } else {
        return Status::InvalidParam(
            fmt::format("out_bufsz({}) not supported", out_bufsz));
      }
    } else {  // 'X', FS_IOC_FSGETXATTR

      iflag |= ((attr.flags & kFlagImmutable) ? FS_XFLAG_IMMUTABLE : 0);
      iflag |= ((attr.flags & kFlagAppend) ? FS_XFLAG_APPEND : 0);
      iflag |= ((attr.flags & kFlagNoDump) ? FS_XFLAG_NODUMP : 0);
      iflag |= ((attr.flags & kFlagSync) ? FS_XFLAG_SYNC : 0);

      if (out_bufsz == 28) {
        utils::EncodeNativeEndian32(out_buf, static_cast<uint32_t>(iflag));
        std::memset(out_buf + 4, 0, 24);  // fill rest data with zeros
      } else {
        return Status::InvalidParam(
            fmt::format("out_bufsz({}) not supported", out_bufsz));
      }
    }

    VLOG(1) << "ioctl ino: " << ino << ", get iflag: " << iflag;
  }

  return Status::OK();
}

uint64_t VFSImpl::GetFsId() { return 10; }

uint64_t VFSImpl::GetMaxNameLength() { return FLAGS_vfs_meta_max_name_length; }

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

  fuse_stat_service_.Init(vfs_hub_.get());

  rc = brpc_server_.AddService(&fuse_stat_service_,
                               brpc::SERVER_DOESNT_OWN_SERVICE);
  if (rc != 0) {
    std::string error_msg =
        fmt::format("Add fuse stat service to brpc server failed, rc: {}", rc);
    LOG(ERROR) << error_msg;
    return Status::Internal(error_msg);
  }

  auto status = cache::AddCacheService(&brpc_server_);
  if (!status.ok()) {
    return status;
  }

  brpc::ServerOptions brpc_server_options;
  if (FLAGS_vfs_bthread_worker_num > 0) {
    brpc_server_options.num_threads = FLAGS_vfs_bthread_worker_num;
  }

  rc = brpc_server_.Start(FLAGS_vfs_dummy_server_port, &brpc_server_options);
  if (rc != 0) {
    std::string error_msg =
        fmt::format("Start brpc dummy server failed, port = {}, rc = {}",
                    FLAGS_vfs_dummy_server_port, rc);

    LOG(ERROR) << error_msg;
    return Status::InvalidParam(error_msg);
  }

  LOG(INFO) << "Start brpc server success, listen port = "
            << FLAGS_vfs_dummy_server_port;

  std::string local_ip;
  if (!utils::NetCommon::GetLocalIP(&local_ip)) {
    std::string error_msg =
        fmt::format("Get local ip failed, please check network configuration");
    LOG(ERROR) << error_msg;
    return Status::Unknown(error_msg);
  }

  return Status::OK();
}

HandleSPtr VFSImpl::NewHandle(uint64_t fh, Ino ino, int flags, IFileUPtr file) {
  auto handle = std::make_shared<Handle>();
  handle->fh = fh;
  handle->ino = ino;
  handle->flags = flags;
  handle->file = std::move(file);

  handle_manager_->AddHandle(handle);
  return handle;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
