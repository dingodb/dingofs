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

#include <bthread/bthread.h>
#include <fcntl.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "client/common/const.h"
#include "client/vfs/common/helper.h"
#include "client/vfs/components/uid_gid_mapper.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/data/reader/file_reader.h"
#include "client/vfs/data/writer/file_writer.h"
#include "client/vfs/data_buffer.h"
#include "client/vfs/handle/handle_manager.h"
#include "client/vfs/vfs_fh.h"
#include "client/vfs/vfs_xattr.h"
#include "common/blockaccess/accesser_common.h"
#include "common/const.h"
#include "common/helper.h"
#include "common/metrics/metrics_dumper.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/writer.h"
#include "linux/fs.h"
#include "mds/common/trash.h"
#include "utils/encode.h"
#include "utils/net_common.h"
#include "vfs_meta.h"

#define VFS_CHECK_HANDLE(handle, ino, fh) \
  CHECK((handle) != nullptr)              \
      << "handle is null, ino: " << (ino) << ", fh: " << (fh);

namespace dingofs {
namespace client {
namespace vfs {

// Translate a single host-local uid/gid to the hashed id used by MDS
// (client->mds).
static uint32_t LocalIdToHashedId(UidGidMapper* mapper, UidGidMapper::Kind kind,
                                  uint32_t local_id) {
  return mapper != nullptr ? mapper->LocalIdToHashedId(kind, local_id)
                           : local_id;
}

uint32_t VFSImpl::LocalUidToHashed(uint32_t uid) const {
  return LocalIdToHashedId(vfs_hub_->GetUidGidMapper(),
                           UidGidMapper::Kind::kUid, uid);
}

uint32_t VFSImpl::LocalGidToHashed(uint32_t gid) const {
  return LocalIdToHashedId(vfs_hub_->GetUidGidMapper(),
                           UidGidMapper::Kind::kGid, gid);
}

void VFSImpl::TranslateAttrToLocal(Attr* attr) const {
  auto* mapper = vfs_hub_->GetUidGidMapper();
  if (mapper == nullptr) return;
  mapper->HashedPairToLocal(attr->uid, attr->gid, attr->uid, attr->gid);
}

void VFSImpl::LocalPairToHashed(uint32_t uid, uint32_t gid, uint32_t& out_uid,
                                uint32_t& out_gid) const {
  auto* mapper = vfs_hub_->GetUidGidMapper();
  if (mapper == nullptr) {
    out_uid = uid;
    out_gid = gid;
    return;
  }
  mapper->LocalPairToHashed(uid, gid, out_uid, out_gid);
}

VFSImpl::VFSImpl(const VFSConfig& vfs_conf, const ClientId& client_id)
    : client_id_(client_id),
      mount_root_path_(
          vfs_conf.mount_root_path.empty() ? "/" : vfs_conf.mount_root_path),
      vfs_hub_(std::make_unique<VFSHubImpl>(vfs_conf, client_id_)){};

VFSImpl::VFSImpl(std::unique_ptr<VFSHub> hub)
    : client_id_(), vfs_hub_(std::move(hub)) {
  meta_system_ = vfs_hub_->GetMetaSystem();
  handle_manager_ = vfs_hub_->GetHandleManager();
}

Status VFSImpl::Start(bool skip_mount) {
  CHECK(vfs_hub_ != nullptr) << "vfs_hub is null";

  DINGOFS_RETURN_NOT_OK(vfs_hub_->Start(skip_mount));
  meta_system_ = vfs_hub_->GetMetaSystem();
  handle_manager_ = vfs_hub_->GetHandleManager();

  DINGOFS_RETURN_NOT_OK(ResolveMountRoot());

  DINGOFS_RETURN_NOT_OK(StartBrpcServer());

  return Status::OK();
}

Status VFSImpl::ResolveMountRoot() {
  // Whole-FS mount: nothing to resolve.
  if (mount_root_path_ == "/") {
    mount_root_ino_ = kRootIno;
    return Status::OK();
  }

  CHECK(meta_system_ != nullptr) << "meta_system is null";

  Ino parent = kRootIno;
  ContextSPtr ctx = std::make_shared<Context>("");
  std::vector<std::string> dir_names;
  Helper::SplitString(mount_root_path_, '/', dir_names);
  // remove the empty string before the first '/'
  if (!dir_names.empty()) dir_names.erase(dir_names.begin());
  for (const auto& dir_name : dir_names) {
    if (dir_name.empty()) break;

    Attr attr;
    Status s = meta_system_->Lookup(ctx, parent, dir_name, &attr);
    if (!s.ok()) {
      LOG(ERROR) << fmt::format(
          "resolve mount root fail, dir({}/{}) status({}).", parent, dir_name,
          s.ToString());
      return s;
    }
    if (attr.type != FileType::kDirectory) {
      return Status::NotDirectory(fmt::format("{} is not directory", dir_name));
    }
    parent = attr.ino;
  }

  mount_root_ino_ = parent;

  LOG(INFO) << fmt::format("resolve mount root finish, path({}) ino({}).",
                           mount_root_path_, mount_root_ino_);
  return Status::OK();
}

Status VFSImpl::Stop(bool skip_unmount) { return vfs_hub_->Stop(skip_unmount); }

bool VFSImpl::Dump(ContextSPtr ctx, Json::Value& value) {
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!client_id_.Dump(value)) {
    return false;
  }

  if (!handle_manager_->Dump(value)) {
    return false;
  }

  value["mount_root_path"] = mount_root_path_;
  value["mount_root_ino"] = static_cast<Json::UInt64>(mount_root_ino_);

  return meta_system_->Dump(ctx, value);
}

bool VFSImpl::Load(ContextSPtr ctx, const Json::Value& value) {
  CHECK(handle_manager_ != nullptr) << "handle_manager is null";

  if (!handle_manager_->Load(value)) {
    return false;
  }

  // Validate mount-root state across smooth upgrades.
  if (value.isMember("mount_root_path")) {
    const std::string persisted_path = value["mount_root_path"].asString();
    if (persisted_path != mount_root_path_) {
      LOG(ERROR) << fmt::format(
          "mount root path mismatch on upgrade: persisted({}) current({})",
          persisted_path, mount_root_path_);
      return false;
    }
    if (value.isMember("mount_root_ino")) {
      Ino persisted_ino = static_cast<Ino>(value["mount_root_ino"].asUInt64());
      if (mount_root_ino_ != kRootIno && persisted_ino != mount_root_ino_) {
        LOG(ERROR) << fmt::format(
            "mount root ino mismatch on upgrade: persisted({}) resolved({})",
            persisted_ino, mount_root_ino_);
        return false;
      }
      // Reuse persisted ino so the new process points at the same directory
      // even if path resolution has not yet been performed.
      mount_root_ino_ = persisted_ino;
    }
  }

  return meta_system_->Load(ctx, value);
}

double VFSImpl::GetAttrTimeout(const FileType& type) {  // NOLINT
  return FLAGS_fuse_attr_cache_timeout_s;
}

double VFSImpl::GetEntryTimeout(const FileType& type) {  // NOLINT
  return FLAGS_fuse_entry_cache_timeout_s;
}

static Attr GenerateTrashDirAttr() {
  Attr attr;
  attr.ino = kTrashIno;
  attr.mode = S_IFDIR | 0555;
  attr.nlink = 2;
  attr.length = 4096;
  attr.uid = 0;
  attr.gid = 0;

  // Align timestamps to the start of the current hour so they match the
  // hour-bucket that the next delete will materialize under .trash. Stable
  // within an hour (kernel attr cache hits); at hour boundaries the stamp
  // advances by exactly one hour. Trash content freshness is still guaranteed
  // by `cache_readdir=0` and by the MDS-side partition bypass on trash inodes.
  const uint64_t now_s = static_cast<uint64_t>(::time(nullptr));
  const uint64_t hour_aligned_s = (now_s / 3600ULL) * 3600ULL;
  const uint64_t t_ns = hour_aligned_s * 1000000000ULL;
  attr.atime = t_ns;
  attr.mtime = t_ns;
  attr.ctime = t_ns;

  attr.type = FileType::kDirectory;

  return attr;
}

// Trash protection helper for create-style operations: returns true if
// creating a child of `parent` named `name` is forbidden, either because
// `parent` is in the trash range, or because this would create the
// synthesized .trash dir at the FS root. Used by MkNod / MkDir / Symlink /
// Link to keep one canonical predicate. Each call site keeps its own
// op-specific error string.
static bool IsCreateInTrashForbidden(Ino parent, const std::string& name) {
  return mds::IsTrashInode(parent) ||
         (parent == kRootIno && name == kTrashDirName);
}

Status VFSImpl::Lookup(ContextSPtr ctx, Ino parent, const std::string& name,
                       Attr* attr) {
  // check if parent is root inode and file name is .stats name
  if (BAIDU_UNLIKELY(parent == kRootIno && name == kStatsName)) {  // stats node
    *attr = GenerateVirtualInodeAttr(kStatsIno);
    return Status::OK();
  }

  // check if parent is root inode and name is .trash
  if (BAIDU_UNLIKELY(parent == kRootIno && name == kTrashDirName)) {
    if (!IsTrashVisible()) return Status::NotFound("trash disabled");
    *attr = GenerateTrashDirAttr();
    return Status::OK();
  }

  Status s = meta_system_->Lookup(ctx, TranslateIno(parent), name, attr);
  if (s.ok()) {
    TranslateAttrToLocal(attr);
    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }
  return s;
}

Status VFSImpl::GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    *attr = GenerateVirtualInodeAttr(kStatsIno);
    return Status::OK();
  }

  if (BAIDU_UNLIKELY(ino == kTrashIno)) {
    if (!IsTrashVisible()) return Status::NotFound("trash disabled");
    *attr = GenerateTrashDirAttr();
    return Status::OK();
  }

  Status s = meta_system_->GetAttr(ctx, TranslateIno(ino), attr);
  if (s.ok()) {
    RewriteRootAttr(ino, attr);
    TranslateAttrToLocal(attr);
  }

  return s;
}

Status VFSImpl::SetAttr(ContextSPtr ctx, Ino ino, int set, const Attr& in_attr,
                        Attr* out_attr) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::OK();
  }

  Attr send_attr = in_attr;
  if (set & kSetAttrUid) send_attr.uid = LocalUidToHashed(in_attr.uid);
  if (set & kSetAttrGid) send_attr.gid = LocalGidToHashed(in_attr.gid);

  // A size change (truncate) must be ordered after buffered writes: flush them
  // to storage first so the MDS truncate's zero-slices shadow them. Otherwise a
  // write still buffered here could land *after* the truncate and re-expose the
  // bytes the truncate was meant to drop.
  if (set & kSetAttrSize) {
    Status s = handle_manager_->FlushByIno(ino);
    if (!s.ok()) return s;
  }

  Status s =
      meta_system_->SetAttr(ctx, TranslateIno(ino), set, send_attr, out_attr);
  if (s.ok()) {
    RewriteRootAttr(ino, out_attr);
    TranslateAttrToLocal(out_attr);
  }

  // Truncate (size change) must invalidate cached read buffers — readahead
  // buffers in FileReader::requests_ are indexed by (ino, frange) and would
  // otherwise serve stale data after the inode shrinks/grows.
  if (s.ok() && (set & kSetAttrSize)) {
    handle_manager_->InvalidateByIno(ino, 0,
                                     std::numeric_limits<int64_t>::max());
  }

  return s;
}

Status VFSImpl::Fallocate(ContextSPtr ctx, Ino ino, int mode, uint64_t offset,
                          uint64_t length) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoPermitted("fallocate on internal node");
  }

  // Same ordering requirement as truncate: PUNCH_HOLE / ZERO_RANGE write zero
  // slices that must shadow already-written data, so flush buffered writes
  // first — otherwise a late-flushed write lands after and survives the hole.
  Status s = handle_manager_->FlushByIno(ino);
  if (!s.ok()) return s;

  s = meta_system_->Fallocate(ctx, TranslateIno(ino), mode, offset, length);
  // Mirror the SetAttr(size) path: PUNCH_HOLE / ZERO_RANGE / extending the
  // file all change byte contents in [offset, offset+length); cached readahead
  // buffers in FileReader::requests_ on the same fd would otherwise serve
  // stale bytes for the affected range.
  if (s.ok()) {
    handle_manager_->InvalidateByIno(ino, static_cast<int64_t>(offset),
                                     static_cast<int64_t>(length));
  }

  return s;
}

Status VFSImpl::CopyFileRange(ContextSPtr ctx, Ino src_ino, uint64_t src_off,
                              uint64_t src_fh, Ino dst_ino, uint64_t dst_off,
                              uint64_t dst_fh, uint64_t len, uint32_t flags,
                              uint64_t* bytes_copied) {
  CHECK(bytes_copied != nullptr);
  *bytes_copied = 0;

  if (BAIDU_UNLIKELY(IsInternalNode(src_ino) || IsInternalNode(dst_ino))) {
    return Status::NoPermitted("copy_file_range on internal node");
  }
  // Linux kernel currently passes flags == 0; reject anything else so we
  // surface kernel/protocol drift rather than silently ignore it.
  if (flags != 0) {
    return Status::InvalidParam("unsupported copy_file_range flags");
  }
  if (len == 0) return Status::OK();

  // Same-file overlap is undefined by POSIX; reject before touching MDS.
  if (src_ino == dst_ino) {
    const uint64_t src_end = src_off + len;
    const uint64_t dst_end = dst_off + len;
    if (src_off < dst_end && dst_off < src_end) {
      return Status::InvalidParam("overlapping ranges in same file");
    }
  }

  // Flush both files' buffered writes so MDS observes durable slices.
  // Use FlushByIno to cover all open handles for each inode.
  Status s = handle_manager_->FlushByIno(src_ino);
  if (!s.ok()) return s;
  if (dst_ino != src_ino) {
    s = handle_manager_->FlushByIno(dst_ino);
    if (!s.ok()) return s;
  }

  s = meta_system_->CopyFileRange(ctx, TranslateIno(src_ino), src_off,
                                  TranslateIno(dst_ino), dst_off, len, flags,
                                  bytes_copied);
  if (!s.ok()) return s;

  // Invalidate any in-flight reader caches for the rewritten dst range.
  if (*bytes_copied > 0) {
    handle_manager_->InvalidateByIno(dst_ino, static_cast<int64_t>(dst_off),
                                     static_cast<int64_t>(*bytes_copied));
  }

  return s;
}

Status VFSImpl::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  return meta_system_->ReadLink(ctx, TranslateIno(ino), link);
}

Status VFSImpl::MkNod(ContextSPtr ctx, Ino parent, const std::string& name,
                      uint32_t uid, uint32_t gid, uint32_t mode, uint64_t dev,
                      Attr* attr) {
  if (IsCreateInTrashForbidden(parent, name)) {
    return Status::NoPermitted("cannot mknod in trash");
  }

  uint32_t s_uid = 0, s_gid = 0;
  LocalPairToHashed(uid, gid, s_uid, s_gid);
  Status s = meta_system_->MkNod(ctx, TranslateIno(parent), name, s_uid, s_gid,
                                 mode, dev, attr);
  if (s.ok()) {
    TranslateAttrToLocal(attr);
    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }

  return s;
}

Status VFSImpl::Unlink(ContextSPtr ctx, Ino parent, const std::string& name) {
  if (IsInternalName(name) && parent == kRootIno) {
    LOG(WARNING) << "Can not unlink internal node, parent inodeId=" << parent
                 << ", name: " << name;
    return Status::NoPermitted("Can not unlink internal node");
  }
  if (mds::IsTrashInode(parent) && ctx->uid != 0) {
    LOG(WARNING) << "Non-root cannot manually clean trash, parent inodeId="
                 << parent << ", name: " << name;
    return Status::NoPermitted("only root can manually clean trash");
  }

  return meta_system_->Unlink(ctx, TranslateIno(parent), name);
}

Status VFSImpl::Symlink(ContextSPtr ctx, Ino parent, const std::string& name,
                        uint32_t uid, uint32_t gid, const std::string& link,
                        Attr* attr) {
  // Trash protection: cannot create new entries inside .trash/sub-buckets,
  // nor a symlink literally named .trash at root.
  if (IsCreateInTrashForbidden(parent, name)) {
    return Status::NoPermitted("cannot symlink in trash");
  }
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

  uint32_t s_uid = 0, s_gid = 0;
  LocalPairToHashed(uid, gid, s_uid, s_gid);
  Status s = meta_system_->Symlink(ctx, TranslateIno(parent), name, s_uid,
                                   s_gid, link, attr);
  if (s.ok()) TranslateAttrToLocal(attr);
  return s;
}

Status VFSImpl::Rename(ContextSPtr ctx, Ino old_parent,
                       const std::string& old_name, Ino new_parent,
                       const std::string& new_name) {
  // internel name can not be rename or rename to
  if ((IsInternalName(old_name) || IsInternalName(new_name)) &&
      old_parent == kRootIno) {
    return Status::NoPermitted("Can not rename internal node");
  }

  // Rename into .trash or any sub-trash bucket is forbidden for everyone.
  if (mds::IsTrashInode(new_parent)) {
    return Status::NoPermitted("cannot move into trash");
  }

  // Rename out of .trash root requires root; hour buckets fall through to
  // MDS-side manual-cleanup rules.
  if (old_parent == kTrashIno && ctx->uid != 0) {
    return Status::NoPermitted("only root can move out of .trash");
  }

  // TODO: maybe call file suffix watcher to forget old name?
  return meta_system_->Rename(ctx, TranslateIno(old_parent), old_name,
                              TranslateIno(new_parent), new_name);
}

Status VFSImpl::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                     const std::string& new_name, Attr* attr) {
  // Trash protection: cannot create a hard link inside .trash/sub-buckets,
  // nor a hardlink literally named .trash at root.
  if (IsCreateInTrashForbidden(new_parent, new_name)) {
    return Status::NoPermitted("cannot link in trash");
  }
  // cant't allow  ln   <file> .stats
  // cant't allow  ln  .stats  <file>
  if (IsInternalNode(ino) ||
      (new_parent == kRootIno && IsInternalName(new_name))) {
    return Status::NoPermitted("Can not link internal node");
  }

  Status s = meta_system_->Link(ctx, TranslateIno(ino),
                                TranslateIno(new_parent), new_name, attr);
  if (s.ok()) {
    TranslateAttrToLocal(attr);
    vfs_hub_->GetFileSuffixWatcher()->Forget(ino);
  }

  return s;
}

Status VFSImpl::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t* fh) {
  // check if ino is .stats inode,if true ,get metric data and generate
  // inodeattr information
  uint64_t gfh = vfs::FhGenerator::GenFh();

  // MDS rejects write ops on trashed inodes, but the async-open fast path
  // returns OK before the MDS response is known — without this gate the
  // caller's first write would queue and retry until timeout.
  if ((flags & (O_WRONLY | O_RDWR | O_TRUNC | O_APPEND)) &&
      meta_system_->IsInodeInTrash(ctx, ino)) {
    return Status::NoPermitted("cannot open trashed inode for write");
  }

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

    auto* handler = new Handle();
    handler->fh = gfh;
    handler->ino = kStatsIno;
    handler->file_buffer.size = len;
    handler->file_buffer.data = std::move(file_data_ptr);

    if (!handle_manager_->AddHandle(handler)) {
      delete handler;
      return Status::BadFd("handle manager stopped");
    }

    *fh = handler->fh;
    return Status::OK();
  }

  Status s = meta_system_->Open(ctx, TranslateIno(ino), flags, gfh);
  if (s.ok()) {
    auto* handle = handle_manager_->NewHandle(gfh, ino, flags);
    if (handle == nullptr) {
      return Status::Internal("NewHandle failed");
    }
    *fh = handle->fh;
  }

  return s;
}

Status VFSImpl::Create(ContextSPtr ctx, Ino parent, const std::string& name,
                       uint32_t uid, uint32_t gid, uint32_t mode, int flags,
                       uint64_t* fh, Attr* attr) {
  if (IsCreateInTrashForbidden(parent, name)) {
    return Status::NoPermitted("cannot create in trash");
  }

  uint64_t gfh = vfs::FhGenerator::GenFh();
  uint32_t s_uid = 0, s_gid = 0;
  LocalPairToHashed(uid, gid, s_uid, s_gid);
  Status s = meta_system_->Create(ctx, TranslateIno(parent), name, s_uid, s_gid,
                                  mode, flags, attr, gfh);
  if (s.ok()) {
    TranslateAttrToLocal(attr);
    CHECK_GT(attr->ino, 0) << "ino in attr is null";
    Ino ino = attr->ino;

    auto* handle = handle_manager_->NewHandle(gfh, ino, flags);
    if (handle == nullptr) {
      return Status::Internal("NewHandle failed");
    }
    *fh = handle->fh;

    vfs_hub_->GetFileSuffixWatcher()->Remeber(*attr, name);
  }

  return s;
}

Status VFSImpl::Read(ContextSPtr ctx, Ino ino, DataBuffer* data_buffer,
                     uint64_t size, uint64_t offset, uint64_t fh,
                     uint64_t* out_rsize) {
  Status s;
  auto handle = handle_manager_->FindHandlerGuard(fh);
  VFS_CHECK_HANDLE(handle.get(), ino, fh);
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

  if (handle->resources.reader == nullptr) {
    LOG(ERROR) << "reader is null in handle, ino: " << ino << ", fh: " << fh;
    s = Status::BadFd(fmt::format("bad fh:{}", fh));
    SpanScope::SetStatus(span, s);
    return s;
  }

  if (FLAGS_vfs_tiny_file_data_enable) {
    // read from meta system
    s = meta_system_->Read(SpanScope::GetContext(span), ino, fh, offset, size,
                           *data_buffer, *out_rsize);
    if (!s.IsNoData()) {
      SpanScope::SetStatus(span, s);
      return s;
    }
  }

  {
    auto flush_span = vfs_hub_->GetTraceManager()->StartChildSpan(
        "VFSImpl::Read.Flush", span);
    // Flush all writers for this inode across all open file handles.
    // This ensures read-after-write consistency when multiple file descriptors
    // are open for the same inode: data buffered by any writer fd is flushed
    // to storage before this read proceeds.
    s = handle_manager_->FlushByIno(ino);
    if (!s.ok()) {
      SpanScope::SetStatus(flush_span, s);
      return s;
    }
  }

  s = handle->resources.reader->Read(SpanScope::GetContext(span), data_buffer,
                                     size, offset, out_rsize);
  SpanScope::SetStatus(span, s);

  return s;
}

Status VFSImpl::Write(ContextSPtr ctx, Ino ino, const char* buf, uint64_t size,
                      uint64_t offset, uint64_t fh, uint64_t* out_wsize) {
  // Define the out-param on every path: the read-only-fh early return below
  // leaves it 0 instead of relying on the caller to pre-zero.
  *out_wsize = 0;

  Status s;
  auto handle = handle_manager_->FindHandlerGuard(fh);
  VFS_CHECK_HANDLE(handle.get(), ino, fh);

  auto span = vfs_hub_->GetTraceManager()->StartChildSpan("VFSImpl::Write",
                                                          ctx->GetTraceSpan());

  if (handle->resources.writer == nullptr) {
    LOG(ERROR) << "writer is null (read-only fh), ino: " << ino
               << ", fh: " << fh;
    s = Status::BadFd(fmt::format("read-only fh:{}", fh));
    return s;
  }

  s = handle->resources.writer->Write(SpanScope::GetContext(span), buf, size,
                                      offset, out_wsize);
  // Use *out_wsize, not size: writer->Write may short-write (OK with
  // *out_wsize < size) when the page pool is exhausted mid-write. Metadata must
  // reflect only what is durable -- MetaSystem::Write extends the inode length
  // to offset + len, so passing the full size would claim bytes that were never
  // written (reads past the durable prefix would see a phantom hole).
  if (s.ok() && *out_wsize > 0) {
    s = meta_system_->Write(SpanScope::GetContext(span), ino, buf, offset,
                            *out_wsize, fh);
    // Invalidate read cache for all handles of this inode in the written range,
    // so that no other open file descriptor can serve stale cached data.
    handle_manager_->InvalidateByIno(ino, static_cast<int64_t>(offset),
                                     static_cast<int64_t>(*out_wsize));
  }

  // No status logging here: VFSImpl returns Status without logging per-op
  // outcomes (like the other ops); the uniform status + out_wsize record lives
  // in VFSWrapper's access log, the pool-pressure locality in SliceWriter, and
  // the failure rate in the vfs_write_pool_alloc_fail_num metric.
  return s;
}

Status VFSImpl::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  if (BAIDU_UNLIKELY(IsInternalNode(ino))) {
    return Status::OK();
  }

  Status s;
  auto handle = handle_manager_->FindHandlerGuard(fh);
  VFS_CHECK_HANDLE(handle.get(), ino, fh);

  // O_RDONLY fh has no writer — nothing to flush at the data layer.
  if (handle->resources.writer != nullptr) {
    s = handle->resources.writer->Flush();
    if (!s.ok()) return s;
  }

  s = meta_system_->Flush(ctx, ino, fh);

  return s;
}

Status VFSImpl::Release(ContextSPtr ctx, Ino ino, uint64_t fh) {
  if (BAIDU_UNLIKELY(IsInternalNode(ino))) {
    handle_manager_->ReleaseHandler(fh);
    return Status::OK();
  }

  Status s;
  auto handle = handle_manager_->FindHandlerForRelease(fh);
  if (!handle) {
    VLOG(1) << "Release ignored, fh not found, ino: " << ino << ", fh: " << fh;
    return Status::OK();
  }
  VFS_CHECK_HANDLE(handle.get(), ino, fh);

  // If Stop() has already detached reader/writer resources, this late release
  // must only remove the fh identity. The stop/dump path owns the remaining
  // metadata state; calling meta Close here could race with meta teardown or
  // erase state needed by hot-upgrade dump.
  const bool resources_detached = (handle->resources.reader == nullptr &&
                                   handle->resources.writer == nullptr);

  // Per-fh reader is closed here when resources are still live; writer release
  // happens via ~Handle when ReleaseHandler triggers refs→0.
  if (handle->resources.reader != nullptr) {
    handle->resources.reader->Close();
  }
  if (!resources_detached) {
    s = meta_system_->Close(ctx, ino, fh);
  }

  handle_manager_->ReleaseHandler(fh);

  return s;
}

// TODO: seperate data flush with metadata flush
Status VFSImpl::Fsync(ContextSPtr ctx, Ino ino, int datasync, uint64_t fh) {
  Status s;
  auto handle = handle_manager_->FindHandlerGuard(fh);
  VFS_CHECK_HANDLE(handle.get(), ino, fh);

  if (handle->resources.writer != nullptr) {
    s = handle->resources.writer->Flush();
  }

  if (datasync == 0) {
    s = meta_system_->Flush(ctx, ino, fh);
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

  return meta_system_->SetXattr(ctx, TranslateIno(ino), name, value, flags);
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

  return meta_system_->GetXattr(ctx, TranslateIno(ino), name, value);
}

Status VFSImpl::RemoveXattr(ContextSPtr ctx, Ino ino, const std::string& name) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->RemoveXattr(ctx, TranslateIno(ino), name);
}

Status VFSImpl::ListXattr(ContextSPtr ctx, Ino ino,
                          std::vector<std::string>* xattrs) {
  if (BAIDU_UNLIKELY(ino == kStatsIno)) {
    return Status::NoData("No Xattr data in .stats");
  }

  return meta_system_->ListXattr(ctx, TranslateIno(ino), xattrs);
}

Status VFSImpl::MkDir(ContextSPtr ctx, Ino parent, const std::string& name,
                      uint32_t uid, uint32_t gid, uint32_t mode, Attr* attr) {
  if (IsCreateInTrashForbidden(parent, name)) {
    return Status::NoPermitted("cannot mkdir in trash");
  }

  uint32_t s_uid = 0, s_gid = 0;
  LocalPairToHashed(uid, gid, s_uid, s_gid);
  Status s = meta_system_->MkDir(ctx, TranslateIno(parent), name, s_uid, s_gid,
                                 mode, attr);
  if (s.ok()) TranslateAttrToLocal(attr);
  return s;
}

Status VFSImpl::OpenDir(ContextSPtr ctx, Ino ino, uint64_t* fh,
                        bool& need_cache) {
  *fh = vfs::FhGenerator::GenFh();

  return meta_system_->OpenDir(ctx, TranslateIno(ino), *fh, need_cache);
}

Status VFSImpl::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh, uint64_t offset,
                        bool with_attr, ReadDirHandler handler,
                        uint32_t& count) {
  // root dir(add .stats file and .trash)
  if (BAIDU_UNLIKELY(ino == kRootIno) && offset == 0) {
    DirEntry stats_entry{kStatsIno, kStatsName,
                         GenerateVirtualInodeAttr(kStatsIno)};
    handler(stats_entry, 1);

    if (IsTrashVisible()) {
      DirEntry trash_entry{kTrashIno, kTrashDirName, GenerateTrashDirAttr()};
      handler(trash_entry, 2);
    }
  }

  auto* mapper = vfs_hub_->GetUidGidMapper();
  ReadDirHandler wrapped =
      (with_attr && mapper != nullptr)
          ? ReadDirHandler(
                [this, &handler](const DirEntry& entry, uint64_t off) {
                  DirEntry e = entry;
                  TranslateAttrToLocal(&e.attr);
                  return handler(e, off);
                })
          : handler;

  return meta_system_->ReadDir(ctx, TranslateIno(ino), fh, offset, with_attr,
                               wrapped, count);
}

Status VFSImpl::ReleaseDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  return meta_system_->ReleaseDir(ctx, TranslateIno(ino), fh);
}

Status VFSImpl::RmDir(ContextSPtr ctx, Ino parent, const std::string& name) {
  if (IsInternalName(name) && parent == kRootIno) {
    return Status::NoPermitted("not permit rmdir internal dir");
  }
  if (parent == kTrashIno) {
    // Hour buckets are GC-managed; never user-removable, even by root.
    return Status::NoPermitted("not permit rmdir trash hour buckets");
  }
  if (mds::IsTrashInode(parent) && ctx->uid != 0) {
    return Status::NoPermitted("only root can manually clean trash");
  }

  return meta_system_->RmDir(ctx, TranslateIno(parent), name);
}

Status VFSImpl::StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
  return meta_system_->StatFs(ctx, TranslateIno(ino), fs_stat);
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

Status VFSImpl::GetInfo(std::string* info) {
  Json::Value root;

  // FS Info
  FsInfo fs_info = vfs_hub_->GetFsInfo();
  root["fs_name"] = fs_info.name;
  root["fs_id"] = fs_info.id;
  root["chunk_size"] = fs_info.chunk_size;
  root["block_size"] = fs_info.block_size;

  // Storage type
  const auto& storage = fs_info.storage_info;
  switch (storage.store_type) {
    case StoreType::kS3:
      root["store_type"] = "S3";
      root["s3_endpoint"] = storage.s3_info.endpoint;
      root["s3_bucket"] = storage.s3_info.bucket_name;
      break;
    case StoreType::kRados:
      root["store_type"] = "Rados";
      root["rados_mon_host"] = storage.rados_info.mon_host;
      root["rados_pool"] = storage.rados_info.pool_name;
      break;
    case StoreType::kLocalFile:
      root["store_type"] = "LocalFile";
      root["local_path"] = storage.file_info.path;
      break;
    default:
      root["store_type"] = "Unknown";
  }

  // Block access options
  blockaccess::BlockAccessOptions opts = vfs_hub_->GetBlockAccesserOptions();
  root["max_inflight_bytes"] = static_cast<Json::UInt64>(
      opts.throttle_options.maxAsyncRequestInflightBytes);
  root["iops_total_limit"] =
      static_cast<Json::UInt64>(opts.throttle_options.iopsTotalLimit);
  root["bps_total_mb"] =
      static_cast<Json::UInt64>(opts.throttle_options.bpsTotalMB);

  Json::StreamWriterBuilder writer;
  writer["indentation"] = "";
  *info = Json::writeString(writer, root);
  return Status::OK();
}

Status VFSImpl::StartBrpcServer() {
  int rc = 0;
  {
    inode_blocks_service_.Init(vfs_hub_.get());

    int rc = brpc_server_.AddService(&inode_blocks_service_,
                                     brpc::SERVER_DOESNT_OWN_SERVICE);
    if (rc != 0) {
      std::string error_msg = fmt::format(
          "Add inode blocks service to brpc server failed, rc: {}", rc);
      LOG(ERROR) << error_msg;
      return Status::Internal(error_msg);
    }
  }

  {
    compact_service_.Init(vfs_hub_.get());
    int rc = brpc_server_.AddService(&compact_service_,
                                     brpc::SERVER_DOESNT_OWN_SERVICE);
    if (rc != 0) {
      std::string error_msg =
          fmt::format("Add compact service to brpc server failed, rc: {}", rc);
      LOG(ERROR) << error_msg;
      return Status::Internal(error_msg);
    }
  }

  {
    client_stat_service_.Init(vfs_hub_.get());

    rc = brpc_server_.AddService(&client_stat_service_,
                                 brpc::SERVER_DOESNT_OWN_SERVICE);
    if (rc != 0) {
      std::string error_msg = fmt::format(
          "Add fuse stat service to brpc server failed, rc: {}", rc);
      LOG(ERROR) << error_msg;
      return Status::Internal(error_msg);
    }
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

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
