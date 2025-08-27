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

#include "client/vfs/meta/v2/filesystem.h"

#include <fcntl.h>
#include <openssl/rsa.h>

#include <cstdint>
#include <string>
#include <vector>

#include "client/meta/vfs_meta.h"
#include "client/vfs/meta/v2/client_id.h"
#include "client/vfs/meta/v2/helper.h"
#include "common/status.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "glog/logging.h"
#include "json/value.h"
#include "json/writer.h"
#include "trace/context.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const uint32_t kHeartbeatIntervalS = 5;  // seconds

const std::string kSliceIdCacheName = "slice";

static std::string GetHostName() {
  char hostname[kMaxHostNameLength];
  int ret = gethostname(hostname, kMaxHostNameLength);
  if (ret < 0) {
    LOG(ERROR) << "[meta.filesystem] GetHostName fail, ret=" << ret;
    return "";
  }

  return std::string(hostname);
}
DEFINE_bool(client_meta_read_chunk_cache_enable, true,
            "enable read chunk cache");

MDSV2FileSystem::MDSV2FileSystem(mdsv2::FsInfoPtr fs_info,
                                 const ClientId& client_id,
                                 MDSDiscoveryPtr mds_discovery,
                                 InodeCacheSPtr inode_cache,
                                 MDSClientPtr mds_client)
    : name_(fs_info->GetName()),
      client_id_(client_id),
      fs_info_(fs_info),
      mds_discovery_(mds_discovery),
      id_cache_(kSliceIdCacheName, mds_client),
      file_session_map_(fs_info),
      inode_cache_(inode_cache),
      mds_client_(mds_client) {}

MDSV2FileSystem::~MDSV2FileSystem() {}  // NOLINT

Status MDSV2FileSystem::Init() {
  LOG(INFO) << fmt::format("[meta.filesystem] fs_info: {}.",
                           fs_info_->ToString());
  // mount fs
  if (!MountFs()) {
    LOG(ERROR) << fmt::format("[meta.filesystem] mount fs fail.");
    return Status::MountFailed("mount fs fail");
  }

  // init crontab
  if (!InitCrontab()) {
    LOG(ERROR) << fmt::format("[meta.filesystem] init crontab fail.");
    return Status::Internal("init crontab fail");
  }

  return Status::OK();
}

void MDSV2FileSystem::UnInit() {
  // unmount fs
  UnmountFs();

  crontab_manager_.Destroy();
}

bool MDSV2FileSystem::Dump(ContextSPtr, Json::Value& value) {
  if (!file_session_map_.Dump(value)) {
    return false;
  }

  if (!dir_iterator_manager_.Dump(value)) {
    return false;
  }

  if (!mds_client_->Dump(value)) {
    return false;
  }

  return true;
}

bool MDSV2FileSystem::Load(ContextSPtr, const Json::Value& value) {
  if (!file_session_map_.Load(value)) {
    return false;
  }

  if (!dir_iterator_manager_.Load(mds_client_, value)) {
    return false;
  }

  if (!mds_client_->Load(value)) {
    return false;
  }

  return true;
}

Status MDSV2FileSystem::GetFsInfo(ContextSPtr, FsInfo* fs_info) {
  auto temp_fs_info = fs_info_->Get();

  fs_info->name = name_;
  fs_info->id = temp_fs_info.fs_id();
  fs_info->chunk_size = temp_fs_info.chunk_size();
  fs_info->block_size = temp_fs_info.block_size();
  fs_info->uuid = temp_fs_info.uuid();

  fs_info->storage_info.store_type =
      Helper::ToStoreType(temp_fs_info.fs_type());
  if (fs_info->storage_info.store_type == StoreType::kS3) {
    CHECK(temp_fs_info.extra().has_s3_info())
        << "fs type is S3, but s3 info is not set";

    fs_info->storage_info.s3_info =
        Helper::ToS3Info(temp_fs_info.extra().s3_info());

  } else if (fs_info->storage_info.store_type == StoreType::kRados) {
    CHECK(temp_fs_info.extra().has_rados_info())
        << "fs type is Rados, but rados info is not set";

    fs_info->storage_info.rados_info =
        Helper::ToRadosInfo(temp_fs_info.extra().rados_info());

  } else {
    LOG(ERROR) << fmt::format("[meta.filesystem] unknown fs type: {}.",
                              pb::mdsv2::FsType_Name(temp_fs_info.fs_type()));
    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

bool MDSV2FileSystem::MountFs() {
  pb::mdsv2::MountPoint mount_point;
  mount_point.set_client_id(client_id_.ID());
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_path(client_id_.Mountpoint());
  mount_point.set_cto(false);

  LOG(INFO) << fmt::format("[meta.filesystem] mount point: {}.",
                           mount_point.ShortDebugString());

  auto status = mds_client_->MountFs(name_, mount_point);
  if (!status.ok() && status.Errno() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem] mount fs info fail, mountpoint({}), {}.",
        client_id_.Mountpoint(), status.ToString());
    return false;
  }

  return true;
}

bool MDSV2FileSystem::UnmountFs() {
  auto status = mds_client_->UmountFs(name_, client_id_.ID());
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem] mount fs info fail, mountpoint({}).",
        client_id_.Mountpoint());
    return false;
  }

  return true;
}

void MDSV2FileSystem::Heartbeat() {
  // prevent multiple heartbeats running at the same time
  static std::atomic<bool> is_running{false};
  if (is_running.exchange(true)) return;

  auto status = mds_client_->Heartbeat();
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[meta.filesystem] heartbeat fail, error({}).",
                              status.ToString());
  }

  is_running = false;
}

bool MDSV2FileSystem::InitCrontab() {
  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      kHeartbeatIntervalS * 1000,
      true,
      [this](void*) { this->Heartbeat(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

Status MDSV2FileSystem::StatFs(ContextSPtr ctx, Ino, FsStat* fs_stat) {
  auto status = mds_client_->GetFsQuota(ctx, *fs_stat);

  if (fs_stat->max_bytes == 0) {
    fs_stat->max_bytes = INT64_MAX;
  }

  if (fs_stat->max_inodes == 0) {
    fs_stat->max_inodes = INT64_MAX;
  }

  return status;
};

Status MDSV2FileSystem::Lookup(ContextSPtr ctx, Ino parent,
                               const std::string& name, Attr* out_attr) {
  auto status = mds_client_->Lookup(ctx, parent, name, *out_attr);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_FOUND) {
      return Status::NotExist("not found dentry");
    }
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Create(ContextSPtr ctx, Ino parent,
                               const std::string& name, uint32_t uid,
                               uint32_t gid, uint32_t mode, int flags,
                               Attr* attr, uint64_t fh) {
  auto status = MkNod(ctx, parent, name, uid, gid, mode, 0, attr);
  if (!status.ok()) {
    return status;
  }

  return Open(ctx, attr->ino, flags, fh);
}

Status MDSV2FileSystem::MkNod(ContextSPtr ctx, Ino parent,
                              const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, uint64_t rdev,
                              Attr* out_attr) {
  auto status =
      mds_client_->MkNod(ctx, parent, name, uid, gid, mode, rdev, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) {
  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  std::string session_id;
  AttrEntry attr_entry;
  std::vector<mdsv2::ChunkEntry> chunks;
  bool is_prefetch_chunk = FLAGS_client_meta_read_chunk_cache_enable;
  auto status = mds_client_->Open(ctx, ino, flags, is_prefetch_chunk,
                                  session_id, attr_entry, chunks);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}] open file fail, error({}).", ino, fh,
        status.ToString());
    return status;
  }

  LOG(INFO) << fmt::format(
      "[meta.filesystem.{}.{}] open file flags({:o}:{}) session_id({}) "
      "is_prefetch_chunk({}) chunks({}).",
      ino, fh, flags, mdsv2::Helper::DescOpenFlags(flags), session_id,
      is_prefetch_chunk, chunks.size());

  // add file session and chunk
  auto file_session = file_session_map_.Put(ino, fh, session_id);
  if (is_prefetch_chunk && !chunks.empty()) {
    file_session->UpsertChunk(fh, chunks);
  }

  return Status::OK();
}

Status MDSV2FileSystem::Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
  std::string session_id = file_session_map_.GetSessionID(ino, fh);
  CHECK(!session_id.empty())
      << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

  LOG(INFO) << fmt::format("[meta.filesystem.{}.{}] close file session_id({}).",
                           ino, fh, session_id);

  auto status = mds_client_->Release(ctx, ino, session_id);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}] close file fail, error({}).", ino, fh,
        status.ToString());
  }

  // clean cache
  file_session_map_.Delete(ino, fh);

  return Status::OK();
}

Status MDSV2FileSystem::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                  uint64_t fh, std::vector<Slice>* slices) {
  if (fh != 0 && GetSliceFromCache(ino, index, slices)) {
    ctx->hit_cache = true;
    LOG(INFO) << fmt::format(
        "[meta.filesystem.{}.{}.{}] readslice from cache, slices{}.", ino, fh,
        index, Helper::GetSliceIds(*slices));
    return Status::OK();
  }

  std::vector<mdsv2::ChunkEntry> chunks;
  auto status =
      mds_client_->ReadSlice(ctx, ino, {static_cast<uint32_t>(index)}, chunks);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}.{}] reeadslice fail, error({}).", ino, fh,
        index, status.ToString());
    return status;
  }

  if (!chunks.empty()) {
    const auto& chunk = chunks.front();
    for (const auto& slice : chunk.slices()) {
      slices->emplace_back(Helper::ToSlice(slice));
    }
  }

  return Status::OK();
}

Status MDSV2FileSystem::NewSliceId(ContextSPtr, Ino ino, uint64_t* id) {
  if (!id_cache_.GenID(*id)) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}] newsliceid fail, ino({}) error: {}.", name_, ino);
    return Status::Internal("gen id fail");
  }

  return Status::OK();
}

Status MDSV2FileSystem::WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                   uint64_t fh,
                                   const std::vector<Slice>& slices) {
  LOG(INFO) << fmt::format(
      "[meta.filesystem.{}.{}.{}] writeslice missing cache.", ino, fh, index);

  mdsv2::DeltaSliceEntry delta_slice_entry;
  delta_slice_entry.set_chunk_index(index);
  for (const auto& slice : slices) {
    *delta_slice_entry.add_slices() = Helper::ToSlice(slice);
  }

  auto status =
      mds_client_->WriteSlice(ctx, ino, {std::move(delta_slice_entry)});
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}.{}] writeslice fail, error({}).", ino, fh,
        index, status.ToString());
    return status;
  }

  if (FLAGS_client_meta_read_chunk_cache_enable) {
    ClearChunkCache(ino, fh, index);
  }

  return Status::OK();
}

Status MDSV2FileSystem::Write(ContextSPtr, Ino ino, uint64_t offset,
                              uint64_t size, uint64_t fh) {
  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  LOG(INFO) << fmt::format(
      "[meta.filesystem.{}.{}] write memo, offset({}) size({}).", ino, fh,
      offset, size);

  file_session->AddWriteMemo(offset, size);

  return Status::OK();
}

Status MDSV2FileSystem::MkDir(ContextSPtr ctx, Ino parent,
                              const std::string& name, uint32_t uid,
                              uint32_t gid, uint32_t mode, Attr* out_attr) {
  auto status =
      mds_client_->MkDir(ctx, parent, name, uid, gid, mode, 0, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::RmDir(ContextSPtr ctx, Ino parent,
                              const std::string& name) {
  auto status = mds_client_->RmDir(ctx, parent, name);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dir not empty");
    }
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  auto dir_iterator = DirIterator::New(ctx, mds_client_, ino);
  auto status = dir_iterator->Seek();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}] opendir fail, error({}).", ino, fh,
        status.ToString());
    return status;
  }

  dir_iterator_manager_.Put(fh, dir_iterator);

  return Status::OK();
}

Status MDSV2FileSystem::ReadDir(ContextSPtr, Ino, uint64_t fh, uint64_t offset,
                                bool with_attr, ReadDirHandler handler) {
  auto dir_iterator = dir_iterator_manager_.Get(fh);
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  while (dir_iterator->Valid()) {
    DirEntry entry = dir_iterator->GetValue(with_attr);

    if (!handler(entry, offset)) {
      break;
    }

    dir_iterator->Next();
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReleaseDir(ContextSPtr, Ino, uint64_t fh) {
  dir_iterator_manager_.Delete(fh);
  return Status::OK();
}

Status MDSV2FileSystem::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                             const std::string& new_name, Attr* attr) {
  auto status = mds_client_->Link(ctx, ino, new_parent, new_name, *attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}] link to {} fail, error({}).", new_parent,
        new_name, ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Unlink(ContextSPtr ctx, Ino parent,
                               const std::string& name) {
  auto status = mds_client_->UnLink(ctx, parent, name);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}.{}] unlink fail, error({}).",
                              parent, name, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::Symlink(ContextSPtr ctx, Ino parent,
                                const std::string& name, uint32_t uid,
                                uint32_t gid, const std::string& link,
                                Attr* out_attr) {
  auto status =
      mds_client_->Symlink(ctx, parent, name, uid, gid, link, *out_attr);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}.{}] symlink fail, symlink({}) error({}).", parent,
        name, link, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  auto status = mds_client_->ReadLink(ctx, ino, *link);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] readlink fail, error({}).",
                              ino, status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetAttr(ContextSPtr ctx, Ino ino, Attr* out_attr) {
  CHECK(ctx != nullptr) << "context is null";

  auto status = mds_client_->GetAttr(ctx, ino, *out_attr);
  if (!status.ok()) {
    return status;
  }

  auto file_session = file_session_map_.GetSession(ino);
  if (file_session != nullptr) {
    uint64_t write_memo_length = file_session->GetLength();
    if (write_memo_length != 0) {
      out_attr->length = std::max(out_attr->length, write_memo_length);

      uint64_t time_ns = file_session->GetLastTimeNs();
      out_attr->atime = time_ns;
      out_attr->ctime = time_ns;
      out_attr->mtime = time_ns;

      ctx->is_amend = true;
    }
  }

  LOG(INFO) << fmt::format(
      "[meta.filesystem.{}] get attr length({}) is_amend({}).", ino,
      out_attr->length, ctx->is_amend);

  return Status::OK();
}

Status MDSV2FileSystem::SetAttr(ContextSPtr ctx, Ino ino, int set,
                                const Attr& attr, Attr* out_attr) {
  auto status = mds_client_->SetAttr(ctx, ino, attr, set, *out_attr);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::GetXattr(ContextSPtr ctx, Ino ino,
                                 const std::string& name, std::string* value) {
  auto status = mds_client_->GetXAttr(ctx, ino, name, *value);
  if (!status.ok()) {
    return Status::NoData(status.Errno(), status.ToString());
  }

  return Status::OK();
}

Status MDSV2FileSystem::SetXattr(ContextSPtr ctx, Ino ino,
                                 const std::string& name,
                                 const std::string& value, int flags) {
  AttrEntry attr_entry;
  auto status = mds_client_->SetXAttr(ctx, ino, name, value, attr_entry);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::RemoveXattr(ContextSPtr ctx, Ino ino,
                                    const std::string& name) {
  AttrEntry attr_entry;
  auto status = mds_client_->RemoveXAttr(ctx, ino, name, attr_entry);
  if (!status.ok()) {
    return status;
  }

  return Status::OK();
}

Status MDSV2FileSystem::ListXattr(ContextSPtr ctx, Ino ino,
                                  std::vector<std::string>* xattrs) {
  CHECK(xattrs != nullptr) << "xattrs is null.";

  std::map<std::string, std::string> xattr_map;
  auto status = mds_client_->ListXAttr(ctx, ino, xattr_map);
  if (!status.ok()) {
    return status;
  }

  for (auto& [key, _] : xattr_map) {
    xattrs->push_back(key);
  }

  return Status::OK();
}

Status MDSV2FileSystem::Rename(ContextSPtr ctx, Ino old_parent,
                               const std::string& old_name, Ino new_parent,
                               const std::string& new_name) {
  auto status =
      mds_client_->Rename(ctx, old_parent, old_name, new_parent, new_name);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dist dir not empty");
    }

    return status;
  }

  return Status::OK();
}

// bool MDSV2FileSystem::GetAttrFromCache(Ino ino, Attr& out_attr) {
//   if (!FLAGS_client_meta_enable_inode_cache) return false;

//   auto inode = inode_cache_->GetInode(ino);
//   if (inode == nullptr) return false;

//   out_attr = Helper::ToAttr(inode->Copy());

//   return true;
// }

// bool MDSV2FileSystem::GetXAttrFromCache(Ino ino, const std::string& name,
//                                         std::string& value) {
//   if (!FLAGS_client_meta_enable_inode_cache) return false;

//   auto inode = inode_cache_->GetInode(ino);
//   if (inode == nullptr) return false;

//   value = inode->XAttr(name);

//   return true;
// }

// void MDSV2FileSystem::InsertInodeToCache(Ino ino, const AttrEntry&
// attr_entry) {
//   if (!FLAGS_client_meta_enable_inode_cache) return;

//   inode_cache_->UpsertInode(ino, attr_entry);
// }

// void MDSV2FileSystem::UpdateInodeToCache(Ino ino, const Attr& attr) {
//   if (!FLAGS_client_meta_enable_inode_cache) return;

//   auto inode = inode_cache_->GetInode(ino);
//   if (inode != nullptr) inode->UpdateIf(Helper::ToAttr(attr));
// }

// void MDSV2FileSystem::UpdateInodeToCache(Ino ino, const AttrEntry&
// attr_entry) {
//   if (!FLAGS_client_meta_enable_inode_cache) return;

//   auto inode = inode_cache_->GetInode(ino);
//   if (inode != nullptr) inode->UpdateIf(attr_entry);
// }

// void MDSV2FileSystem::DeleteInodeFromCache(Ino ino) {
//   inode_cache_->DeleteInode(ino);
// }

bool MDSV2FileSystem::GetSliceFromCache(Ino ino, uint64_t index,
                                        std::vector<Slice>* slices) {
  auto file_session = file_session_map_.GetSession(ino);
  if (file_session == nullptr) return false;

  auto chunk_mutation = file_session->GetChunkMutation(index);
  if (chunk_mutation == nullptr) return false;
  if (!chunk_mutation->HasChunk()) return false;

  *slices = chunk_mutation->GetAllSlice();

  return true;
}

// void MDSV2FileSystem::UpdateInodeLength(Ino ino, uint64_t new_length) {
//   auto inode = inode_cache_->GetInode(ino);
//   if (inode != nullptr) {
//     inode->ExpandLength(new_length);
//   }
// }

// bool MDSV2FileSystem::WriteSliceToCache(Ino ino, uint64_t index, uint64_t fh,
//                                         const std::vector<Slice>& slices) {
//   auto file_session = file_session_map_.GetSession({ino, fh});
//   CHECK(file_session != nullptr) << fmt::format(
//       "file session is nullptr, ino({}) index({}) fh({}).", ino, index, fh);

//   file_session->AppendSlice(index, slices);

//   UpdateInodeLength(ino, Helper::CalLength(slices));

//   return true;
// }

// void MDSV2FileSystem::DeleteDeltaSliceFromCache(
//     Ino ino, uint64_t fh,
//     const std::vector<mdsv2::DeltaSliceEntry>& delta_slice_entries) {
//   auto file_session = file_session_map_.GetSession({ino, fh});

//   for (const auto& delta_slice_entry : delta_slice_entries) {
//     auto chunk_muataion =
//         file_session->GetChunkMutation(delta_slice_entry.chunk_index());
//     if (chunk_muataion != nullptr) {
//       std::vector<uint64_t> delete_slice_ids;
//       for (const auto& slice : delta_slice_entry.slices()) {
//         delete_slice_ids.push_back(slice.id());
//       }

//       if (!delete_slice_ids.empty()) {
//         chunk_muataion->DeleteDeltaSlice(delete_slice_ids);
//       }
//     }
//   }
// }

// void MDSV2FileSystem::UpdateChunkToCache(
//     Ino ino, uint64_t fh, const std::vector<mdsv2::ChunkEntry>& chunks) {
//   auto file_session = file_session_map_.GetSession({ino, fh});
//   CHECK(file_session != nullptr)
//       << fmt::format("file session {}/{} is nullptr.", ino, fh);

//   for (const auto& chunk : chunks) {
//     file_session->UpsertChunkMutation(chunk);
//   }
// }

void MDSV2FileSystem::ClearChunkCache(Ino ino, uint64_t fh, uint64_t index) {
  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr) << fmt::format(
      "file session is nullptr, ino({}) index({}) fh({}).", ino, index, fh);

  file_session->DeleteChunkMutation(index);
}

// Status MDSV2FileSystem::SyncDeltaSlice(ContextSPtr ctx, Ino ino, uint64_t fh)
// {
//   auto file_session = file_session_map_.GetSession({ino, fh});
//   CHECK(file_session != nullptr)
//       << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

//   auto chunk_mutations = file_session->GetAllChunkMutation();
//   std::vector<mdsv2::DeltaSliceEntry> delta_slice_entries;
//   delta_slice_entries.reserve(chunk_mutations.size());
//   for (auto& chunk_mutation : chunk_mutations) {
//     auto delta_slice = chunk_mutation->GetDeltaSlice();
//     if (!delta_slice.empty()) {
//       delta_slice_entries.push_back(
//           Helper::ToDeltaSliceEntry(chunk_mutation->GetIndex(),
//           delta_slice));
//     }
//   }

//   if (delta_slice_entries.empty()) return Status::OK();

//   auto status = mds_client_->WriteSlice(ctx, ino, delta_slice_entries);
//   if (!status.ok()) {
//     LOG(ERROR) << fmt::format(
//         "[meta.filesystem.{}] sync delta slice fail, error({}).", ino,
//         status.ToString());
//     return status;
//   }

//   LOG(INFO) << fmt::format(
//       "[meta.filesystem.{}.{}] sync delta slice finish, "
//       "delta_slice_entries({}).",
//       ino, fh, delta_slice_entries.size());

//   // clean local cache delta slice
//   DeleteDeltaSliceFromCache(ino, fh, delta_slice_entries);

//   return Status::OK();
// }

MDSV2FileSystemUPtr MDSV2FileSystem::Build(const std::string& fs_name,
                                           const std::string& mds_addr,
                                           const std::string& mountpoint) {
  LOG(INFO) << fmt::format(
      "[meta.filesystem.{}] build filesystem mds_addr: {}, mountpoint: {}.",
      fs_name, mds_addr, mountpoint);

  CHECK(!fs_name.empty()) << "fs_name is empty.";
  CHECK(!mds_addr.empty()) << "mds_addr is empty.";
  CHECK(!mountpoint.empty()) << "mountpoint is empty.";

  std::string hostname = Helper::GetHostName();
  if (hostname.empty()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] get hostname fail.",
                              fs_name);
    return nullptr;
  }

  ClientId client_id(hostname, 0, mountpoint);
  LOG(INFO) << fmt::format("[meta.filesystem.{}] client_id: {}", fs_name,
                           client_id.ID());

  auto rpc = RPC::New(mds_addr);
  if (!rpc->Init()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] RPC init fail.", fs_name);
    return nullptr;
  }

  auto mds_discovery = MDSDiscovery::New(rpc);
  if (!mds_discovery->Init()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] MDSDiscovery init fail.",
                              fs_name);
    return nullptr;
  }

  mdsv2::FsInfoEntry pb_fs_info;
  auto status = MDSClient::GetFsInfo(rpc, fs_name, pb_fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] Get fs info fail.",
                              fs_name);
    return nullptr;
  }

  // parent cache
  auto parent_memo = ParentMemo::New();

  // mds router
  MDSRouterPtr mds_router;
  if (pb_fs_info.partition_policy().type() ==
      dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION) {
    mds_router = MonoMDSRouter::New(mds_discovery);

  } else if (pb_fs_info.partition_policy().type() ==
             dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
    mds_router = ParentHashMDSRouter::New(mds_discovery, parent_memo);

  } else {
    LOG(ERROR) << fmt::format(
        "[meta.filesystem.{}] not support partition policy type({}).", fs_name,
        dingofs::pb::mdsv2::PartitionType_Name(
            pb_fs_info.partition_policy().type()));
    return nullptr;
  }

  if (!mds_router->Init(pb_fs_info.partition_policy())) {
    LOG(ERROR) << fmt::format("[meta.filesystem.{}] MDSRouter init fail.",
                              fs_name);
    return nullptr;
  }

  auto fs_info = mdsv2::FsInfo::New(pb_fs_info);

  auto inode_cache = InodeCache::New(fs_info->GetFsId());

  // create mds client
  auto mds_client = MDSClient::New(client_id, fs_info, parent_memo,
                                   mds_discovery, mds_router, rpc);
  if (!mds_client->Init()) {
    LOG(INFO) << fmt::format("[meta.filesystem.{}] MDSClient init fail.",
                             fs_name);
    return nullptr;
  }

  // create filesystem
  return MDSV2FileSystem::New(fs_info, client_id, mds_discovery, inode_cache,
                              mds_client);
}

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs