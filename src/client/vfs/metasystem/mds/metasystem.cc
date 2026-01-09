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

#include "client/vfs/metasystem/mds/metasystem.h"

#include <fcntl.h>
#include <openssl/rsa.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/types.h"
#include "client/vfs/common/client_id.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/const.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "fmt/ranges.h"
#include "glog/logging.h"
#include "json/value.h"
#include "json/writer.h"
#include "mds/common/helper.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const uint32_t kHeartbeatIntervalS = 5;                     // seconds
const uint32_t kCleanExpiredModifyTimeMemoIntervalS = 300;  // seconds

const std::string kSliceIdCacheName = "slice";

DEFINE_bool(client_meta_read_chunk_cache_enable, true,
            "enable read chunk cache");

MDSMetaSystem::MDSMetaSystem(mds::FsInfoEntry fs_info_entry,
                             const ClientId& client_id, RPC&& rpc,
                             TraceManagerSPtr trace_manager)
    : name_(fs_info_entry.fs_name()),
      client_id_(client_id),
      fs_info_(fs_info_entry),
      mds_client_(client_id, fs_info_, std::move(rpc), trace_manager),
      inode_cache_(fs_info_.GetFsId()),
      id_cache_(kSliceIdCacheName, mds_client_),
      batch_processor_(mds_client_) {}

MDSMetaSystem::~MDSMetaSystem() {}  // NOLINT

Status MDSMetaSystem::Init(bool upgrade) {
  LOG(INFO) << fmt::format("[meta.fs] init, upgrade({}).", upgrade);

  LOG(INFO) << fmt::format("[meta.fs] fs_info: {}.", fs_info_.ToString());

  if (!mds_client_.Init()) {
    return Status::MountFailed("init mds_client fail");
  }

  // mount fs
  if (!upgrade && !MountFs()) {
    return Status::MountFailed("mount fs fail");
  }

  if (!batch_processor_.Init()) {
    return Status::Internal("init batch processor fail");
  }

  // init crontab
  if (!InitCrontab()) {
    return Status::Internal("init crontab fail");
  }

  return Status::OK();
}

void MDSMetaSystem::Stop(bool upgrade) {
  LOG(INFO) << fmt::format("[meta.fs] stopping, upgrade({}).", upgrade);

  stopped_.store(true);

  FlushAllSlice();

  if (!upgrade) UnmountFs();

  mds_client_.Stop();

  crontab_manager_.Destroy();

  batch_processor_.Stop();

  LOG(INFO) << fmt::format("[meta.fs] stopped, upgrade({}).", upgrade);
}

bool MDSMetaSystem::Dump(ContextSPtr, Json::Value& value) {
  if (!file_session_map_.Dump(value)) {
    return false;
  }

  if (!dir_iterator_manager_.Dump(value)) {
    return false;
  }

  if (!mds_client_.Dump(value)) {
    return false;
  }

  if (!modify_time_memo_.Dump(value)) {
    return false;
  }

  if (!chunk_memo_.Dump(value)) {
    return false;
  }

  return true;
}

bool MDSMetaSystem::Dump(const DumpOption& options, Json::Value& value) {
  LOG(INFO) << "[meta.fs] dump...";

  if (options.file_session) {
    if (options.ino != 0) {
      if (!file_session_map_.Dump(options.ino, value)) return false;

    } else {
      if (!file_session_map_.Dump(value)) return false;
    }
  }

  if (options.dir_iterator && !dir_iterator_manager_.Dump(value)) {
    return false;
  }

  if (!mds_client_.Dump(options, value)) {
    return false;
  }

  if (options.inode_cache && !inode_cache_.Dump(value)) {
    return false;
  }

  if (options.modify_time_memo && !modify_time_memo_.Dump(value)) {
    return false;
  }

  if (options.chunk_memo && !chunk_memo_.Dump(value)) {
    return false;
  }

  return true;
}

bool MDSMetaSystem::Load(ContextSPtr, const Json::Value& value) {
  LOG(INFO) << "[meta.fs] load...";

  if (!file_session_map_.Load(value)) {
    return false;
  }

  if (!dir_iterator_manager_.Load(mds_client_, value)) {
    return false;
  }

  if (!mds_client_.Load(value)) {
    return false;
  }

  if (!modify_time_memo_.Load(value)) {
    return false;
  }

  if (!chunk_memo_.Load(value)) {
    return false;
  }

  return true;
}

Status MDSMetaSystem::GetFsInfo(ContextSPtr, FsInfo* fs_info) {
  auto temp_fs_info = fs_info_.Get();

  fs_info->name = name_;
  fs_info->id = temp_fs_info.fs_id();
  fs_info->chunk_size = temp_fs_info.chunk_size();
  fs_info->block_size = temp_fs_info.block_size();
  fs_info->uuid = temp_fs_info.uuid();
  fs_info->status = Helper::ToFsStatus(temp_fs_info.status());

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
    LOG(ERROR) << fmt::format("[meta.fs] unknown fs type: {}.",
                              pb::mds::FsType_Name(temp_fs_info.fs_type()));
    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

bool MDSMetaSystem::MountFs() {
  pb::mds::MountPoint mount_point;
  mount_point.set_client_id(client_id_.ID());
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_port(client_id_.Port());
  mount_point.set_path(client_id_.Mountpoint());
  mount_point.set_cto(false);

  LOG(INFO) << fmt::format("[meta.fs] mount point({}).",
                           mount_point.ShortDebugString());

  auto status = mds_client_.MountFs(name_, mount_point);
  if (!status.ok() && status.Errno() != pb::error::EEXISTED) {
    LOG(ERROR) << fmt::format(
        "[meta.fs] mount fs info fail, mountpoint({}), {}.",
        client_id_.Mountpoint(), status.ToString());
    return false;
  }

  return true;
}

bool MDSMetaSystem::UnmountFs() {
  auto status = mds_client_.UmountFs(name_, client_id_.ID());
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs] mount fs info fail, mountpoint({}).",
                              client_id_.Mountpoint());
    return false;
  }

  return true;
}

void MDSMetaSystem::Heartbeat() {
  // prevent multiple heartbeats running at the same time
  static std::atomic<bool> is_running{false};
  if (is_running.exchange(true)) return;

  auto status = mds_client_.Heartbeat();
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[meta.fs] heartbeat fail, error({}).",
                              status.ToString());
  }

  is_running = false;
}

void MDSMetaSystem::CleanExpiredModifyTimeMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  modify_time_memo_.ForgetExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredChunkMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  chunk_memo_.ForgetExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredInodeCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_inode_cache_expired_s;

  inode_cache_.CleanExpired(expired_time_s);
}

bool MDSMetaSystem::InitCrontab() {
  // add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEAT",
      kHeartbeatIntervalS * 1000,
      true,
      [this](void*) { this->Heartbeat(); },
  });

  // add clean expired crontab
  crontab_configs_.push_back({
      "CLEAN_EXPIRED_MEMO",
      kCleanExpiredModifyTimeMemoIntervalS * 1000,
      true,
      [this](void*) {
        this->CleanExpiredModifyTimeMemo();
        this->CleanExpiredChunkMemo();
        this->CleanExpiredInodeCache();
      },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

Status MDSMetaSystem::StatFs(ContextSPtr ctx, Ino ino, FsStat* fs_stat) {
  Status status;
  if (ino <= kRootIno) {
    status = mds_client_.GetFsQuota(ctx, *fs_stat);
  } else {
    status = mds_client_.GetDirQuota(ctx, ino, *fs_stat);
  }

  if (fs_stat->max_bytes == 0) {
    fs_stat->max_bytes = INT64_MAX;
  }

  if (fs_stat->max_inodes == 0) {
    fs_stat->max_inodes = INT64_MAX;
  }

  fs_stat->used_bytes = std::max<int64_t>(fs_stat->used_bytes, 0);
  fs_stat->used_inodes = std::max<int64_t>(fs_stat->used_inodes, 0);

  return status;
};

Status MDSMetaSystem::Lookup(ContextSPtr ctx, Ino parent,
                             const std::string& name, Attr* attr) {
  AssertStop();

  AttrEntry attr_entry;
  auto status = mds_client_.Lookup(ctx, parent, name, attr_entry);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_FOUND) {
      return Status::NotExist("not found dentry");
    }
    return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::Create(ContextSPtr ctx, Ino parent,
                             const std::string& name, uint32_t uid,
                             uint32_t gid, uint32_t mode, int flags, Attr* attr,
                             uint64_t fh) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  std::vector<std::string> session_ids;
  auto status = mds_client_.Create(ctx, parent, name, uid, gid, mode, flags,
                                   attr_entry, parent_attr_entry, session_ids);
  if (!status.ok()) {
    return status;
  }

  // add file session
  CHECK(!session_ids.empty()) << "session_ids is empty.";
  const auto& session_id = session_ids.front();
  auto file_session = file_session_map_.Put(attr_entry.ino(), fh, session_id);

  *attr = Helper::ToAttr(attr_entry);

  DeleteInodeFromCache(parent);
  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::RunOperation(OperationSPtr operation) {
  CHECK(operation != nullptr) << "operation is null.";

  bthread::CountdownEvent count_down(1);

  operation->SetEvent(&count_down);

  if (!batch_processor_.RunBatched(operation)) {
    return Status::Internal("flush operation fail");
  }

  CHECK(count_down.wait() == 0) << "count down wait fail.";

  return operation->GetStatus();
}

Status MDSMetaSystem::MkNod(ContextSPtr ctx, Ino parent,
                            const std::string& name, uint32_t uid, uint32_t gid,
                            uint32_t mode, uint64_t rdev, Attr* attr) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  if (FLAGS_vfs_meta_batch_operation_enable) {
    auto operation = std::make_shared<MkNodOperation>(ctx, parent, name, uid,
                                                      gid, mode, rdev);
    auto status = RunOperation(operation);
    if (!status.ok()) return status;

    auto& result = operation->GetResult();
    attr_entry = result.attr_entry;
    parent_attr_entry = result.parent_attr_entry;

    LOG(INFO) << fmt::format("[meta.fs] mknod {}/{} attr({}) parent_attr({})",
                             parent, name, attr_entry.ShortDebugString(),
                             parent_attr_entry.ShortDebugString());

  } else {
    auto status = mds_client_.MkNod(ctx, parent, name, uid, gid, mode, rdev,
                                    attr_entry, parent_attr_entry);
    if (!status.ok()) return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  DeleteInodeFromCache(parent);
  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) {
  AssertStop();

  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  std::string session_id;
  AttrEntry attr_entry;
  std::vector<mds::ChunkEntry> chunks;
  bool is_prefetch_chunk = FLAGS_client_meta_read_chunk_cache_enable;

  // prepare chunk descriptors for expect chunk version
  std::vector<mds::ChunkDescriptor> chunk_descriptors;
  if (is_prefetch_chunk) {
    auto versions = chunk_memo_.GetVersion(ino);
    for (auto& [chunk_index, version] : versions) {
      mds::ChunkDescriptor chunk_descriptor;
      chunk_descriptor.set_index(chunk_index);
      chunk_descriptor.set_version(version);
      chunk_descriptors.push_back(chunk_descriptor);
    }
  }

  auto status = mds_client_.Open(ctx, ino, flags, session_id, is_prefetch_chunk,
                                 chunk_descriptors, attr_entry, chunks);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] open file fail, error({}).", ino,
                              fh, status.ToString());
    return status;
  }

  LOG(INFO) << fmt::format(
      "[meta.fs.{}.{}] open file flags({:o}:{}) session_id({}) "
      "is_prefetch_chunk({}) chunks({}).",
      ino, fh, flags, mds::Helper::DescOpenFlags(flags), session_id,
      is_prefetch_chunk, chunks.size());

  // add file session and chunk
  auto file_session = file_session_map_.Put(ino, fh, session_id);
  if (is_prefetch_chunk && !chunks.empty()) {
    file_session->GetChunkSet().Put(chunks);
  }

  if (flags & O_TRUNC) chunk_memo_.Forget(ino);

  // update chunk memo
  for (const auto& chunk : chunks) {
    uint64_t memo_version = chunk_memo_.GetVersion(ino, chunk.index());
    CHECK(chunk.version() >= memo_version) << fmt::format(
        "[meta.fs.{}.{}.{}] chunk version invalid, index({}) version({}<{}).",
        ino, fh, session_id, chunk.index(), chunk.version(), memo_version);

    chunk_memo_.Remember(ino, chunk.index(), chunk.version());
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  LOG(INFO) << fmt::format("[meta.fs.{}.{}] flush.", ino, fh);

  return FlushSlice(ctx, ino);
}

Status MDSMetaSystem::Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  struct Param {
    MDSClient& mds_client;
    ContextSPtr ctx;
    Ino ino;
    uint64_t fh;
    std::string session_id;

    Param(ContextSPtr ctx, MDSClient& mds_client, Ino ino, uint64_t fh,
          const std::string& session_id)
        : mds_client(mds_client),
          ctx(ctx),
          ino(ino),
          fh(fh),
          session_id(session_id) {}
  };

  std::string session_id = file_session_map_.GetSessionID(ino, fh);
  CHECK(!session_id.empty())
      << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

  // clean cache
  file_session_map_.Delete(ino, fh);

  LOG(INFO) << fmt::format("[meta.fs.{}.{}] close file session_id({}).", ino,
                           fh, session_id);

  // async close file
  Param* param = new Param(ctx, mds_client_, ino, fh, session_id);

  bthread_t tid;
  const bthread_attr_t attr = BTHREAD_ATTR_SMALL;
  bthread_start_background(
      &tid, &attr,
      [](void* arg) -> void* {
        Param* param = static_cast<Param*>(arg);
        auto& mds_client = param->mds_client;
        auto& ctx = param->ctx;
        auto& ino = param->ino;
        auto& fh = param->fh;
        auto& session_id = param->session_id;

        auto status = mds_client.Release(ctx, ino, session_id);
        if (!status.ok()) {
          LOG(ERROR) << fmt::format(
              "[meta.fs.{}.{}] close file fail, error({}).", ino, fh,
              status.ToString());
        }

        return nullptr;
      },
      param);

  return Status::OK();
}

Status MDSMetaSystem::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                uint64_t fh, std::vector<Slice>* slices,
                                uint64_t& version) {
  AssertStop();

  // take from cache
  auto file_session = file_session_map_.GetSession(ino);
  if (file_session != nullptr) {
    auto chunk = file_session->GetChunkSet().Get(index);
    if (chunk != nullptr && chunk->IsCompleted()) {
      *slices = chunk->GetAllSlice();
      ctx->hit_cache = true;

      LOG(INFO) << fmt::format(
          "[meta.fs.{}.{}.{}] readslice from cache, version({}) slices({}).",
          ino, fh, index, version, Helper::ToString(*slices));
      return Status::OK();
    }
  }
  // set chunk version
  mds::ChunkDescriptor chunk_descriptor;
  chunk_descriptor.set_index(static_cast<uint32_t>(index));
  chunk_descriptor.set_version(
      chunk_memo_.GetVersion(ino, static_cast<uint32_t>(index)));

  std::vector<mds::ChunkEntry> chunks;
  auto status = mds_client_.ReadSlice(ctx, ino, {chunk_descriptor}, chunks);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}.{}] reeadslice fail, error({}).",
                              ino, fh, index, status.ToString());
    return status;
  }

  // not found chunk, return empty slice
  if (chunks.empty()) {
    mds::ChunkEntry chunk_entry;
    chunk_entry.set_index(index);
    chunk_entry.set_chunk_size(fs_info_.GetChunkSize());
    chunk_entry.set_block_size(fs_info_.GetBlockSize());
    chunk_entry.set_version(0);
    chunks.push_back(chunk_entry);
  }

  // update cache
  if (file_session != nullptr) file_session->GetChunkSet().Put(chunks);

  const auto& chunk_entry = chunks.front();
  for (const auto& slice : chunk_entry.slices()) {
    slices->emplace_back(Helper::ToSlice(slice));
  }
  version = chunk_entry.version();

  chunk_memo_.Remember(ino, index, version);

  LOG(INFO) << fmt::format(
      "[meta.fs.{}.{}.{}] readslice from server, version({}) slices({}).", ino,
      fh, index, version, Helper::ToString(*slices));

  return Status::OK();
}

Status MDSMetaSystem::NewSliceId(ContextSPtr, Ino ino, uint64_t* id) {
  AssertStop();

  if (!id_cache_.GenID(*id)) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] newsliceid fail.", ino);
    return Status::Internal("gen id fail");
  }

  return Status::OK();
}

Status MDSMetaSystem::WriteSlice(ContextSPtr, Ino ino, uint64_t index,
                                 uint64_t fh, const std::vector<Slice>&) {
  LOG(FATAL) << fmt::format("[meta.fs.{}.{}.{}] writeslice missing cache.", ino,
                            fh, index);
  return Status::OK();
}

Status MDSMetaSystem::AsyncWriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                      uint64_t fh,
                                      const std::vector<Slice>& slices,
                                      DoneClosure done) {
  AssertStop();

  LOG(INFO) << fmt::format("[meta.fs.{}.{}.{}] async writeslice, slices({}).",
                           ino, fh, index, Helper::ToString(slices));

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  file_session->GetChunkSet().Append(index, slices);

  AsyncFlushSlice(ctx, file_session, false, false);

  done(Status::OK());

  return Status::OK();
}

Status MDSMetaSystem::Write(ContextSPtr, Ino ino, uint64_t offset,
                            uint64_t size, uint64_t fh) {
  AssertStop();

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  LOG(INFO) << fmt::format("[meta.fs.{}.{}] write, offset({}) size({}).", ino,
                           fh, offset, size);

  file_session->GetChunkSet().AddWriteMemo(offset, size);

  // update last modify time
  modify_time_memo_.Remember(ino);

  return Status::OK();
}

Status MDSMetaSystem::MkDir(ContextSPtr ctx, Ino parent,
                            const std::string& name, uint32_t uid, uint32_t gid,
                            uint32_t mode, Attr* attr) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;

  if (FLAGS_vfs_meta_batch_operation_enable) {
    auto operation =
        std::make_shared<MkDirOperation>(ctx, parent, name, uid, gid, mode);
    auto status = RunOperation(operation);
    if (!status.ok()) return status;

    auto& result = operation->GetResult();
    attr_entry = result.attr_entry;
    parent_attr_entry = result.parent_attr_entry;

    LOG(INFO) << fmt::format("[meta.fs] mkdir {}/{} attr({}) parent_attr({})",
                             parent, name, attr_entry.ShortDebugString(),
                             parent_attr_entry.ShortDebugString());

  } else {
    auto status = mds_client_.MkDir(ctx, parent, name, uid, gid, mode, 0,
                                    attr_entry, parent_attr_entry);
    if (!status.ok()) return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  DeleteInodeFromCache(parent);
  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::RmDir(ContextSPtr ctx, Ino parent,
                            const std::string& name) {
  AssertStop();

  AttrEntry parent_attr_entry;
  Ino ino;
  auto status = mds_client_.RmDir(ctx, parent, name, ino, parent_attr_entry);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dir not empty");
    }
    return status;
  }

  DeleteInodeFromCache(parent);
  DeleteInodeFromCache(ino);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::OpenDir(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  auto dir_iterator = DirIterator::New(mds_client_, ino, fh);

  bool need_cache = false;
  dir_iterator_manager_.PutWithFunc(
      ino, fh, dir_iterator,
      [&need_cache](const std::vector<DirIteratorSPtr>& vec) {
        if (vec.size() <= 1) {
          need_cache = true;
        }
      });

  ctx->need_cache = need_cache;

  return Status::OK();
}

Status MDSMetaSystem::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh,
                              uint64_t offset, bool with_attr,
                              ReadDirHandler handler) {
  AssertStop();

  auto dir_iterator = dir_iterator_manager_.Get(ino, fh);
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  dir_iterator->Remember(offset);

  while (true) {
    DirEntry entry;
    auto status = dir_iterator->GetValue(ctx, offset++, with_attr, entry);
    if (!status.ok()) {
      if (status.IsNoData()) break;
      return status;
    }

    CorrectAttr(ctx, dir_iterator->LastFetchTimeNs(), entry.attr, "readdir");

    PutInodeToCache(Helper::ToAttr(entry.attr));

    if (!handler(entry, offset)) {
      break;
    }
  }

  return Status::OK();
}

Status MDSMetaSystem::ReleaseDir(ContextSPtr, Ino ino, uint64_t fh) {
  AssertStop();

  dir_iterator_manager_.Delete(ino, fh);
  return Status::OK();
}

Status MDSMetaSystem::Link(ContextSPtr ctx, Ino ino, Ino new_parent,
                           const std::string& new_name, Attr* attr) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  auto status = mds_client_.Link(ctx, ino, new_parent, new_name, attr_entry,
                                 parent_attr_entry);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] link to {} fail, error({}).",
                              new_parent, new_name, ino, status.ToString());
    return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  DeleteInodeFromCache(new_parent);
  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::Unlink(ContextSPtr ctx, Ino parent,
                             const std::string& name) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;

  if (FLAGS_vfs_meta_batch_operation_enable) {
    auto operation = std::make_shared<UnlinkOperation>(ctx, parent, name);
    auto status = RunOperation(operation);
    if (!status.ok()) return status;

    auto& result = operation->GetResult();
    attr_entry = result.attr_entry;
    parent_attr_entry = result.parent_attr_entry;

    LOG(INFO) << fmt::format("[meta.fs] unlink {}/{} attr({}) parent_attr({})",
                             parent, name, attr_entry.ShortDebugString(),
                             parent_attr_entry.ShortDebugString());

  } else {
    auto status =
        mds_client_.UnLink(ctx, parent, name, attr_entry, parent_attr_entry);
    if (!status.ok()) return status;
  }

  DeleteInodeFromCache(parent);
  if (attr_entry.nlink() == 0) {
    DeleteInodeFromCache(attr_entry.ino());

  } else {
    PutInodeToCache(attr_entry);
    PutInodeToCache(parent_attr_entry);
  }

  return Status::OK();
}

Status MDSMetaSystem::Symlink(ContextSPtr ctx, Ino parent,
                              const std::string& name, uint32_t uid,
                              uint32_t gid, const std::string& link,
                              Attr* attr) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  auto status = mds_client_.Symlink(ctx, parent, name, uid, gid, link,
                                    attr_entry, parent_attr_entry);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}.{}] symlink fail, symlink({}) error({}).", parent, name,
        link, status.ToString());
    return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  DeleteInodeFromCache(parent);
  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::ReadLink(ContextSPtr ctx, Ino ino, std::string* link) {
  AssertStop();

  auto status = mds_client_.ReadLink(ctx, ino, *link);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] readlink fail, error({}).", ino,
                              status.ToString());
    return status;
  }

  return Status::OK();
}

Status MDSMetaSystem::GetAttr(ContextSPtr ctx, Ino ino, Attr* attr) {
  AssertStop();

  CHECK(ctx != nullptr) << "context is null";

  AttrEntry attr_entry;

  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    attr_entry = inode->ToAttrEntry();
    ctx->hit_cache = true;

  } else {
    auto status = mds_client_.GetAttr(ctx, ino, attr_entry);
    if (!status.ok()) return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  auto status = CorrectAttr(ctx, ctx->start_time_ns, *attr, "getattr");
  if (!status.ok()) return status;

  LOG(INFO) << fmt::format("[meta.fs.{}] get attr length({}) is_amend({}).",
                           ino, attr->length, ctx->is_amend);

  return Status::OK();
}

Status MDSMetaSystem::SetAttr(ContextSPtr ctx, Ino ino, int set,
                              const Attr& attr, Attr* out_attr) {
  AssertStop();

  AttrEntry attr_entry;
  bool shrink_file;
  auto status =
      mds_client_.SetAttr(ctx, ino, attr, set, attr_entry, shrink_file);
  if (!status.ok()) {
    return status;
  }

  *out_attr = Helper::ToAttr(attr_entry);

  status = CorrectAttr(ctx, ctx->start_time_ns, *out_attr, "setattr");
  if (!status.ok()) return status;

  modify_time_memo_.Remember(ino);
  if (shrink_file) chunk_memo_.Forget(ino);

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::GetXattr(ContextSPtr ctx, Ino ino,
                               const std::string& name, std::string* value) {
  AssertStop();

  // take out xattr cache
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    *value = inode->GetXAttr(name);
    ctx->hit_cache = true;
    return Status::OK();
  }

  // get xattr from mds
  auto status = mds_client_.GetXAttr(ctx, ino, name, *value);
  if (!status.ok()) {
    return Status::NoData(status.Errno(), status.ToString());
  }

  return Status::OK();
}

Status MDSMetaSystem::SetXattr(ContextSPtr ctx, Ino ino,
                               const std::string& name,
                               const std::string& value, int) {
  AssertStop();

  AttrEntry attr_entry;
  auto status = mds_client_.SetXAttr(ctx, ino, name, value, attr_entry);
  if (!status.ok()) {
    return status;
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::RemoveXattr(ContextSPtr ctx, Ino ino,
                                  const std::string& name) {
  AssertStop();

  AttrEntry attr_entry;
  auto status = mds_client_.RemoveXAttr(ctx, ino, name, attr_entry);
  if (!status.ok()) {
    return status;
  }

  PutInodeToCache(attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::ListXattr(ContextSPtr ctx, Ino ino,
                                std::vector<std::string>* xattrs) {
  AssertStop();
  CHECK(xattrs != nullptr) << "xattrs is null.";

  // take out xattr cache
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    for (auto& pair : inode->ListXAttrs()) {
      xattrs->push_back(pair.first);
    }
    ctx->hit_cache = true;
    return Status::OK();
  }

  // get xattr from mds
  std::map<std::string, std::string> xattr_map;
  auto status = mds_client_.ListXAttr(ctx, ino, xattr_map);
  if (!status.ok()) {
    return status;
  }

  for (auto& [key, _] : xattr_map) {
    xattrs->push_back(key);
  }

  return Status::OK();
}

Status MDSMetaSystem::Rename(ContextSPtr ctx, Ino old_parent,
                             const std::string& old_name, Ino new_parent,
                             const std::string& new_name) {
  AssertStop();

  std::vector<Ino> effected_inos;
  auto status = mds_client_.Rename(ctx, old_parent, old_name, new_parent,
                                   new_name, effected_inos);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dist dir not empty");
    }

    return status;
  }

  for (const auto& ino : effected_inos) {
    DeleteInodeFromCache(ino);
  }

  return Status::OK();
}

void MDSMetaSystem::PutInodeToCache(const AttrEntry& attr_entry) {
  if (FLAGS_vfs_meta_inode_cache_enable) {
    inode_cache_.Put(attr_entry.ino(), attr_entry);
  }
}

void MDSMetaSystem::DeleteInodeFromCache(Ino ino) {
  if (FLAGS_vfs_meta_inode_cache_enable) {
    inode_cache_.Delete(ino);
  }
}

InodeSPtr MDSMetaSystem::GetInodeFromCache(Ino ino) {
  return (FLAGS_vfs_meta_inode_cache_enable) ? inode_cache_.Get(ino) : nullptr;
}

Status MDSMetaSystem::SetInodeLength(ContextSPtr ctx,
                                     FileSessionSPtr file_session, Ino ino) {
  uint64_t length = file_session->GetChunkSet().GetLength();
  auto inode = inode_cache_.Get(ino);
  if (inode != nullptr && length <= inode->Length()) return Status::OK();

  LOG(INFO) << fmt::format("[meta.fs.{}] set inode length({}).", ino, length);

  uint64_t now_ns = utils::TimestampNs();
  Attr attr;
  attr.ino = ino;
  attr.length = length;
  attr.mtime = now_ns;
  attr.ctime = now_ns;
  int set = kSetAttrSize | kSetAttrMtime | kSetAttrCtime;

  AttrEntry attr_entry;
  bool shrink_file;
  auto status =
      mds_client_.SetAttr(ctx, ino, attr, set, attr_entry, shrink_file);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}] set inode length({}) fail, error({}).", ino, length,
        status.ToString());
    return status;
  }

  modify_time_memo_.Remember(ino);
  if (shrink_file) chunk_memo_.Forget(ino);

  PutInodeToCache(attr_entry);

  return Status::OK();
}

void MDSMetaSystem::LaunchWriteSlice(ContextSPtr& ctx,
                                     FileSessionSPtr file_session,
                                     CommitTaskSPtr task) {
  Ino ino = file_session->GetIno();

  LOG(INFO) << fmt::format("[meta.fs.{}] launch write slice {}.", ino,
                           task->Describe());

  auto operation = std::make_shared<WriteSliceOperation>(
      ctx, ino, task,
      [this, file_session, ino](
          const Status& status, CommitTaskSPtr task,
          const std::vector<mds::ChunkDescriptor>& chunk_descriptors) {
        task->SetDone(status);

        if (status.ok()) {
          LOG(INFO) << fmt::format(
              "[meta.fs.{}] flush delta slice done, task({}) status({}).", ino,
              task->TaskID(), status.ToString());

          auto& chunk_set = file_session->GetChunkSet();
          chunk_set.DeleteCommitTask(task->TaskID());
          chunk_set.MarkCommited(chunk_descriptors);

          chunk_memo_.Remember(ino, chunk_descriptors);

        } else {
          LOG(ERROR) << fmt::format(
              "[meta.fs.{}] flush delta slice fail, task({}) retry({}) "
              "status({}).",
              ino, task->TaskID(), task->Retries(), status.ToString());
        }
      });

  if (!batch_processor_.AsyncRun(operation)) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] flush delta slice fail.", ino);
    task->SetDone(Status::Internal("launch write slice fail"));
  }
}

void MDSMetaSystem::AsyncFlushSlice(ContextSPtr& ctx,
                                    FileSessionSPtr file_session, bool is_force,
                                    bool is_wait) {
  Ino ino = file_session->GetIno();
  auto& chunk_set = file_session->GetChunkSet();

  uint32_t new_task_count = chunk_set.TryCommitSlice(is_force);
  if (new_task_count == 0) return;

  uint32_t launched_count = 0;
  auto tasks = chunk_set.ListCommitTask();
  for (auto& task : tasks) {
    if (task->MaybeRun()) {
      ++launched_count;
      LaunchWriteSlice(ctx, file_session, task);
    }
  }

  LOG(INFO) << fmt::format(
      "[meta.fs.{}] async flush task new({}) launch({}) total({}).", ino,
      new_task_count, launched_count, tasks.size());

  if (is_wait) {
    for (auto& task : tasks) task->Wait();
  }
}

Status MDSMetaSystem::FlushSlice(ContextSPtr ctx, Ino ino) {
  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}).", ino);

  auto& chunk_set = file_session->GetChunkSet();

  LOG(INFO) << fmt::format("[meta.fs.{}] flush all slice.", ino);

  do {
    bool has_stage = chunk_set.HasStage();
    bool has_committing = chunk_set.HasCommitting();
    bool has_commit_task = chunk_set.HasCommitTask();
    if (!has_stage && !has_committing && !has_commit_task) break;

    LOG(INFO) << fmt::format(
        "[meta.fs.{}] flush all slice loop, has_stage({}) has_committing({}) "
        "has_commit_task({}).",
        ino, has_stage, has_committing, has_commit_task);

    AsyncFlushSlice(ctx, file_session, true, true);

  } while (true);

  // modify inode length
  return SetInodeLength(ctx, file_session, ino);
}

void MDSMetaSystem::FlushAllSlice() {
  auto file_sessions = file_session_map_.GetAllSession();

  LOG(INFO) << fmt::format("[meta.fs] flush all slice, count({}).",
                           file_sessions.size());

  for (auto& file_session : file_sessions) {
    Ino ino = file_session->GetIno();
    ContextSPtr ctx = std::make_shared<Context>("");

    auto status = FlushSlice(ctx, ino);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("[meta.fs.{}] flush all slice fail, error({}).",
                                ino, status.ToString());
    }
  }
}

Status MDSMetaSystem::CorrectAttr(ContextSPtr ctx, uint64_t time_ns, Attr& attr,
                                  const std::string& caller) {
  if (modify_time_memo_.ModifiedSince(attr.ino, time_ns)) {
    LOG(INFO) << fmt::format("[meta.fs.{}] correct attr, caller({}).", attr.ino,
                             caller);
    // correct attr, fetch latest attr from mds
    AttrEntry attr_entry;

    auto status = mds_client_.GetAttr(ctx, attr.ino, attr_entry);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.fs.{}] get attr fail for correct, caller({}) error({}).",
          attr.ino, caller, status.ToString());
      return status;
    }
    attr = Helper::ToAttr(attr_entry);
    ctx->is_amend = true;
  }

  // correct length with write memo
  CorrectAttrLength(ctx, attr, caller);

  return Status::OK();
}

void MDSMetaSystem::CorrectAttrLength(ContextSPtr ctx, Attr& attr,
                                      const std::string& caller) {
  auto file_session = file_session_map_.GetSession(attr.ino);
  if (file_session != nullptr) {
    auto& chunk_set = file_session->GetChunkSet();
    uint64_t write_memo_length = chunk_set.GetLength();
    if (write_memo_length > attr.length) {
      LOG(INFO) << fmt::format("[meta.fs.{}] correct length, caller({}).",
                               attr.ino, caller);

      attr.length = write_memo_length;

      uint64_t time_ns = chunk_set.GetLastWriteTimeNs();
      attr.ctime = std::max(attr.ctime, time_ns);
      attr.mtime = std::max(attr.mtime, time_ns);

      ctx->is_amend = true;
    }
  }
}

static std::vector<std::string> SplitMdsAddrs(const std::string& mds_addrs) {
  std::vector<std::string> addrs;

  if (mds_addrs.find(',') != std::string::npos) {
    mds::Helper::SplitString(mds_addrs, ',', addrs);
    return addrs;

  } else if (mds_addrs.find(';') != std::string::npos) {
    mds::Helper::SplitString(mds_addrs, ';', addrs);
    return addrs;
  }

  addrs.push_back(mds_addrs);
  return addrs;
}

static std::string GetAliveMdsAddr(const std::string& mds_addrs) {
  auto mds_addr_vec = SplitMdsAddrs(mds_addrs);
  for (const auto& mds_addr : mds_addr_vec) {
    if (RPC::CheckMdsAlive(mds_addr)) {
      return mds_addr;
    }
  }

  return "";
}

MDSMetaSystemUPtr MDSMetaSystem::Build(const std::string& fs_name,
                                       const std::string& mds_addrs,
                                       const ClientId& client_id,
                                       TraceManager& trace_manager) {
  LOG(INFO) << fmt::format("[meta.fs.{}] build filesystem mds_addrs({}).",
                           fs_name, mds_addrs);

  CHECK(!fs_name.empty()) << "fs_name is empty.";
  CHECK(!mds_addrs.empty()) << "mds_addrs is empty.";

  // check mds addr
  std::string alive_mds_addr = GetAliveMdsAddr(mds_addrs);
  if (alive_mds_addr.empty()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}] mds addr check fail, mds_addrs({}).", fs_name, mds_addrs);
    return nullptr;
  }

  RPC rpc(alive_mds_addr);
  if (!rpc.Init()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] RPC init fail.", fs_name);
    return nullptr;
  }

  mds::FsInfoEntry fs_info;
  auto status = MDSClient::GetFsInfo(rpc, fs_name, fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] Get fs info fail.", fs_name);
    return nullptr;
  }

  // create filesystem
  return MDSMetaSystem::New(fs_info, client_id, std::move(rpc), trace_manager);
}

bool MDSMetaSystem::GetDescription(Json::Value& value) {
  // client
  Json::Value client_id;
  client_id["id"] = client_id_.ID();
  client_id["host_name"] = client_id_.Hostname();
  client_id["port"] = client_id_.Port();
  client_id["mount_point"] = client_id_.Mountpoint();
  client_id["mds_addr"] = mds_client_.GetRpc().GetInitEndPoint();
  value["client_id"] = client_id;

  // fs info
  Json::Value fs_info;
  auto fs_info_entry = fs_info_.Get();
  fs_info["id"] = fs_info_entry.fs_id();
  fs_info["name"] = fs_info_entry.fs_name();
  fs_info["owner"] = fs_info_entry.owner();
  fs_info["block_size"] = fs_info_entry.block_size();
  fs_info["chunk_size"] = fs_info_entry.chunk_size();
  fs_info["capacity"] = fs_info_entry.capacity();
  fs_info["create_time_s"] = fs_info_entry.create_time_s();
  fs_info["last_update_time_ns"] = fs_info_entry.last_update_time_ns();
  fs_info["recycle_time"] = fs_info_entry.recycle_time_hour();
  fs_info["s3_endpoint"] = fs_info_entry.extra().s3_info().endpoint();
  fs_info["s3_bucket"] = fs_info_entry.extra().s3_info().bucketname();
  fs_info["rados_mon_host"] = fs_info_entry.extra().rados_info().mon_host();
  fs_info["rados_pool_name"] = fs_info_entry.extra().rados_info().pool_name();
  fs_info["rados_user_name"] = fs_info_entry.extra().rados_info().user_name();
  fs_info["rados_cluster_name"] =
      fs_info_entry.extra().rados_info().cluster_name();

  value["fs_info"] = fs_info;

  // mds info
  DumpOption options;
  options.mds_discovery = true;
  return mds_client_.Dump(options, value);
}

}  // namespace meta
}  // namespace vfs
}  // namespace client
}  // namespace dingofs