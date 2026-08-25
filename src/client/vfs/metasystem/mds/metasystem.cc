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

#include "chunk.h"
#include "client/vfs/common/client_id.h"
#include "client/vfs/components/context.h"
#include "client/vfs/components/warmup_manager.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/logging.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "compact.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "file_session.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "inode_cache.h"
#include "json/value.h"
#include "json/writer.h"
#include "mds/common/helper.h"
#include "mds/common/trash.h"
#include "utils/uuid.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace meta {

const uint32_t kMaxXAttrNameLength = 255;
const uint32_t kMaxXAttrValueLength = 64 * 1024;

const uint32_t kHeartbeatIntervalS = 5;                     // seconds
const uint32_t kCleanExpiredModifyTimeMemoIntervalS = 300;  // seconds

const std::string kSliceIdCacheName = "slice";

const std::string kExecutorWorkerSetName = "meta_executor";

DEFINE_uint32(vfs_meta_executor_worker_num, 64, "number of meta workers");
DEFINE_uint32(vfs_meta_executor_max_pending_num, 1048576,
              "meta worker max pending num");

const std::string kBgExecutorWorkerSetName = "meta_bg_executor";

DEFINE_uint32(vfs_meta_bg_executor_worker_num, 16, "number of compact workers");
DEFINE_uint32(vfs_meta_bg_executor_max_pending_num, 4096,
              "compact worker max pending num");
DEFINE_bool(vfs_meta_bg_executor_use_pthread, true,
            "compact worker use pthread");

DEFINE_uint32(vfs_meta_copy_file_range_max_chunks_per_rpc, 256,
              "Max dst chunks affected by one CopyFileRange RPC window.");

MDSMetaSystemUPtr MDSMetaSystem::Build(const std::string& fs_name,
                                       const std::string& mds_addrs,
                                       const ClientId& client_id,
                                       TraceManager& trace_manager,
                                       Compactor& compactor) {
  LOG(INFO) << fmt::format("[meta.fs.{}] build filesystem mds_addrs({}).",
                           fs_name, mds_addrs);

  CHECK(!fs_name.empty()) << "fs_name is empty.";
  CHECK(!mds_addrs.empty()) << "mds_addrs is empty.";

  RPC rpc(mds_addrs, "mds");
  auto status = rpc.Init();
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] RPC init fail, status({}).",
                              fs_name, status.ToString());
    return nullptr;
  }

  mds::FsInfoEntry fs_info;
  status = MDSClient::GetFsInfo(rpc, fs_name, fs_info);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}] get fs info fail, status({}).",
                              fs_name, status.ToString());
    return nullptr;
  }

  // create filesystem
  return MDSMetaSystem::New(fs_info, client_id, std::move(rpc), trace_manager,
                            compactor);
}

MDSMetaSystem::MDSMetaSystem(mds::FsInfoEntry fs_info_entry,
                             const ClientId& client_id, RPC&& rpc,
                             TraceManager& trace_manager, Compactor& compactor)
    : name_(fs_info_entry.fs_name()),
      client_id_(client_id),
      fs_info_(fs_info_entry),
      executor_(kExecutorWorkerSetName, FLAGS_vfs_meta_executor_worker_num,
                FLAGS_vfs_meta_executor_max_pending_num),
      bg_executor_(kBgExecutorWorkerSetName,
                   FLAGS_vfs_meta_bg_executor_worker_num,
                   FLAGS_vfs_meta_bg_executor_max_pending_num,
                   FLAGS_vfs_meta_bg_executor_use_pthread),
      mds_client_(client_id, fs_info_, std::move(rpc), trace_manager),
      inode_cache_(fs_info_.GetFsId()),
      id_cache_(kSliceIdCacheName, mds_client_),
      file_session_map_(inode_cache_, fs_info_.GetChunkSize()),
      dir_profile_cache_(DirProfileCache::New()),
      batch_processor_(mds_client_),
      compactor_(compactor),
      compact_processor_(mds_client_, compactor_, bg_executor_),
      block_cache_cleaner_(fs_info_.GetFsId(), fs_info_.GetChunkSize(),
                           bg_executor_, mds_client_) {}

MDSMetaSystem::~MDSMetaSystem() {}  // NOLINT

Status MDSMetaSystem::Init(bool skip_mount) {
  LOG(INFO) << fmt::format("[meta.fs] init, skip_mount({}).", skip_mount);

  LOG(INFO) << fmt::format("[meta.fs] fs_info: {}.", fs_info_.ToString());

  if (!mds_client_.Init()) {
    return Status::MountFailed("init mds_client fail");
  }

  if (!executor_.Init()) {
    return Status::Internal("init executor fail");
  }

  if (!bg_executor_.Init()) {
    return Status::Internal("init bg_executor fail");
  }

  // mount fs — skipped when this is a new process inheriting an existing
  // session
  if (!skip_mount && !MountFs()) {
    return Status::MountFailed("mount fs fail");
  }

  if (!batch_processor_.Init()) {
    return Status::Internal("init batch processor fail");
  }

  // init crontab
  if (!InitCrontab()) {
    return Status::Internal("init crontab fail");
  }

  LOG(INFO) << fmt::format("[meta.fs] inited, skip_mount({}).", skip_mount);

  return Status::OK();
}

void MDSMetaSystem::Stop(bool skip_unmount) {
  LOG(INFO) << fmt::format("[meta.fs] stopping metasystem, skip_unmount({}).",
                           skip_unmount);

  stopped_.store(true);

  FlushAllFile();

  crontab_manager_.Stop();

  batch_processor_.Stop();

  compact_processor_.Stop();

  block_cache_cleaner_.Stop();

  executor_.Stop();

  bg_executor_.Stop();

  // Skipped when this is the old process handing off its session to the new
  // process.
  if (!skip_unmount) UnmountFs();

  mds_client_.Stop();

  LOG(INFO) << fmt::format("[meta.fs] stopped metasystem, skip_unmount({}).",
                           skip_unmount);
}

bool MDSMetaSystem::GetSummary(Json::Value& value) {
  CHECK(value.isArray()) << "value is not array.";

  Json::Value modify_time_memo_value = Json::objectValue;
  modify_time_memo_.Summary(modify_time_memo_value);
  value.append(modify_time_memo_value);

  Json::Value chunk_memo_value = Json::objectValue;
  chunk_memo_.Summary(chunk_memo_value);
  value.append(chunk_memo_value);

  Json::Value file_session_value = Json::objectValue;
  file_session_map_.Summary(file_session_value);
  value.append(file_session_value);

  Json::Value dir_iterator_value = Json::objectValue;
  dir_iterator_manager_.Summary(dir_iterator_value);
  value.append(dir_iterator_value);

  Json::Value inode_cache_value = Json::objectValue;
  inode_cache_.Summary(inode_cache_value);
  value.append(inode_cache_value);

  mds_client_.Summary(value);

  return true;
}

// dump state for upgrade
bool MDSMetaSystem::Dump(ContextSPtr, Json::Value& value) {
  LOG(INFO) << "[meta.fs] dumping...";

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

  if (!inode_cache_.Dump(value)) {
    return false;
  }

  LOG(INFO) << "[meta.fs] dumped...";

  return true;
}

// dump state for show
bool MDSMetaSystem::Dump(const DumpOption& options, Json::Value& value) {
  LOG(INFO) << "[meta.fs] dumping...";

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

  if (options.chunk_set) {
    LOG(INFO) << fmt::format("[meta.fs] dump chunk set, ino({}).", options.ino);
    FileSessionSPtr file_session = file_session_map_.GetSession(options.ino);
    if (file_session != nullptr) {
      auto chunk_set = file_session->GetChunkSet();
      if (!chunk_set->Dump(value)) return false;
    }
  }

  if (options.chunk) {
    LOG(INFO) << fmt::format("[meta.fs] dump chunk, ino({}) index({}).",
                             options.ino, options.chunk_index);
    FileSessionSPtr file_session = file_session_map_.GetSession(options.ino);
    if (file_session == nullptr) return true;

    auto chunk_set = file_session->GetChunkSet();
    auto chunk = chunk_set->Get(options.chunk_index);
    if (chunk == nullptr) return true;

    if (!chunk->Dump(value)) return false;
  }

  LOG(INFO) << "[meta.fs] dumped...";

  return true;
}

bool MDSMetaSystem::Load(ContextSPtr, const Json::Value& value) {
  LOG(INFO) << "[meta.fs] loading...";

  if (!inode_cache_.Load(value)) {
    return false;
  }

  if (!file_session_map_.Load(value)) {
    return false;
  }

  if (!dir_iterator_manager_.Load(mds_client_, inode_cache_, value)) {
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

  LOG(INFO) << "[meta.fs] loaded...";

  return true;
}

Status MDSMetaSystem::ToVfsFsInfo(const mds::FsInfoEntry& src,
                                  FsInfo* dst) const {
  dst->name = name_;
  dst->id = src.fs_id();
  dst->chunk_size = src.chunk_size();
  dst->block_size = src.block_size();
  dst->uuid = src.uuid();
  dst->status = Helper::ToFsStatus(src.status());
  dst->create_time_s = src.create_time_s();
  dst->trash_days = src.trash_days();
  dst->enable_uid_gid_map = src.enable_uid_gid_map();

  dst->storage_info.store_type = Helper::ToStoreType(src.fs_type());
  if (dst->storage_info.store_type == StoreType::kS3) {
    CHECK(src.extra().has_s3_info()) << "fs type is S3, but s3 info is not set";

    dst->storage_info.s3_info = Helper::ToS3Info(src.extra().s3_info());

  } else if (dst->storage_info.store_type == StoreType::kRados) {
    CHECK(src.extra().has_rados_info())
        << "fs type is Rados, but rados info is not set";

    dst->storage_info.rados_info =
        Helper::ToRadosInfo(src.extra().rados_info());

  } else if (dst->storage_info.store_type == StoreType::kLocalFile) {
    CHECK(src.extra().has_file_info())
        << "fs type is LocalFile, but file info is not set";

    dst->storage_info.file_info =
        Helper::ToLocalFileInfo(src.extra().file_info());

  } else {
    LOG(ERROR) << fmt::format("[meta.fs] unknown fs type: {}.",
                              pb::mds::FsType_Name(src.fs_type()));
    return Status::InvalidParam("unknown fs type");
  }

  return Status::OK();
}

Status MDSMetaSystem::GetFsInfo(ContextSPtr, FsInfo* fs_info) {
  return ToVfsFsInfo(fs_info_.Get(), fs_info);
}

bool MDSMetaSystem::MountFs() {
  pb::mds::MountPoint mount_point;
  mount_point.set_client_id(client_id_.ID());
  mount_point.set_hostname(client_id_.Hostname());
  mount_point.set_ip(client_id_.IP());
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
    LOG(ERROR) << fmt::format("[meta.fs] umount fs info fail, mountpoint({}).",
                              client_id_.Mountpoint());
    return false;
  }

  return true;
}

void MDSMetaSystem::Heartbeat() {
  // prevent multiple heartbeats running at the same time
  static std::atomic<bool> is_running{false};
  if (is_running.exchange(true)) return;

  uint64_t last_fs_version = 0;
  auto status = mds_client_.Heartbeat(
      file_session_map_.GetNeedKeepAliveSession(), last_fs_version);
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[meta.fs] heartbeat fail, error({}).",
                              status.ToString());
    is_running = false;
    return;
  }

  // Piggyback: if MDS reports a newer fs version, trigger a one-shot
  // GetFsInfo to pull the full record and refresh the cached fs_info.
  // last_fs_version == 0 means MDS did not echo a ClientReply (e.g.
  // pre-feature server or no fs match); fall back to no-op rather than
  // thrashing.
  if (last_fs_version > 0 && last_fs_version > fs_info_.GetVersion()) {
    RefreshCachedFsInfo();
  }

  is_running = false;
}

void MDSMetaSystem::RefreshCachedFsInfo() {
  // Only ever called from Heartbeat(), which is already serialized by its
  // own re-entrancy guard, so no guard is needed here.
  mds::FsInfoEntry new_fs_info;
  auto status = mds_client_.RefreshFsInfo(new_fs_info);
  if (!status.ok()) {
    LOG(WARNING) << fmt::format("[meta.fs] refresh fs info fail, error({}).",
                                status.ToString());
    return;
  }

  // Update() is a no-op when the incoming version is not newer, so it is
  // safe to call on every tick.
  fs_info_.Update(new_fs_info);
}

void MDSMetaSystem::CleanExpiredModifyTimeMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  modify_time_memo_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredChunkMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  chunk_memo_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredInodeCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_inode_cache_expired_s;

  inode_cache_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredDirProfileCache() {
  if (!FLAGS_vfs_meta_warmup_small_file_enable) return;
  dir_profile_cache_->CleanExpired(utils::Timestamp());
}

void MDSMetaSystem::CleanExpiredCompactMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  compact_processor_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredReadChunkCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_read_chunk_cache_expired_s;

  read_chunk_cache_.CleanExpired(expired_time_s);
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
      "CLEAN_EXPIRED",
      kCleanExpiredModifyTimeMemoIntervalS * 1000,
      true,
      [this](void*) {
        this->CleanExpiredModifyTimeMemo();
        this->CleanExpiredChunkMemo();
        this->CleanExpiredInodeCache();
        this->CleanExpiredDirProfileCache();
        this->CleanExpiredCompactMemo();
        this->CleanExpiredReadChunkCache();
      },
  });

  // Note: the cached fs_info refreshes via the 5s heartbeat path — MDS echoes
  // the current FsInfo.version, and MDSMetaSystem::Heartbeat triggers
  // RefreshCachedFsInfo on diff. No separate poll crontab is needed.

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

  auto inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  bool is_amend = false;
  status = CorrectAttr(ctx, ctx->start_time_ns, *attr, is_amend, "lookup");
  if (!status.ok()) return status;

  if (attr->nlink == 0) return Status::NotExist("inode is deleted");

  if (!ctx->inner_req) {
    modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);
  }

  return Status::OK();
}

Status MDSMetaSystem::Create(ContextSPtr ctx, Ino parent,
                             const std::string& name, uint32_t uid,
                             uint32_t gid, uint32_t mode, int flags, Attr* attr,
                             uint64_t fh) {
  AssertStop();

  std::string session_id = utils::GenerateUUID();
  AttrEntry attr_entry, parent_attr_entry;
  auto status = mds_client_.Create(ctx, parent, name, uid, gid, mode, flags,
                                   session_id, attr_entry, parent_attr_entry);
  if (!status.ok()) return status;

  PutInodeToCache(parent_attr_entry);
  InodeSPtr inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  dir_iterator_manager_.InsertEntry(parent, name, attr->ino);

  // add file session
  auto file_session =
      file_session_map_.Put(attr_entry.ino(), fh, session_id, flags);
  file_session->SetInode(inode);
  file_session->GetChunkSet()->InitFlushCheckpoint(inode->Length());

  modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);

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
  auto status = mds_client_.MkNod(ctx, parent, name, uid, gid, mode, rdev,
                                  attr_entry, parent_attr_entry);
  if (!status.ok()) return status;

  PutInodeToCache(parent_attr_entry);
  auto inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  dir_iterator_manager_.InsertEntry(parent, name, attr->ino);

  modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);

  return Status::OK();
}

Status MDSMetaSystem::DoOpen(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                             const std::string& session_id,
                             FileSessionSPtr file_session) {
  CHECK(file_session != nullptr) << "file_session is null.";

  // check whether prefetch chunk
  // prepare chunk descriptors for expect chunk version
  std::vector<mds::ChunkDescriptor> chunk_descriptors;
  auto versions = chunk_memo_.GetVersion(ino);
  for (auto& [chunk_index, version] : versions) {
    mds::ChunkDescriptor chunk_descriptor;
    chunk_descriptor.set_index(chunk_index);
    chunk_descriptor.set_version(version);
    chunk_descriptors.push_back(chunk_descriptor);
  }

  AttrEntry attr_entry;
  std::vector<mds::ChunkEntry> chunks;
  auto status = mds_client_.Open(ctx, ino, flags, session_id, true,
                                 chunk_descriptors, attr_entry, chunks);

  LOG_DEBUG << fmt::format(
      "[meta.fs.{}.{}] open file flags({:o}:{}) session_id({}) "
      "chunks({}) status({}).",
      ino, fh, flags, dingofs::Helper::DescOpenFlags(flags), session_id,
      chunks.size(), status.ToString());

  if (!status.ok()) return status;

  // update inode cache
  InodeSPtr inode = PutInodeToCache(attr_entry);
  file_session->SetInode(inode);

  if (mds::IsDeleted(attr_entry)) {
    LOG(WARNING) << fmt::format(
        "[meta.fs.{}.{}] open file skipped, file is deleted.", ino, fh);
    return Status::NotExist("file is deleted");
  }

  // truncate file, forget chunk memo and delete chunk cache
  // update chunk cache
  auto& chunk_set = file_session->GetChunkSet();
  if (flags & O_TRUNC) {
    ResetFileChunkSet(ino, "open");
  } else {
    chunk_set->InitFlushCheckpoint(inode->Length());
  }
  if (!chunks.empty()) chunk_set->Put(chunks, "open");

  // update chunk memo
  for (const auto& chunk : chunks) {
    chunk_memo_.Remember(ino, chunk.index(), chunk.version());
  }

  return Status::OK();
}

void MDSMetaSystem::AsyncOpen(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                              const std::string& session_id,
                              FileSessionSPtr file_session) {
  class OpenTask;
  using OpenTaskPtr = std::shared_ptr<OpenTask>;

  class OpenTask : public TaskRunnable {
   public:
    OpenTask(MDSMetaSystem& metasystem, ContextSPtr ctx, Ino ino, int flags,
             uint64_t fh, const std::string& session_id,
             FileSessionSPtr file_session)
        : metasystem_(metasystem),
          ctx_(ctx),
          ino_(ino),
          flags_(flags),
          fh_(fh),
          session_id_(session_id),
          file_session_(file_session) {}
    ~OpenTask() override = default;

    static OpenTaskPtr New(MDSMetaSystem& metasystem, ContextSPtr ctx, Ino ino,
                           int flags, uint64_t fh,
                           const std::string& session_id,
                           FileSessionSPtr file_session) {
      return std::make_shared<OpenTask>(metasystem, ctx, ino, flags, fh,
                                        session_id, file_session);
    }

    std::string Type() override { return "OPEN"; }
    std::string Key() override { return fmt::format("{}:{}", ino_, fh_); }

    void Run() override {
      // check whether file is deleted
      auto inode = metasystem_.GetInodeFromCache(ino_);
      if (inode != nullptr && inode->IsDeleted()) {
        LOG(WARNING) << fmt::format(
            "[meta.fs.{}.{}] async open skipped, file is deleted.", ino_, fh_);
        return;
      }

      auto status = metasystem_.DoOpen(ctx_, ino_, flags_, fh_, session_id_,
                                       file_session_);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format(
            "[meta.fs.{}.{}] async open file fail, error({}).", ino_, fh_,
            status.ToString());
      }
    }

   private:
    MDSMetaSystem& metasystem_;
    ContextSPtr ctx_;

    const Ino ino_;
    const int flags_;
    const uint64_t fh_;
    const std::string session_id_;
    FileSessionSPtr file_session_;
  };

  executor_.ExecuteByHash(
      ino, OpenTask::New(*this, ctx, ino, flags, fh, session_id, file_session),
      true);
}

Status MDSMetaSystem::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                           bool* keep_cache) {
  AssertStop();

  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  // for warmup small file
  if ((flags & O_ACCMODE) == O_RDONLY) {
    auto dir_profile = GetDirProfile(ino);
    if (dir_profile != nullptr) {
      WarmupSmallFiles(dir_profile->CheckAndGenWarmupInos(ino));
    }
  }

  const std::string session_id = utils::GenerateUUID();
  auto file_session = file_session_map_.Put(ino, fh, session_id, flags);
  if (file_session->HasMultipleWriters()) {
    LOG(WARNING) << fmt::format(
        "[meta.fs.{}.{}] open file has multiple writers.", ino, fh);
  }

  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    if (inode->IsDeleted()) {
      LOG(WARNING) << fmt::format(
          "[meta.fs.{}.{}] open file skipped, file is deleted.", ino, fh);
      return Status::NotExist("file is deleted");
    }

    file_session->SetInode(inode);

    if (!(flags & O_TRUNC)) {
      file_session->GetChunkSet()->InitFlushCheckpoint(inode->Length());
      // launch async open
      AsyncOpen(ctx, ino, flags, fh, session_id, file_session);

      if (inode->Mtime() > modify_time_memo_.GetKernelMtime(ino)) {
        *keep_cache = false;
      }

      return Status::OK();
    }
  }

  auto status = DoOpen(ctx, ino, flags, fh, session_id, file_session);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] open file fail, error({}).", ino,
                              fh, status.ToString());
    if ((flags & O_TRUNC) && status.IsNetError()) *keep_cache = false;
    file_session_map_.Delete(ino, fh);

  } else {
    if (flags & O_TRUNC) *keep_cache = false;

    auto inode = GetInodeFromCache(ino);
    if (inode == nullptr ||
        inode->Mtime() > modify_time_memo_.GetKernelMtime(ino)) {
      *keep_cache = false;
    }
  }

  return status;
}

DirProfileSPtr MDSMetaSystem::GetDirProfile(Ino ino) {
  auto inode = GetInodeFromCache(ino);
  if (inode == nullptr) return nullptr;
  auto parents = inode->Parents();
  if (parents.empty()) return nullptr;
  Ino parent = parents.front();

  LOG_DEBUG << fmt::format("[meta.fs.{}] get dir profile, parent({}).", ino,
                           parent);

  return dir_profile_cache_->Get(parent);
}

void MDSMetaSystem::WarmupSmallFiles(const std::vector<Ino>& inoes) {
  if (inoes.empty()) return;
  if (warmup_manager_ == nullptr) return;

  LOG_DEBUG << fmt::format("[meta.fs] submit warmup task, ino({}).", inoes);

  // warmup small file data
  warmup_manager_->SubmitTask(WarmupTaskContext(inoes));

  // warmup small file chunk
  WarmupSmallFileChunk(inoes);
}

class WarmupChunkTask final : public mds::TaskRunnable {
 public:
  WarmupChunkTask(uint32_t fs_id, const std::vector<Ino>& inoes,
                  ChunkMemo& chunk_memo, MDSClient& mds_client,
                  ReadChunkCache& read_chunk_cache)
      : fs_id_(fs_id),
        inoes_(inoes),
        chunk_memo_(chunk_memo),
        mds_client_(mds_client),
        read_chunk_cache_(read_chunk_cache) {}

  std::string Type() override { return "WARMUP_CHUNK"; }
  std::string Key() override { return std::to_string(inoes_.front()); }

  void Run() override {
    Status status = DoWarmup();
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("[meta.fs.{}] warmup chunk fail, error({}).",
                                fs_id_, status.ToString());
    }
  }

 private:
  Status DoWarmup() {
    std::vector<MDSClient::ReadSliceInEntry> in_entries;
    in_entries.reserve(inoes_.size());
    for (const auto& ino : inoes_) {
      MDSClient::ReadSliceInEntry in_entry;
      in_entry.ino = ino;
      in_entry.index = 0;
      in_entry.version = chunk_memo_.GetVersion(ino, in_entry.index);
      in_entries.push_back(in_entry);
    }

    auto ctx = std::make_shared<Context>("");
    std::vector<MDSClient::ReadSliceOutEntry> out_entries;
    Status status = mds_client_.ReadSlice(ctx, in_entries, out_entries);
    if (!status.ok() && !status.IsNotFound()) return status;

    for (const auto& entry : out_entries) {
      read_chunk_cache_.Put(entry.ino, entry.chunk);
    }

    return Status::OK();
  }

  const uint32_t fs_id_;
  const std::vector<Ino> inoes_;

  ChunkMemo& chunk_memo_;
  MDSClient& mds_client_;
  ReadChunkCache& read_chunk_cache_;
};

void MDSMetaSystem::WarmupSmallFileChunk(const std::vector<Ino>& inoes) {
  if (inoes.empty()) return;

  auto task = std::make_shared<WarmupChunkTask>(
      fs_info_.GetFsId(), inoes, chunk_memo_, mds_client_, read_chunk_cache_);

  if (!bg_executor_.ExecuteByHash(inoes.front(), task)) {
    LOG(ERROR) << fmt::format(
        "[meta.fs] submit warmup chunk task fail, ino({}).", inoes.front());
  }
}

Status MDSMetaSystem::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  LOG_DEBUG << fmt::format("[meta.fs.{}.{}] flush.", ino, fh);

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  uint32_t flags = file_session->GetFlags(fh);

  return FlushSliceAndFile(ctx, ino);
}

Status MDSMetaSystem::RollbackFile(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  auto file_session = file_session_map_.GetSession(ino);
  if (file_session == nullptr) {
    LOG(WARNING) << fmt::format(
        "[meta.fs.{}.{}] rollback skipped, no file session.", ino, fh);
    return Status::OK();
  }

  auto& chunk_set = file_session->GetChunkSet();
  const uint64_t checkpoint = chunk_set->GetFlushCheckpoint();
  const uint64_t last_write_length = chunk_set->GetLastWriteLength();

  // nothing pushed beyond the checkpoint in this session -- nothing to undo.
  if (last_write_length == 0 || last_write_length <= checkpoint) {
    LOG_DEBUG << fmt::format(
        "[meta.fs.{}.{}] rollback skipped, write({}) checkpoint({}).", ino, fh,
        last_write_length, checkpoint);
    return Status::OK();
  }

  LOG(WARNING) << fmt::format(
      "[meta.fs.{}.{}] rollback write length, write({}) -> checkpoint({}).",
      ino, fh, last_write_length, checkpoint);

  AttrEntry attr_entry;
  bool shrink_file = false;
  auto status = mds_client_.RollbackFile(ctx, ino, last_write_length,
                                         checkpoint, attr_entry, shrink_file);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}.{}] rollback write length fail, checkpoint({}) "
        "error({}).",
        ino, fh, checkpoint, status.ToString());
    return status;
  }

  PutInodeToCache(attr_entry);

  if (shrink_file) ResetFileChunkSet(ino, "rollback");

  return Status::OK();
}

void MDSMetaSystem::AsyncClose(ContextSPtr ctx, Ino ino, uint64_t fh,
                               const std::string& session_id) {
  class CloseTask;
  using CloseTaskPtr = std::shared_ptr<CloseTask>;

  class CloseTask : public TaskRunnable {
   public:
    CloseTask(MDSMetaSystem& metasystem, MDSClient& mds_client, ContextSPtr ctx,
              Ino ino, uint64_t fh, const std::string& session_id)
        : metasystem_(metasystem),
          mds_client_(mds_client),
          ctx_(ctx),
          ino_(ino),
          fh_(fh),
          session_id_(session_id) {}
    ~CloseTask() override = default;

    static CloseTaskPtr New(MDSMetaSystem& metasystem, MDSClient& mds_client,
                            ContextSPtr ctx, Ino ino, uint64_t fh,
                            const std::string& session_id) {
      return std::make_shared<CloseTask>(metasystem, mds_client, ctx, ino, fh,
                                         session_id);
    }

    std::string Type() override { return "CLOSE"; }
    std::string Key() override {
      return fmt::format("{}:{}:{}", ino_, fh_, session_id_);
    }

    void Run() override {
      // check whether file is deleted
      auto inode = metasystem_.GetInodeFromCache(ino_);
      if (inode != nullptr && inode->IsDeleted()) {
        LOG(WARNING) << fmt::format(
            "[meta.fs.{}.{}] async close skipped, file is deleted.", ino_, fh_);
        return;
      }

      auto status = mds_client_.Release(ctx_, ino_, session_id_);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format("[meta.fs.{}.{}] close file fail, error({}).",
                                  ino_, fh_, status.ToString());
      }
    }

   private:
    MDSMetaSystem& metasystem_;
    MDSClient& mds_client_;

    ContextSPtr ctx_;
    const Ino ino_;
    const uint64_t fh_;
    const std::string session_id_;
  };

  executor_.ExecuteByHash(
      ino, CloseTask::New(*this, mds_client_, ctx, ino, fh, session_id), true);
}

Status MDSMetaSystem::Close(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  std::string session_id = file_session_map_.GetSessionID(ino, fh);
  if (session_id.empty()) {
    LOG(WARNING) << fmt::format(
        "[meta.fs.{}.{}] close file session not found, maybe already closed.",
        ino, fh);
    return Status::OK();
  }

  file_session_map_.Delete(ino, fh);

  LOG_DEBUG << fmt::format("[meta.fs.{}.{}] close file session_id({}).", ino,
                           fh, session_id);

  AsyncClose(ctx, ino, fh, session_id);

  return Status::OK();
}

Status MDSMetaSystem::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                uint64_t fh, std::vector<Slice>* slices,
                                uint64_t& version) {
  AssertStop();

  auto file_session = file_session_map_.GetSession(ino);
  // directly fetch from mds when file session is not found, just for prefetch
  // data
  if (file_session == nullptr) {
    // set chunk version
    mds::ChunkDescriptor chunk_descriptor;
    chunk_descriptor.set_index(static_cast<uint32_t>(index));
    chunk_descriptor.set_version(
        chunk_memo_.GetVersion(ino, static_cast<uint32_t>(index)));

    mds::ChunkEntry chunk_entry;
    auto status =
        mds_client_.ReadSlice(ctx, ino, chunk_descriptor, chunk_entry);
    if (!status.ok() && !status.IsNotFound()) {
      LOG(ERROR) << fmt::format(
          "[meta.fs.{}.{}.{}] reeadslice fail, error({}).", ino, fh, index,
          status.ToString());
      return status;
    }

    for (const auto& slice : chunk_entry.slices()) {
      slices->push_back(Helper::ToSlice(slice));
    }
    version = chunk_entry.version();

    LOG_DEBUG << fmt::format(
        "[meta.fs.{}.{}.{}] readslice from remote, version({}) slices({}).",
        ino, fh, index, version, Helper::ToString(*slices));

    return Status::OK();
  }

  auto chunk_set = file_session->GetChunkSet();

  bool is_fetch = false;
  do {
    auto chunk = chunk_set->Get(index);
    if (chunk != nullptr && chunk->IsCompleted()) {
      *slices = chunk->GetAllSlice(version);

      LOG_DEBUG << fmt::format(
          "[meta.fs.{}.{}.{}] readslice from {}, version({}) slices({}).", ino,
          fh, index, is_fetch ? "remote" : "cache", version,
          Helper::ToString(*slices));
      return Status::OK();
    }

    // check read cache for readonly
    if (!file_session->HasWriter() &&
        GetChunkFromReadCache(ino, index, slices, version)) {
      LOG_DEBUG << fmt::format(
          "[meta.fs.{}.{}.{}] readslice from read-cache, version({}) "
          "slices({}).",
          ino, fh, index, version, Helper::ToString(*slices));
      return Status::OK();
    }

    // set chunk version
    mds::ChunkDescriptor chunk_descriptor;
    chunk_descriptor.set_index(static_cast<uint32_t>(index));
    chunk_descriptor.set_version(
        chunk_memo_.GetVersion(ino, static_cast<uint32_t>(index)));

    mds::ChunkEntry chunk_entry;
    auto status =
        mds_client_.ReadSlice(ctx, ino, chunk_descriptor, chunk_entry);
    if (!status.ok() && !status.IsNotFound()) {
      LOG(ERROR) << fmt::format(
          "[meta.fs.{}.{}.{}] reeadslice fail, error({}).", ino, fh, index,
          status.ToString());
      return status;
    }

    // not found chunk, return empty slice
    if (status.IsNotFound()) {
      chunk_entry.set_index(index);
      chunk_entry.set_chunk_size(fs_info_.GetChunkSize());
      chunk_entry.set_block_size(fs_info_.GetBlockSize());
      chunk_entry.set_version(0);
    }

    // update cache
    chunk_set->Put(chunk_entry, "readslice");
    // update chunk memo

    chunk_memo_.Remember(ino, chunk_entry.index(), chunk_entry.version());

    is_fetch = true;

    LOG_DEBUG << fmt::format("[meta.fs.{}.{}.{}] fetch slice, version({}).",
                             ino, fh, index, chunk_entry.version());

  } while (true);

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

Status MDSMetaSystem::WriteSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                 uint64_t fh,
                                 const std::vector<Slice>& slices) {
  AssertStop();

  for (const auto& slice : slices) {
    LOG_DEBUG << fmt::format("[meta.fs.{}.{}.{}] writeslice, {}.", ino, fh,
                             index, slice.ToString());
  }

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  auto& chunk_set = file_session->GetChunkSet();
  chunk_set->Append(index, slices);

  AsyncFlushSlice(ctx, chunk_set, false, false);

  return Status::OK();
}

Status MDSMetaSystem::Write(ContextSPtr, Ino ino, const char* /*buf*/,
                            uint64_t offset, uint64_t size, uint64_t fh) {
  AssertStop();

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  file_session->GetChunkSet()->SetLastWriteLength(offset, size);

  // update last modify time
  modify_time_memo_.Remember(ino);

  return Status::OK();
}

Status MDSMetaSystem::MkDir(ContextSPtr ctx, Ino parent,
                            const std::string& name, uint32_t uid, uint32_t gid,
                            uint32_t mode, Attr* attr) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  auto status = mds_client_.MkDir(ctx, parent, name, uid, gid, mode, 0,
                                  attr_entry, parent_attr_entry);
  if (!status.ok()) return status;

  PutInodeToCache(parent_attr_entry);
  auto inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  dir_iterator_manager_.InsertEntry(parent, name, attr->ino);

  modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);

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

  DeleteInodeFromCache(ino);
  PutInodeToCache(parent_attr_entry);

  dir_iterator_manager_.DeleteEntry(parent, name);

  return Status::OK();
}

Status MDSMetaSystem::OpenDir(ContextSPtr, Ino ino, uint64_t fh,
                              bool& need_cache) {
  AssertStop();

  auto dir_iterator = DirIterator::New(mds_client_, inode_cache_, ino, fh);

  need_cache = false;
  dir_iterator_manager_.PutWithFunc(
      ino, fh, dir_iterator,
      [&need_cache](const std::vector<DirIteratorSPtr>& vec) {
        if (vec.size() <= 1) {
          need_cache = true;
        }
      });

  return Status::OK();
}

Status MDSMetaSystem::ReadDir(ContextSPtr ctx, Ino ino, uint64_t fh,
                              uint64_t offset, bool with_attr,
                              ReadDirHandler handler, uint32_t& count) {
  AssertStop();

  auto dir_iterator = dir_iterator_manager_.Get(ino, fh);
  CHECK(dir_iterator != nullptr) << "dir_iterator is null";

  std::lock_guard<std::mutex> lock(dir_iterator->Mutex());

  dir_iterator->Remember(offset);

  while (true) {
    DirEntry entry;
    auto status = dir_iterator->GetValue(ctx, offset++, with_attr, entry);
    if (!status.ok()) {
      // IsNoData is the EOF signal — the directory has been fully streamed,
      // so this is the moment to lock in the profile decision.
      if (status.IsNoData()) break;

      if (status.IsNotExist()) DeleteInodeFromCache(ino);

      return status;
    }

    if (with_attr) {
      InodeSPtr inode;
      status = GetInode(entry.ino, "readdir", inode);
      if (!status.ok()) return status;

      entry.attr = inode->ToAttr();

      bool is_amend = false;
      CorrectAttr(ctx, dir_iterator->LastFetchTimeNs(), entry.attr, is_amend,
                  "readdir");
    }

    if (!handler(entry, offset)) {
      // Caller stopped pagination early — leave the profile un-finalized.
      // Either the next ReadDir page reaches EOF, or the entry expires by
      // TTL/LRU. We never want a partial profile to drive warmup.
      break;
    }

    dir_iterator->MarkEmitted(offset);
    modify_time_memo_.UpdateKernelMtime(entry.attr.ino, entry.attr.mtime);
    ++count;
  }

  return Status::OK();
}

Status MDSMetaSystem::ReleaseDir(ContextSPtr, Ino ino, uint64_t fh) {
  AssertStop();

  auto dir_iterator = dir_iterator_manager_.Get(ino, fh);
  if (dir_iterator != nullptr) {
    auto dir_profile = dir_iterator->GetDirProfile();
    if (dir_profile != nullptr) {
      dir_profile->Finalize();
      if (dir_profile->IsSmallFileDir()) dir_profile_cache_->Put(dir_profile);
    }
  }

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

  PutInodeToCache(parent_attr_entry);
  auto inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  dir_iterator_manager_.InsertEntry(new_parent, new_name, attr->ino);

  modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);

  return Status::OK();
}

Status MDSMetaSystem::Unlink(ContextSPtr ctx, Ino parent,
                             const std::string& name) {
  AssertStop();

  AttrEntry attr_entry, parent_attr_entry;
  auto status =
      mds_client_.UnLink(ctx, parent, name, attr_entry, parent_attr_entry);
  if (!status.ok()) {
    if (status.IsNotExist()) return Status::OK();
    return status;
  }

  PutInodeToCache(parent_attr_entry);

  if (mds::IsDeleted(attr_entry)) {
    DeleteInodeFromCache(attr_entry.ino());
  } else {
    PutInodeToCache(attr_entry);
  }

  dir_iterator_manager_.DeleteEntry(parent, name);

  if (attr_entry.type() == pb::mds::FileType::FILE &&
      mds::IsDeleted(attr_entry)) {
    block_cache_cleaner_.Execute(attr_entry.ino(), attr_entry.length());
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

  PutInodeToCache(parent_attr_entry);
  auto inode = PutInodeToCache(attr_entry);
  *attr = inode->ToAttr();

  dir_iterator_manager_.InsertEntry(parent, name, attr->ino);

  modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);

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

  std::string reason = "missing";
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr && !inode->IsAttrFresh()) {
    // attr ttl expired, treat as cache miss to refetch from mds for
    // multi-client consistency
    inode = nullptr;
    reason = "expired";
  }

  if (inode == nullptr) {
    // Cold path: pull a fresh AttrEntry (carries hashed uid/gid) and
    // route it through PutInodeToCache so the inode cache is
    // populated/refreshed before inode->ToAttr() emits the hashed-id attr.
    // The VFS layer above (not ToAttr) performs the local-host uid/gid
    // translation.
    ctx->reason = reason;
    Status status = FetchInode(ctx, ino, inode);
    if (!status.ok()) return status;
  }

  *attr = inode->ToAttr();

  bool is_amend = false;
  auto status =
      CorrectAttr(ctx, ctx->start_time_ns, *attr, is_amend, "getattr");
  if (!status.ok()) return status;

  LOG_DEBUG << fmt::format("[meta.fs.{}] get attr length({}) is_amend({}).",
                           ino, attr->length, is_amend);

  if (attr->nlink == 0 && !file_session_map_.HasSession(ino)) {
    return Status::NotExist("inode is deleted");
  }

  if (!ctx->inner_req) {
    modify_time_memo_.UpdateKernelMtime(attr->ino, attr->mtime);
  }

  return Status::OK();
}

bool MDSMetaSystem::IsInodeInTrash(ContextSPtr ctx, Ino ino) {
  AssertStop();

  // An inode is "in trash" only when ALL of its parents are trash buckets,
  // mirroring the MDS-side gate (mds/filesystem.cc IsInodeInTrash). This is
  // the deferred-delete-on-last-hardlink model: an inode with a surviving
  // hardlink in a normal directory keeps that real parent alongside the
  // sub_trash entry and stays mutable. Note the ALL-of-empty trap -- an
  // inode with no known parents is NOT in trash.
  auto all_parents_trashed = [](const auto& parents) {
    if (parents.empty()) return false;
    for (auto p : parents) {
      if (!mds::IsTrashInode(p)) return false;
    }
    return true;
  };

  // Hot path: read parents directly from the inode cache (no RPC, no proto
  // round-trip). Cache is populated by Lookup/Open/Unlink and stays in sync
  // with mv-to-trash via PutInodeToCache, so a hit reflects the latest known
  // parent set for this client.
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    return all_parents_trashed(inode->Parents());
  }

  // Cold cache: fall back to GetAttr (which itself prefers cache, then RPC).
  // Open's fast path also requires a cache hit, so reaching here means the
  // caller bypassed prior Lookup; we accept the extra RPC to preserve the
  // early-reject guarantee.
  Attr attr;
  if (!GetAttr(ctx, ino, &attr).ok()) return false;
  return all_parents_trashed(attr.parents);
}

Status MDSMetaSystem::SetAttr(ContextSPtr ctx, Ino ino, int set,
                              const Attr& attr, Attr* out_attr) {
  AssertStop();

  if (set & kSetAttrSize) {
    // flush src file
    Status status = FlushSliceAndFile(ctx, ino);
    if (!status.ok()) return status;
  }

  MDSClient::AttrWithChunkOut out;
  auto status = mds_client_.SetAttr(ctx, ino, attr, set, out);
  if (!status.ok()) return status;

  auto inode = PutInodeToCache(out.attr_entry);
  *out_attr = inode->ToAttr();

  // update file length, need update local chunk cache write length
  if (set & kSetAttrSize) {
    ResetFileChunkSet(ino, "setattr");

    auto file_session = file_session_map_.GetSession(ino);
    if (file_session != nullptr && !out.effected_chunks.empty()) {
      file_session->GetChunkSet()->Put(out.effected_chunks, "setattr");
    }
  }

  bool is_amend = false;
  status = CorrectAttr(ctx, ctx->start_time_ns, *out_attr, is_amend, "setattr");
  if (!status.ok()) return status;

  modify_time_memo_.Remember(ino);
  modify_time_memo_.UpdateKernelMtime(ino, out_attr->mtime);

  return Status::OK();
}

Status MDSMetaSystem::Fallocate(ContextSPtr ctx, Ino ino, int mode,
                                uint64_t offset, uint64_t length) {
  AssertStop();

  // Like truncate, PUNCH_HOLE / ZERO_RANGE / preallocate append zero slices on
  // the MDS; drain buffered writes first so a late data slice can't land after
  // the zero slices and survive the hole.
  Status status = FlushSliceAndFile(ctx, ino);
  if (!status.ok()) return status;

  MDSClient::AttrWithChunkOut out;
  status = mds_client_.Fallocate(ctx, ino, mode, offset, length, out);
  auto file_session = file_session_map_.GetSession(ino);
  const bool collapse = mode == FALLOC_FL_COLLAPSE_RANGE;
  if (!status.ok()) {
    if (status.IsNetError()) {
      chunk_memo_.Forget(ino);
      if (collapse && file_session != nullptr)
        file_session->GetChunkSet()->Reset();
    }
    return status;
  }

  chunk_memo_.Forget(ino);
  modify_time_memo_.Remember(ino);
  PutInodeToCache(out.attr_entry);

  if (file_session != nullptr) {
    if (collapse) {
      // Every cached index at and after offset may now refer to a different
      // chunk. Reset rather than trying to delete and renumber individual
      // entries.
      file_session->GetChunkSet()->Reset();
    } else if (!out.effected_chunks.empty()) {
      file_session->GetChunkSet()->Put(out.effected_chunks, "fallocate");
    }
  }

  return status;
}

Status MDSMetaSystem::GetXattr(ContextSPtr ctx, Ino ino,
                               const std::string& name, std::string* value) {
  AssertStop();

  // take out xattr cache
  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    *value = inode->GetXAttr(name);

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
    // ctx->hit_cache = true;
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

Status MDSMetaSystem::FetchInode(ContextSPtr& ctx, Ino ino, InodeSPtr& inode) {
  AttrEntry attr_entry;
  auto status = mds_client_.GetAttr(ctx, ino, attr_entry);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}] fetch inode fail, reason({}) error({}).", ino,
        ctx->reason, status.ToString());
    return status;
  }

  LOG_DEBUG << fmt::format("[meta.fs.{}] fetch inode success, reason({}).", ino,
                           ctx->reason);

  InodeSPtr old_inode = GetInodeFromCache(ino);
  uint64_t old_mtime = old_inode != nullptr ? old_inode->Mtime() : 0;

  inode = PutInodeToCache(attr_entry);

  // check whether mtime changed
  // if changed, update file session's chunk is not completed
  if (old_mtime != 0 && old_mtime != inode->Mtime()) {
    InvalidateFileSessionReadCache(ino);
  }

  return Status::OK();
}

InodeSPtr MDSMetaSystem::PutInodeToCache(const AttrEntry& attr_entry) {
  return inode_cache_.Put(attr_entry.ino(), attr_entry);
}

InodeSPtr MDSMetaSystem::GetInode(FileSessionSPtr& file_session,
                                  const std::string& reason) {
  InodeSPtr inode = file_session->GetInode();
  if (inode != nullptr) return inode;

  inode = inode_cache_.Get(file_session->GetIno());
  if (inode != nullptr) return inode;

  ContextSPtr ctx = std::make_shared<Context>("");
  ctx->reason = reason;
  FetchInode(ctx, file_session->GetIno(), inode);

  return inode;
}

Status MDSMetaSystem::GetInode(Ino ino, const std::string& reason,
                               InodeSPtr& inode) {
  inode = inode_cache_.Get(ino);
  if (inode != nullptr) return Status::OK();

  ContextSPtr ctx = std::make_shared<Context>("");
  ctx->reason = reason;
  return FetchInode(ctx, ino, inode);
}

Status MDSMetaSystem::Rename(ContextSPtr ctx, Ino old_parent,
                             const std::string& old_name, Ino new_parent,
                             const std::string& new_name) {
  AssertStop();

  MDSClient::RenameResult result;
  auto status = mds_client_.Rename(ctx, old_parent, old_name, new_parent,
                                   new_name, result);
  if (!status.ok()) {
    if (status.Errno() == pb::error::ENOT_EMPTY) {
      return Status::NotEmpty("dist dir not empty");
    }

    return status;
  }

  PutInodeToCache(result.old_parent_attr);
  if (new_parent != old_parent) PutInodeToCache(result.new_parent_attr);
  PutInodeToCache(result.child_attr);
  PutInodeToCache(result.deleted_attr);

  // in-flight readdir snapshots still hold the old binding, drop it before
  // readdirplus feeds a stale dentry back to the kernel
  dir_iterator_manager_.DeleteEntry(old_parent, old_name);
  dir_iterator_manager_.InsertEntry(new_parent, new_name,
                                    result.child_attr.ino());

  return Status::OK();
}

Status MDSMetaSystem::DoFlushFile(ContextSPtr ctx, InodeSPtr inode,
                                  ChunkSetSPtr& chunk_set) {
  CHECK(chunk_set != nullptr) << "chunk_set is null.";

  if (inode == nullptr) {
    LOG(ERROR) << fmt::format(
        "[meta.fs] flush file fail cause inode is null, ino({}).",
        chunk_set->GetIno());
    return Status::Internal("inode is null");
  }

  const Ino ino = inode->Ino();
  const uint64_t file_length = inode->Length();
  const uint64_t last_write_length = chunk_set->GetLastWriteLength();
  const uint64_t last_write_slice_length = chunk_set->GetLastWriteSliceLength();

  if (last_write_length == 0 || last_write_length <= file_length) {
    LOG_DEBUG << fmt::format(
        "[meta.fs.{}] skip flush file, writeslice({}) write({}) "
        "filelength({}).",
        ino, last_write_slice_length, last_write_length, file_length);
    return Status::OK();
  }

  LOG_DEBUG << fmt::format(
      "[meta.fs.{}] flush file, writeslice({}) write({}) filelength({}).", ino,
      last_write_slice_length, last_write_length, file_length);

  AttrEntry attr_entry;
  auto status = mds_client_.FlushFile(ctx, ino, last_write_length, attr_entry);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}] flush file fail, length({}) error({}).", ino,
        last_write_length, status.ToString());
    return status;
  }

  // advance flush checkpoint (ADR-0003): data and length are both durable now.
  chunk_set->SetFlushCheckpoint(last_write_length);

  modify_time_memo_.Remember(ino);
  PutInodeToCache(attr_entry);

  return Status::OK();
}

void MDSMetaSystem::LaunchWriteSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set,
                                     CommitTaskSPtr task) {
  Ino ino = chunk_set->GetIno();

  LOG_DEBUG << fmt::format("[meta.fs.{}] launch write slice {}.", ino,
                           task->Describe());

  struct Params {
    Ino ino;
    ContextSPtr ctx;
    ChunkSetSPtr chunk_set;
    CommitTaskSPtr task;
    MDSMetaSystem* meta_system;
  };

  Params* params = new Params({.ino = ino,
                               .ctx = ctx,
                               .chunk_set = chunk_set,
                               .task = task,
                               .meta_system = this});

  bthread_t tid;
  bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
  if (bthread_start_background(
          &tid, &attr,
          [](void* arg) -> void* {
            Params* params = reinterpret_cast<Params*>(arg);
            const Ino ino = params->ino;
            auto& ctx = params->ctx;
            auto& chunk_set = params->chunk_set;
            auto& task = params->task;
            auto* meta_system = params->meta_system;
            auto& mds_client = meta_system->mds_client_;

            // generate delta slice entries
            std::vector<mds::DeltaSliceEntry> delta_slice_entries;
            for (const auto& delta_slice : task->DeltaSlices()) {
              mds::DeltaSliceEntry delta_slice_entry;
              delta_slice_entry.set_chunk_index(delta_slice.chunk_index);
              delta_slice_entry.set_curr_version(delta_slice.version);

              for (const auto& slice : delta_slice.slices) {
                *delta_slice_entry.add_slices() = Helper::ToSlice(slice);
              }

              delta_slice_entries.push_back(std::move(delta_slice_entry));
            }

            MDSClient::WriteSliceResult result;
            Status status =
                mds_client.WriteSlice(ctx, ino, delta_slice_entries, result);
            if (status.ok()) {
              LOG_DEBUG << fmt::format(
                  "[meta.fs.{}] writeslice done, task({}) status({}).", ino,
                  task->TaskID(), status.ToString());

              meta_system->PutInodeToCache(result.attr);
              auto status = chunk_set->FinishCommitTask(task, result.chunks);
              if (status.ok()) {
                meta_system->chunk_memo_.Remember(ino, result.chunks);
              }

            } else {
              LOG(ERROR) << fmt::format(
                  "[meta.fs.{}] writeslice fail, task({}) retry({}) "
                  "status({}).",
                  ino, task->TaskID(), task->Retries(), status.ToString());
            }

            // Publish completion only after all task-owned state is installed.
            // A waiter that observes OK must not find the task still pending.
            task->SetDone(status);

            delete params;

            return nullptr;
          },
          params) != 0) {
    delete params;
    task->SetDone(Status::Internal("launch write slice fail"));
    LOG(FATAL) << "[meta.batch_processor] start background thread fail.";
  }
}

Status MDSMetaSystem::AsyncFlushSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set,
                                      bool is_force, bool is_wait) {
  Ino ino = chunk_set->GetIno();

  uint32_t new_task_count = chunk_set->TryCommitSlice(is_force);

  uint32_t launched_count = 0;
  auto tasks = chunk_set->ListCommitTask();
  for (auto& task : tasks) {
    // Background writeback launches new tasks but does not reset a completed
    // failure: doing so could erase the status while a synchronous Flush is
    // consuming it.  A later explicit Flush owns the retry attempt.
    if (task->MaybeRun(/*retry_failed=*/is_wait)) {
      ++launched_count;
      LaunchWriteSlice(ctx, chunk_set, task);
    }
  }

  LOG_DEBUG << fmt::format(
      "[meta.fs.{}] async flush task new({}) launch({}) total({}).", ino,
      new_task_count, launched_count, tasks.size());

  if (is_wait) {
    for (auto& task : tasks) task->Wait();
    for (auto& task : tasks) {
      auto status = task->GetStatus();
      if (!status.ok()) return status;
    }
  }

  if (FLAGS_vfs_meta_compact_chunk_enable) {
    // check whether need compact chunk
    auto chunks = chunk_set->GetAll();
    for (auto& chunk : chunks) {
      auto status = chunk->IsNeedCompaction();
      if (status.ok()) {
        compact_processor_.Execute(ino, GetInodeFromCache(ino), chunk, true);
      }
    }
  }

  return Status::OK();
}

Status MDSMetaSystem::FlushSliceAndFile(ContextSPtr ctx, Ino ino) {
  auto file_session = file_session_map_.GetSession(ino);
  if (file_session == nullptr || !file_session->HasWriter()) {
    LOG_DEBUG << fmt::format(
        "[meta.fs.{}] flush file skip cause no file session or no writer.",
        ino);
    return Status::OK();
  }

  auto& chunk_set = file_session->GetChunkSet();

  // Freeze this inode's write frontier for the duration of the metadata
  // flush.  Without this barrier, another writer can keep appending to the
  // shared ChunkSet and prevent the drain loop from ever reaching empty.
  auto flush_guard = chunk_set->AcquireFlushGuard();

  do {
    bool has_stage = chunk_set->HasStage();
    bool has_committing = chunk_set->HasCommitting();
    bool has_commit_task = chunk_set->HasCommitTask();
    if (!has_stage && !has_committing && !has_commit_task) break;

    LOG_DEBUG << fmt::format(
        "[meta.fs.{}] flush all slice loop, has_stage({}) has_committing({}) "
        "has_commit_task({}).",
        ino, has_stage, has_committing, has_commit_task);

    auto status = AsyncFlushSlice(ctx, chunk_set, true, true);
    if (!status.ok()) return status;

  } while (true);

  // flush file length and data
  return DoFlushFile(ctx, GetInode(file_session, "flush"), chunk_set);
}

void MDSMetaSystem::FlushAllFile() {
  auto file_sessions = file_session_map_.GetAllSession();

  LOG_DEBUG << fmt::format("[meta.fs] flush all file, count({}).",
                           file_sessions.size());

  for (auto& file_session : file_sessions) {
    Ino ino = file_session->GetIno();
    ContextSPtr ctx = std::make_shared<Context>("");

    // FlushSliceAndFile drains all work on success.  On failure it deliberately
    // leaves the task retryable, but shutdown must not immediately retry it
    // forever and prevent the remaining components from stopping.
    auto status = FlushSliceAndFile(ctx, ino);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format("[meta.fs.{}] flush all file fail, error({}).",
                                ino, status.ToString());
    }
  }
}

bool MDSMetaSystem::GetChunkFromReadCache(Ino ino, uint32_t chunk_index,
                                          std::vector<Slice>* slices,
                                          uint64_t& version) {
  ChunkEntry chunk_entry;
  if (!read_chunk_cache_.Get(ino, chunk_index, chunk_entry)) {
    LOG_DEBUG << fmt::format("[meta.fs.{}.{}] miss chunk from read cache.", ino,
                             chunk_index);
    return false;
  }

  slices->clear();
  for (const auto& slice : chunk_entry.slices()) {
    slices->push_back(Helper::ToSlice(slice));
  }

  version = chunk_entry.version();

  return true;
}

Status MDSMetaSystem::CorrectAttr(ContextSPtr ctx, uint64_t time_ns, Attr& attr,
                                  bool& is_amend, const std::string& caller) {
  if (modify_time_memo_.ModifiedSince(attr.ino, time_ns)) {
    LOG_DEBUG << fmt::format("[meta.fs.{}] correct attr, caller({}).", attr.ino,
                             caller);
    // correct attr, fetch latest attr from mds
    InodeSPtr inode;
    ctx->reason = "correctattr." + caller;
    Status status = FetchInode(ctx, attr.ino, inode);
    if (!status.ok()) return status;

    attr = inode->ToAttr();
    is_amend = true;
  }

  // correct length with write memo
  if (CorrectAttrLength(attr, caller)) is_amend = true;

  return Status::OK();
}

bool MDSMetaSystem::CorrectAttrLength(Attr& attr, const std::string& caller) {
  if (mds::IsDir(attr.ino)) return false;

  auto file_session = file_session_map_.GetSession(attr.ino);
  if (file_session != nullptr) {
    auto chunk_set = file_session->GetChunkSet();
    uint64_t last_write_length = chunk_set->GetLastWriteLength();
    if (last_write_length != 0) {
      LOG_DEBUG << fmt::format("[meta.fs.{}] correct length, caller({}).",
                               attr.ino, caller);

      attr.length = std::max(last_write_length, attr.length);

      uint64_t last_write_time_ns = chunk_set->GetLastWriteTimeNs();
      attr.ctime = std::max(attr.ctime, last_write_time_ns);
      attr.mtime = std::max(attr.mtime, last_write_time_ns);

      return true;
    }
  }

  return false;
}

void MDSMetaSystem::ResetFileChunkSet(Ino ino, const std::string& reason) {
  LOG_DEBUG << fmt::format("[meta.fs.{}] reset file chunkset, reason({}).", ino,
                           reason);

  auto file_session = file_session_map_.GetSession(ino);
  if (file_session != nullptr) file_session->GetChunkSet()->Reset();

  chunk_memo_.Forget(ino);
}

void MDSMetaSystem::InvalidateFileSessionReadCache(Ino ino) {
  auto file_session = file_session_map_.GetSession(ino);
  if (file_session != nullptr) file_session->InvalidateReadCache(true);
}

Status MDSMetaSystem::Compact(ContextSPtr ctx, Ino ino, uint32_t chunk_index,
                              bool is_async) {
  // set chunk version
  mds::ChunkDescriptor chunk_descriptor;
  chunk_descriptor.set_index(chunk_index);
  chunk_descriptor.set_version(chunk_memo_.GetVersion(ino, chunk_index));

  mds::ChunkEntry chunk_entry;
  Status status =
      mds_client_.ReadSlice(ctx, ino, chunk_descriptor, chunk_entry);
  if (!status.ok() && !status.IsNotFound()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] reeadslice fail, error({}).",
                              ino, chunk_index, status.ToString());
    return status;
  }
  if (status.IsNotFound()) return Status::NoData("not found chunk");

  auto chunk_ptr = Chunk::New(ino, chunk_entry, "manual_compact");
  status = compact_processor_.Execute(ino, GetInodeFromCache(ino), chunk_ptr,
                                      is_async);
  if (!status.ok()) return status;

  return Status::OK();
}

Status MDSMetaSystem::CopyFileRange(ContextSPtr ctx, Ino src_ino,
                                    uint64_t src_off, Ino dst_ino,
                                    uint64_t dst_off, uint64_t len,
                                    uint32_t flags, uint64_t* bytes_copied) {
  AssertStop();
  CHECK(bytes_copied != nullptr) << "bytes_copied is null";

  if (len == 0) return Status::OK();
  if (src_ino == dst_ino && src_off == dst_off) {
    return Status::InvalidParam("src and dst are the same, no need to copy");
  }

  // flush src file
  Status status = FlushSliceAndFile(ctx, src_ino);
  if (!status.ok()) return status;

  MDSClient::CopyFileRangeParam param;
  param.src_ino = src_ino;
  param.dst_ino = dst_ino;
  param.src_off = src_off;
  param.dst_off = dst_off;
  param.len = len;
  param.flags = flags;

  MDSClient::AttrWithChunkOut out;
  status = mds_client_.CopyFileRange(ctx, param, out);
  if (!status.ok()) return status;

  *bytes_copied = out.bytes_copied;

  PutInodeToCache(out.attr_entry);

  if (!out.effected_chunks.empty()) {
    auto file_session = file_session_map_.GetSession(dst_ino);
    if (file_session != nullptr) {
      auto& chunk_set = file_session->GetChunkSet();
      chunk_set->Put(out.effected_chunks, "copyfilerange");
    }
  }

  chunk_memo_.Forget(dst_ino);
  modify_time_memo_.Remember(dst_ino);

  return Status::OK();
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
