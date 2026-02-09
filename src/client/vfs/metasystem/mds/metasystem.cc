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

#include <bthread/types.h>
#include <fcntl.h>
#include <openssl/rsa.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "client/vfs/common/client_id.h"
#include "client/vfs/metasystem/mds/helper.h"
#include "client/vfs/metasystem/mds/mds_client.h"
#include "client/vfs/vfs_meta.h"
#include "common/const.h"
#include "common/io_buffer.h"
#include "common/logging.h"
#include "common/options/client.h"
#include "common/status.h"
#include "common/trace/context.h"
#include "common/trace/trace_manager.h"
#include "compact.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "json/value.h"
#include "json/writer.h"
#include "mds/common/helper.h"
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

const std::string kExecutorWorkerSetName = "meta_worker_set";

DEFINE_uint32(vfs_meta_worker_num, 128, "number of meta workers");
DEFINE_uint32(vfs_meta_worker_max_pending_num, 1048576,
              "meta worker max pending num");

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
                                       TraceManager& trace_manager,
                                       Compactor& compactor) {
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
      executor_(kExecutorWorkerSetName, FLAGS_vfs_meta_worker_num,
                FLAGS_vfs_meta_worker_max_pending_num),
      mds_client_(client_id, fs_info_, std::move(rpc), trace_manager),
      inode_cache_(fs_info_.GetFsId()),
      id_cache_(kSliceIdCacheName, mds_client_),
      file_session_map_(inode_cache_, chunk_cache_),
      batch_processor_(mds_client_),
      compactor_(compactor) {}

MDSMetaSystem::~MDSMetaSystem() {}  // NOLINT

Status MDSMetaSystem::Init(bool upgrade) {
  LOG(INFO) << fmt::format("[meta.fs] init, upgrade({}).", upgrade);

  LOG(INFO) << fmt::format("[meta.fs] fs_info: {}.", fs_info_.ToString());

  if (!mds_client_.Init()) {
    return Status::MountFailed("init mds_client fail");
  }

  if (!executor_.Init()) {
    return Status::Internal("init executor fail");
  }

  if (!compact_processor_.Init()) {
    return Status::Internal("init compact processor fail");
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

  LOG(INFO) << fmt::format("[meta.fs] inited, upgrade({}).", upgrade);

  return Status::OK();
}

void MDSMetaSystem::Stop(bool upgrade) {
  LOG(INFO) << fmt::format("[meta.fs] stopping metasystem, upgrade({}).",
                           upgrade);

  stopped_.store(true);

  FlushAllSlice();

  crontab_manager_.Destroy();

  batch_processor_.Stop();

  executor_.Stop();

  compact_processor_.Stop();

  if (!upgrade) UnmountFs();

  mds_client_.Stop();

  LOG(INFO) << fmt::format("[meta.fs] stopped metasystem, upgrade({}).",
                           upgrade);
}

bool MDSMetaSystem::GetSummary(Json::Value& value) {
  CHECK(value.isArray()) << "value is not array.";

  Json::Value modify_time_memo_value = Json::objectValue;
  modify_time_memo_.Summary(modify_time_memo_value);
  value.append(modify_time_memo_value);

  Json::Value chunk_cache_value = Json::objectValue;
  chunk_cache_.Summary(chunk_cache_value);
  value.append(chunk_cache_value);

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

  Json::Value tiny_file_data_cache_value = Json::objectValue;
  tiny_file_data_cache_.Summary(tiny_file_data_cache_value);
  value.append(tiny_file_data_cache_value);

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

  if (options.chunk_cache && !chunk_cache_.Dump(value, options.is_summary)) {
    return false;
  }

  if (options.chunk_set) {
    LOG(INFO) << fmt::format("[meta.fs] dump chunk set, ino({}).", options.ino);
    auto chunk_set = chunk_cache_.GetOrCreate(options.ino);
    if (chunk_set != nullptr && !chunk_set->Dump(value)) return false;
  }

  if (options.chunk) {
    LOG(INFO) << fmt::format("[meta.fs] dump chunk, ino({}) index({}).",
                             options.ino, options.chunk_index);
    auto chunk_set = chunk_cache_.GetOrCreate(options.ino);
    if (chunk_set == nullptr) return true;

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

  LOG(INFO) << "[meta.fs] loaded...";

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

  auto status =
      mds_client_.Heartbeat(file_session_map_.GetNeedKeepAliveSession());
  if (!status.IsOK()) {
    LOG(ERROR) << fmt::format("[meta.fs] heartbeat fail, error({}).",
                              status.ToString());
  }

  is_running = false;
}

void MDSMetaSystem::CleanExpiredModifyTimeMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  modify_time_memo_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredChunkMemo() {
  uint64_t expired_time_s = utils::Timestamp() - FLAGS_vfs_meta_memo_expired_s;

  chunk_memo_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredChunkCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_chunk_cache_expired_s;
  chunk_cache_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredInodeCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_inode_cache_expired_s;

  inode_cache_.CleanExpired(expired_time_s);
}

void MDSMetaSystem::CleanExpiredTinyFileDataCache() {
  uint64_t expired_time_s =
      utils::Timestamp() - FLAGS_vfs_meta_tiny_file_data_cache_expired_s;

  tiny_file_data_cache_.CleanExpired(expired_time_s);
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
        this->CleanExpiredChunkCache();
        this->CleanExpiredInodeCache();
        this->CleanExpiredTinyFileDataCache();
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

  std::string session_id = utils::GenerateUUID();
  AttrEntry attr_entry, parent_attr_entry;
  auto status = mds_client_.Create(ctx, parent, name, uid, gid, mode, flags,
                                   session_id, attr_entry, parent_attr_entry);
  if (!status.ok()) return status;

  *attr = Helper::ToAttr(attr_entry);

  InodeSPtr inode = PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);
  if (FLAGS_vfs_tiny_file_data_enable) {
    tiny_file_data_cache_.Create(attr_entry.ino());
  }

  // add file session
  auto file_session =
      file_session_map_.Put(attr_entry.ino(), fh, session_id, flags);
  file_session->SetInode(inode);

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

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

bool MDSMetaSystem::IsPrefetchChunk(Ino ino) {
  auto chunk_set = chunk_cache_.Get(ino);
  if (chunk_set == nullptr) return true;

  // todo: check whether all chunks are completed
  // size_t chunk_size = chunk_set->GetChunkSize();

  return false;
}

bool MDSMetaSystem::IsPrefetchTinyFileData(Ino ino) {
  if (!FLAGS_vfs_tiny_file_data_enable) return false;

  auto inode = inode_cache_.Get(ino);
  if (inode == nullptr) return false;
  if (!inode->MaybeTinyFile()) return false;

  auto data_buffer = tiny_file_data_cache_.Get(ino);
  if (data_buffer == nullptr) return true;

  if (data_buffer->IsOutOfRange()) return false;
  if (!data_buffer->IsComplete()) return true;

  return false;
}

Status MDSMetaSystem::DoOpen(ContextSPtr ctx, Ino ino, int flags, uint64_t fh,
                             const std::string& session_id,
                             FileSessionSPtr file_session) {
  CHECK(file_session != nullptr) << "file_session is null.";

  // check whether prefetch chunk
  // prepare chunk descriptors for expect chunk version
  bool is_prefetch_chunk = IsPrefetchChunk(ino);
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

  // check whether prefetch tiny file data
  bool is_prefetch_data = IsPrefetchTinyFileData(ino);

  AttrEntry attr_entry;
  std::vector<mds::ChunkEntry> chunks;
  std::string tiny_file_data;
  uint64_t data_version = 0;
  auto status = mds_client_.Open(
      ctx, ino, flags, session_id, is_prefetch_chunk, chunk_descriptors,
      is_prefetch_data, attr_entry, chunks, tiny_file_data, data_version);

  LOG(INFO) << fmt::format(
      "[meta.fs.{}.{}] open file flags({:o}:{}) session_id({}) "
      "chunks({}:{}) tiny_file_data({}:{}) status({}).",
      ino, fh, flags, mds::Helper::DescOpenFlags(flags), session_id,
      is_prefetch_chunk, chunks.size(), is_prefetch_data, tiny_file_data.size(),
      status.ToString());
  if (!status.ok()) return status;

  // update inode cache
  InodeSPtr inode = PutInodeToCache(attr_entry);
  file_session->SetInode(inode);

  // truncate file, forget chunk memo and delete chunk cache
  if (flags & O_TRUNC) {
    chunk_memo_.Forget(ino);
    chunk_cache_.Delete(ino);
  }

  // update chunk cache
  auto& chunk_set = file_session->GetChunkSet();
  if (!chunks.empty()) chunk_set->Put(chunks, "open");

  // update chunk memo
  for (const auto& chunk : chunks) {
    chunk_memo_.Remember(ino, chunk.index(), chunk.version());
  }

  // update tiny file data cache
  if (is_prefetch_data) {
    if (flags & O_TRUNC) {
      tiny_file_data_cache_.Create(ino);

    } else {
      auto data_buffer = tiny_file_data_cache_.GetOrCreate(ino);
      if (attr_entry.length() == 0) {
        CHECK(tiny_file_data.empty()) << fmt::format(
            "[meta.fs.{}.{}] tiny file data not empty, data_version({}).", ino,
            fh, data_version);
        ++data_version;
      }
      data_buffer->Put(tiny_file_data, data_version);
    }
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
      auto inode = metasystem_.inode_cache_.Get(ino_);
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

Status MDSMetaSystem::Open(ContextSPtr ctx, Ino ino, int flags, uint64_t fh) {
  AssertStop();

  if ((flags & O_TRUNC) && !(flags & O_WRONLY || flags & O_RDWR)) {
    return Status::NoPermission("O_TRUNC without O_WRONLY or O_RDWR");
  }

  const std::string session_id = utils::GenerateUUID();
  auto file_session = file_session_map_.Put(ino, fh, session_id, flags);

  auto inode = GetInodeFromCache(ino);
  if (inode != nullptr) {
    file_session->SetInode(inode);

    if (!(flags & O_TRUNC)) {
      // launch async open
      AsyncOpen(ctx, ino, flags, fh, session_id, file_session);

      return Status::OK();
    }
  }

  auto status = DoOpen(ctx, ino, flags, fh, session_id, file_session);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] open file fail, error({}).", ino,
                              fh, status.ToString());
    file_session_map_.Delete(ino, fh);
  }

  return status;
}

Status MDSMetaSystem::Flush(ContextSPtr ctx, Ino ino, uint64_t fh) {
  AssertStop();

  LOG(INFO) << fmt::format("[meta.fs.{}.{}] flush.", ino, fh);

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

  uint32_t flags = file_session->GetFlags(fh);

  // only flush when file opened with write flag
  if (!(flags & O_WRONLY || flags & O_RDWR)) {
    LOG(INFO) << fmt::format(
        "[meta.fs.{}.{}] flush skipped, file opened with read flag.", ino, fh);
    return Status::OK();
  }

  return FlushSlice(ctx, ino);
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
      auto inode = metasystem_.inode_cache_.Get(ino_);
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
  CHECK(!session_id.empty())
      << fmt::format("get file session fail, ino({}) fh({}).", ino, fh);

  file_session_map_.Delete(ino, fh);

  LOG(INFO) << fmt::format("[meta.fs.{}.{}] close file session_id({}).", ino,
                           fh, session_id);

  AsyncClose(ctx, ino, fh, session_id);

  return Status::OK();
}

Status MDSMetaSystem::ReadSlice(ContextSPtr ctx, Ino ino, uint64_t index,
                                uint64_t fh, std::vector<Slice>* slices,
                                uint64_t& version) {
  AssertStop();

  auto chunk_set = chunk_cache_.GetOrCreate(ino);

  do {
    auto chunk = chunk_set->Get(index);
    if (chunk != nullptr && chunk->IsCompleted()) {
      *slices = chunk->GetAllSlice(version);
      // ctx->hit_cache = true;

      LOG_DEBUG << fmt::format(
          "[meta.fs.{}.{}.{}] readslice, version({}) slices({}).", ino, fh,
          index, version, Helper::ToString(*slices));
      return Status::OK();
    }

    // set chunk version
    mds::ChunkDescriptor chunk_descriptor;
    chunk_descriptor.set_index(static_cast<uint32_t>(index));
    chunk_descriptor.set_version(
        chunk_memo_.GetVersion(ino, static_cast<uint32_t>(index)));

    std::vector<mds::ChunkEntry> chunks;
    auto status = mds_client_.ReadSlice(ctx, ino, {chunk_descriptor}, chunks);
    if (!status.ok()) {
      LOG(ERROR) << fmt::format(
          "[meta.fs.{}.{}.{}] reeadslice fail, error({}).", ino, fh, index,
          status.ToString());
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
    chunk_set->Put(chunks, "readslice");
    // update chunk memo
    for (const auto& chunk : chunks) {
      chunk_memo_.Remember(ino, chunk.index(), chunk.version());

      LOG(INFO) << fmt::format("[meta.fs.{}.{}.{}] fetch slice, version({}).",
                               ino, fh, index, chunk.version());
    }

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

  LOG_DEBUG << fmt::format("[meta.fs.{}.{}.{}] writeslice, slices({}).", ino,
                           fh, index, Helper::ToString(slices));

  auto chunk_set = chunk_cache_.GetOrCreate(ino);
  chunk_set->Append(index, slices);

  AsyncFlushSlice(ctx, chunk_set, false, false);

  return Status::OK();
}

Status MDSMetaSystem::Write(ContextSPtr, Ino ino, const char* buf,
                            uint64_t offset, uint64_t size, uint64_t fh) {
  AssertStop();

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  auto& chunk_set = file_session->GetChunkSet();
  chunk_set->SetLastWriteLength(offset, size);

  if (FLAGS_vfs_tiny_file_data_enable) {
    // tiny file write data
    auto inode = GetInode(file_session);
    if (inode != nullptr && inode->MaybeTinyFile()) {
      auto data_buffer = tiny_file_data_cache_.GetOrCreate(ino);
      data_buffer->Write(buf, offset, size);
    }
  }

  // update last modify time
  modify_time_memo_.Remember(ino);

  // check whether need compact chunk
  // if (FLAGS_vfs_meta_compact_chunk_enable) {
  //   uint32_t chunk_index = offset / fs_info_.GetChunkSize();
  //   auto chunk = chunk_set->Get(chunk_index);
  //   if (chunk != nullptr) {
  //     auto status = chunk->IsNeedCompaction();
  //     if (status.ok()) {
  //       compact_processor_.LaunchCompact(ino, chunk, mds_client_, compactor_,
  //                                        true);
  //     }
  //   }
  // }

  return Status::OK();
}

Status MDSMetaSystem::Read(ContextSPtr, Ino ino, uint64_t fh, uint64_t offset,
                           uint64_t size, vfs::DataBuffer& data_buffer,
                           uint64_t& out_rsize) {
  AssertStop();

  LOG_DEBUG << fmt::format("[meta.fs.{}.{}] read, offset({}) size({}).", ino,
                           fh, offset, size);

  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}) fh({}).", ino, fh);

  auto inode = GetInode(file_session);
  if (!inode->MaybeTinyFile()) {
    return Status::NoData("not tiny file");
  }

  auto data = tiny_file_data_cache_.Get(ino);
  if (data == nullptr) {
    return Status::NoData("tiny file data not found");
  }
  if (data->IsOutOfRange()) {
    return Status::NoData("tiny file data is out of range");
  }

  auto* io_buffer = data_buffer.RawIOBuffer();

  if (!data->Read(offset, size, *io_buffer)) {
    return Status::NoData("tiny file read data fail");
  }

  out_rsize = io_buffer->Size();

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

  DeleteInodeFromCache(ino);
  PutInodeToCache(parent_attr_entry);

  return Status::OK();
}

Status MDSMetaSystem::OpenDir(ContextSPtr, Ino ino, uint64_t fh,
                              bool& need_cache) {
  AssertStop();

  auto dir_iterator = DirIterator::New(mds_client_, ino, fh);

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

    bool is_amend = false;
    CorrectAttr(ctx, dir_iterator->LastFetchTimeNs(), entry.attr, is_amend,
                "readdir");

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

  PutInodeToCache(attr_entry);
  PutInodeToCache(parent_attr_entry);
  if (attr_entry.nlink() == 0) {
    DeleteInodeFromCache(attr_entry.ino());
    chunk_cache_.Delete(attr_entry.ino());
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
    // ctx->hit_cache = true;

  } else {
    auto status = mds_client_.GetAttr(ctx, ino, attr_entry);
    if (!status.ok()) return status;
  }

  *attr = Helper::ToAttr(attr_entry);

  bool is_amend = false;
  auto status =
      CorrectAttr(ctx, ctx->start_time_ns, *attr, is_amend, "getattr");
  if (!status.ok()) return status;

  LOG_DEBUG << fmt::format("[meta.fs.{}] get attr length({}) is_amend({}).",
                           ino, attr->length, is_amend);
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

  bool is_amend = false;
  status = CorrectAttr(ctx, ctx->start_time_ns, *out_attr, is_amend, "setattr");
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
    // ctx->hit_cache = true;
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

Status MDSMetaSystem::FlushFile(ContextSPtr ctx, InodeSPtr inode,
                                ChunkSetSPtr& chunk_set) {
  CHECK(inode != nullptr) << "inode is null.";
  CHECK(chunk_set != nullptr) << "chunk_set is null.";

  Ino ino = inode->Ino();
  uint64_t last_write_length = chunk_set->GetLastWriteLength();
  if (last_write_length == 0) {
    LOG(INFO) << fmt::format(
        "[meta.fs.{}] flush file skip cause no write data, length({}).", ino,
        inode->Length());
    return Status::OK();
  }

  if (last_write_length <= inode->Length()) last_write_length = 0;

  // get tiny file data
  std::string data;
  if (FLAGS_vfs_tiny_file_data_enable && inode->MaybeTinyFile()) {
    auto data_buffer = tiny_file_data_cache_.GetOrCreate(ino);
    if (data_buffer != nullptr && data_buffer->IsComplete()) {
      data_buffer->Copy(data);
    }
  }

  AttrEntry attr_entry;
  bool shrink_file;
  auto status = mds_client_.FlushFile(ctx, ino, last_write_length,
                                      std::move(data), attr_entry, shrink_file);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format(
        "[meta.fs.{}] flush file fail, length({}) error({}).", ino,
        last_write_length, status.ToString());
    return status;
  }

  modify_time_memo_.Remember(ino);
  if (shrink_file) chunk_memo_.Forget(ino);

  PutInodeToCache(attr_entry);

  return Status::OK();
}

void MDSMetaSystem::LaunchWriteSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set,
                                     CommitTaskSPtr task) {
  Ino ino = chunk_set->GetIno();

  LOG(INFO) << fmt::format("[meta.fs.{}] launch write slice {}.", ino,
                           task->Describe());

  auto operation = std::make_shared<WriteSliceOperation>(
      ctx, ino, task,
      [this, chunk_set, ino](const Status& status, CommitTaskSPtr task,
                             const std::vector<mds::ChunkEntry>& chunks) {
        task->SetDone(status);

        if (status.ok()) {
          LOG(INFO) << fmt::format(
              "[meta.fs.{}] flush delta slice done, task({}) status({}).", ino,
              task->TaskID(), status.ToString());

          chunk_set->FinishCommitTask(task->TaskID(), chunks);

          chunk_memo_.Remember(ino, chunks);

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

void MDSMetaSystem::AsyncFlushSlice(ContextSPtr& ctx, ChunkSetSPtr chunk_set,
                                    bool is_force, bool is_wait) {
  Ino ino = chunk_set->GetIno();

  uint32_t new_task_count = chunk_set->TryCommitSlice(is_force);

  uint32_t launched_count = 0;
  auto tasks = chunk_set->ListCommitTask();
  for (auto& task : tasks) {
    if (task->MaybeRun()) {
      ++launched_count;
      LaunchWriteSlice(ctx, chunk_set, task);
    }
  }

  LOG(INFO) << fmt::format(
      "[meta.fs.{}] async flush task new({}) launch({}) total({}).", ino,
      new_task_count, launched_count, tasks.size());

  if (is_wait) {
    for (auto& task : tasks) task->Wait();
  }

  if (FLAGS_vfs_meta_compact_chunk_enable) {
    // check whether need compact chunk
    auto chunks = chunk_set->GetAll();
    for (auto& chunk : chunks) {
      auto status = chunk->IsNeedCompaction();
      if (status.ok()) {
        compact_processor_.LaunchCompact(ino, inode_cache_.Get(ino), chunk,
                                         mds_client_, compactor_, true);
      }
    }
  }
}

Status MDSMetaSystem::FlushSlice(ContextSPtr ctx, Ino ino) {
  auto file_session = file_session_map_.GetSession(ino);
  CHECK(file_session != nullptr)
      << fmt::format("file session is nullptr, ino({}).", ino);

  auto& chunk_set = file_session->GetChunkSet();

  LOG(INFO) << fmt::format("[meta.fs.{}] flush all slice.", ino);

  do {
    bool has_stage = chunk_set->HasStage();
    bool has_committing = chunk_set->HasCommitting();
    bool has_commit_task = chunk_set->HasCommitTask();
    if (!has_stage && !has_committing && !has_commit_task) break;

    LOG(INFO) << fmt::format(
        "[meta.fs.{}] flush all slice loop, has_stage({}) has_committing({}) "
        "has_commit_task({}).",
        ino, has_stage, has_committing, has_commit_task);

    AsyncFlushSlice(ctx, chunk_set, true, true);

  } while (true);

  // flush file length and data
  return FlushFile(ctx, GetInode(file_session), chunk_set);
}

void MDSMetaSystem::FlushAllSlice() {
  auto file_sessions = file_session_map_.GetAllSession();

  LOG(INFO) << fmt::format("[meta.fs] flush all slice, count({}).",
                           file_sessions.size());

  do {
    for (auto& file_session : file_sessions) {
      Ino ino = file_session->GetIno();
      ContextSPtr ctx = std::make_shared<Context>("");

      auto status = FlushSlice(ctx, ino);
      if (!status.ok()) {
        LOG(ERROR) << fmt::format(
            "[meta.fs.{}] flush all slice fail, error({}).", ino,
            status.ToString());
      }
    }

    if (!chunk_cache_.HasUncommitedSlice()) break;

    LOG(INFO) << "[meta.fs] flush all slice loop, still has uncommited slice.";

  } while (true);
}

Status MDSMetaSystem::CorrectAttr(ContextSPtr ctx, uint64_t time_ns, Attr& attr,
                                  bool& is_amend, const std::string& caller) {
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
    is_amend = true;
  }

  // correct length with write memo
  if (CorrectAttrLength(attr, caller)) is_amend = true;

  return Status::OK();
}

bool MDSMetaSystem::CorrectAttrLength(Attr& attr, const std::string& caller) {
  auto file_session = file_session_map_.GetSession(attr.ino);
  if (file_session != nullptr) {
    auto chunk_set = file_session->GetChunkSet();
    uint64_t last_write_length = chunk_set->GetLastWriteLength();
    if (last_write_length != 0) {
      LOG(INFO) << fmt::format("[meta.fs.{}] correct length, caller({}).",
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

Status MDSMetaSystem::Compact(ContextSPtr ctx, Ino ino, uint32_t chunk_index,
                              bool is_async) {
  // set chunk version
  mds::ChunkDescriptor chunk_descriptor;
  chunk_descriptor.set_index(chunk_index);
  chunk_descriptor.set_version(chunk_memo_.GetVersion(ino, chunk_index));

  std::vector<mds::ChunkEntry> chunks;
  auto status = mds_client_.ReadSlice(ctx, ino, {chunk_descriptor}, chunks);
  if (!status.ok()) {
    LOG(ERROR) << fmt::format("[meta.fs.{}.{}] reeadslice fail, error({}).",
                              ino, chunk_index, status.ToString());
    return status;
  }
  if (chunks.empty()) {
    return Status::NoData("not found chunk");
  }

  for (auto& chunk : chunks) {
    auto chunk_ptr = Chunk::New(ino, chunk, "manual_compact");
    auto status =
        compact_processor_.LaunchCompact(ino, inode_cache_.Get(ino), chunk_ptr,
                                         mds_client_, compactor_, is_async);
    if (!status.ok()) return status;
  }

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