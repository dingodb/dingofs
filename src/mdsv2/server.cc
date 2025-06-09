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

#include "mdsv2/server.h"

#include <cstdint>
#include <string>
#include <utility>

#include "fmt/core.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "mdsv2/background/fsinfo_sync.h"
#include "mdsv2/background/heartbeat.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/version.h"
#include "mdsv2/coordinator/dingo_coordinator_client.h"
#include "mdsv2/service/debug_service.h"
#include "mdsv2/service/fsstat_service.h"
#include "mdsv2/service/mds_service.h"
#include "mdsv2/statistics/fs_stat.h"
#include "mdsv2/storage/dingodb_storage.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_int32(heartbeat_interval_s, 5, "heartbeat interval seconds");
DEFINE_int32(fsinfosync_interval_s, 10, "fs info sync interval seconds");
DEFINE_int32(mdsmonitor_interval_s, 5, "mds monitor interval seconds");
DEFINE_int32(quota_sync_interval_s, 6, "quota sync interval seconds");
DEFINE_int32(gc_interval_s, 60, "gc interval seconds");

DEFINE_string(mdsmonitor_lock_name, "/lock/mds/monitor", "mds monitor lock name");
DEFINE_string(gc_lock_name, "/lock/mds/gc", "gc lock name");
DEFINE_int32(compact_chunk_interval_s, 5, "compact chunk interval seconds");

DEFINE_uint32(read_worker_num, 128, "read service worker num");
DEFINE_uint64(read_worker_max_pending_num, 1024, "read service worker num");
DEFINE_bool(read_worker_set_use_pthread, false, "read worker set use pthread");

DEFINE_uint32(write_worker_num, 128, "write service worker num");
DEFINE_uint64(write_worker_max_pending_num, 1024, "write service worker num");
DEFINE_bool(write_worker_set_use_pthread, false, "write worker set use pthread");

DEFINE_string(pid_file_name, "pid", "pid file name");

const std::string kReadWorkerSetName = "READ_WORKER_SET";
const std::string kWriteWorkerSetName = "WRITE_WORKER_SET";

static const int64_t kFsTableId = 1000;
static const int64_t kFsIdBatchSize = 8;
static const int64_t kFsIdStartId = 20000;

static const int64_t kSliceTableId = 1001;
static const int64_t kSliceIdBatchSize = 8;
static const int64_t kSliceIdStartId = 10000000;

Server::~Server() {}  // NOLINT

Server& Server::GetInstance() {
  static Server instance;
  return instance;
}

LogLevel GetDingoLogLevel(const std::string& log_level) {
  if (Helper::IsEqualIgnoreCase(log_level, "DEBUG")) {
    return LogLevel::kDEBUG;
  } else if (Helper::IsEqualIgnoreCase(log_level, "INFO")) {
    return LogLevel::kINFO;
  } else if (Helper::IsEqualIgnoreCase(log_level, "WARNING")) {
    return LogLevel::kWARNING;
  } else if (Helper::IsEqualIgnoreCase(log_level, "ERROR")) {
    return LogLevel::kERROR;
  } else if (Helper::IsEqualIgnoreCase(log_level, "FATAL")) {
    return LogLevel::kFATAL;
  } else {
    return LogLevel::kINFO;
  }
}

bool Server::InitConfig(const std::string& path) {
  DINGO_LOG(INFO) << "Init config: " << path;

  conf_.SetConfigPath(path);
  return conf_.LoadConfig();
}

bool Server::InitLog() {
  DINGO_LOG(INFO) << "init log.";

  std::string log_path;
  conf_.GetValueFatalIfFail("log.path", &log_path);

  std::string log_level;
  conf_.GetValueFatalIfFail("log.level", &log_level);

  DINGO_LOG(INFO) << fmt::format("Init log: {} {}", log_level, log_path);

  DingoLogger::InitLogger(log_path, "mdsv2", GetDingoLogLevel(log_level));

  DingoLogVerion();
  return true;
}

bool Server::InitMDSMeta() {
  DINGO_LOG(INFO) << "init mds meta.";

  int id;
  conf_.GetValueFatalIfFail("id", &id);

  mds_meta_.SetID(id);

  std::string listen_addr;
  conf_.GetValueFatalIfFail("listen.addr", &listen_addr);
  std::string host;
  int port;
  if (!Helper::ParseAddr(listen_addr, host, port)) {
    return false;
  }

  mds_meta_.SetHost(host);
  mds_meta_.SetPort(port);
  mds_meta_.SetState(MDSMeta::State::kNormal);

  DINGO_LOG(INFO) << fmt::format("init mds meta, self: {}.", mds_meta_.ToString());

  mds_meta_map_ = MDSMetaMap::New();
  CHECK(mds_meta_map_ != nullptr) << "new MDSMetaMap fail.";

  mds_meta_map_->UpsertMDSMeta(mds_meta_);

  return true;
}

bool Server::InitCoordinatorClient(const std::string& coor_url) {
  DINGO_LOG(INFO) << fmt::format("init coordinator client, addr({}).", coor_url);

  std::string coor_addrs = Helper::ParseCoorAddr(coor_url);
  if (coor_addrs.empty()) {
    return false;
  }

  coordinator_client_ = DingoCoordinatorClient::New();
  CHECK(coordinator_client_ != nullptr) << "new DingoCoordinatorClient fail.";

  return coordinator_client_->Init(coor_addrs);
}

bool Server::InitStorage(const std::string& store_url) {
  DINGO_LOG(INFO) << fmt::format("init storage, url({}).", store_url);
  CHECK(!store_url.empty()) << "store url is empty.";

  kv_storage_ = DingodbStorage::New();
  CHECK(kv_storage_ != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = Helper::ParseCoorAddr(store_url);
  if (store_addrs.empty()) {
    return false;
  }

  return kv_storage_->Init(store_addrs);
}

bool Server::InitOperationProcessor() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";

  operation_processor_ = OperationProcessor::New(kv_storage_);
  CHECK(operation_processor_ != nullptr) << "new OperationProcessor fail.";

  return operation_processor_->Init();
}

bool Server::InitFileSystem() {
  DINGO_LOG(INFO) << "init filesystem.";

  CHECK(coordinator_client_ != nullptr) << "coordinator client is nullptr.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is nullptr.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  auto fs_id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kFsTableId, kFsIdStartId, kFsIdBatchSize);
  CHECK(fs_id_generator != nullptr) << "new fs AutoIncrementIdGenerator fail.";
  CHECK(fs_id_generator->Init()) << "init fs AutoIncrementIdGenerator fail.";

  auto slice_id_generator =
      AutoIncrementIdGenerator::New(coordinator_client_, kSliceTableId, kSliceIdStartId, kSliceIdBatchSize);
  CHECK(slice_id_generator != nullptr) << "new slice AutoIncrementIdGenerator fail.";
  CHECK(slice_id_generator->Init()) << "init slice AutoIncrementIdGenerator fail.";

  file_system_set_ = FileSystemSet::New(coordinator_client_, std::move(fs_id_generator), std::move(slice_id_generator),
                                        kv_storage_, mds_meta_, mds_meta_map_, operation_processor_, notify_buddy_);
  CHECK(file_system_set_ != nullptr) << "new FileSystem fail.";

  return file_system_set_->Init();
}

bool Server::InitHeartbeat() {
  DINGO_LOG(INFO) << "init heartbeat.";
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";

  heartbeat_ = Heartbeat::New(operation_processor_);
  return heartbeat_->Init();
}

bool Server::InitFsInfoSync() {
  DINGO_LOG(INFO) << "init fs info sync.";
  return fs_info_sync_.Init();
}

bool Server::InitWorkerSet() {
  read_worker_set_ = SimpleWorkerSet::New(kReadWorkerSetName, FLAGS_read_worker_num, FLAGS_read_worker_max_pending_num,
                                          FLAGS_read_worker_set_use_pthread, false);
  if (!read_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service read worker set fail!";
    return false;
  }

  write_worker_set_ =
      SimpleWorkerSet::New(kWriteWorkerSetName, FLAGS_write_worker_num, FLAGS_write_worker_max_pending_num,
                           FLAGS_write_worker_set_use_pthread, false);
  if (!write_worker_set_->Init()) {
    DINGO_LOG(ERROR) << "init service write worker set fail!";
    return false;
  }

  return true;
}

bool Server::InitNotifyBuddy() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  notify_buddy_ = notify::NotifyBuddy::New(mds_meta_map_, mds_meta_.ID());

  return notify_buddy_->Init();
}

bool Server::InitMonitor() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is nullptr.";
  CHECK(mds_meta_.ID() > 0) << "mds id is invalid.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_mdsmonitor_lock_name, mds_meta_.ID());
  CHECK(dist_lock != nullptr) << "gc dist lock is nullptr.";

  monitor_ = Monitor::New(file_system_set_, dist_lock, notify_buddy_);
  CHECK(monitor_ != nullptr) << "new MDSMonitor fail.";

  CHECK(monitor_->Init()) << "init MDSMonitor fail.";

  return true;
}

bool Server::InitQuotaSynchronizer() {
  CHECK(file_system_set_ != nullptr) << "file_system_set is nullptr.";

  quota_synchronizer_ = QuotaSynchronizer::New(file_system_set_);
  CHECK(quota_synchronizer_ != nullptr) << "new QuotaSynchronizer fail.";

  return true;
}

bool Server::InitGcProcessor() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(file_system_set_ != nullptr) << "file system set is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_gc_lock_name, mds_meta_.ID());
  CHECK(dist_lock != nullptr) << "gc dist lock is nullptr.";

  gc_processor_ = GcProcessor::New(file_system_set_, kv_storage_, dist_lock);

  CHECK(gc_processor_->Init()) << "init GcProcessor fail.";

  return true;
}

bool Server::InitCrontab() {
  DINGO_LOG(INFO) << "init crontab.";

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      FLAGS_heartbeat_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetHeartbeat()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "FSINFO_SYNC",
      FLAGS_fsinfosync_interval_s * 1000,
      true,
      [](void*) { FsInfoSync::TriggerFsInfoSync(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "MDS_MONITOR",
      FLAGS_mdsmonitor_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetMonitor()->Run(); },
  });

  // Add quota sync crontab
  crontab_configs_.push_back({
      "QUOTA_SYNC",
      FLAGS_quota_sync_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetQuotaSynchronizer()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "GC",
      FLAGS_gc_interval_s * 1000,
      true,
      [](void*) { Server::GetInstance().GetGcProcessor()->Run(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

std::string Server::GetPidFilePath() {
  std::string log_path;
  conf_.GetValueFatalIfFail("log.path", &log_path);

  return log_path + "/" + FLAGS_pid_file_name;
}

std::string Server::GetListenAddr() {
  std::string addr;
  conf_.GetValueFatalIfFail("listen.addr", &addr);
  return addr;
}

MDSMeta& Server::GetMDSMeta() { return mds_meta_; }

MDSMetaMapSPtr Server::GetMDSMetaMap() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  return mds_meta_map_;
}

KVStorageSPtr Server::GetKVStorage() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  return kv_storage_;
}

HeartbeatSPtr Server::GetHeartbeat() {
  CHECK(heartbeat_ != nullptr) << "heartbeat is nullptr.";

  return heartbeat_;
}

FsInfoSync& Server::GetFsInfoSync() { return fs_info_sync_; }

CoordinatorClientSPtr Server::GetCoordinatorClient() {
  CHECK(coordinator_client_ != nullptr) << "coordinator_client is nullptr.";

  return coordinator_client_;
}

FileSystemSetSPtr Server::GetFileSystemSet() {
  CHECK(coordinator_client_ != nullptr) << "coordinator_client is nullptr.";

  return file_system_set_;
}

notify::NotifyBuddySPtr Server::GetNotifyBuddy() {
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  return notify_buddy_;
}

MonitorSPtr Server::GetMonitor() {
  CHECK(monitor_ != nullptr) << "mds_monitor is nullptr.";

  return monitor_;
}

OperationProcessorSPtr Server::GetOperationProcessor() {
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  return operation_processor_;
}

QuotaSynchronizerSPtr Server::GetQuotaSynchronizer() {
  CHECK(quota_synchronizer_ != nullptr) << "quota_synchronizer is nullptr.";

  return quota_synchronizer_;
}

GcProcessorSPtr Server::GetGcProcessor() {
  CHECK(gc_processor_ != nullptr) << "gc_processor is nullptr.";

  return gc_processor_;
}

void Server::Run() {
  CHECK(read_worker_set_ != nullptr) << "read worker set is nullptr.";
  CHECK(write_worker_set_ != nullptr) << "write worker set is nullptr.";
  CHECK(file_system_set_ != nullptr) << "file system set is nullptr.";
  CHECK(gc_processor_ != nullptr) << "gc_processor is nullptr.";

  auto fs_stats = FsStats::New(kv_storage_);
  CHECK(fs_stats != nullptr) << "fsstats is nullptr.";

  MDSServiceImpl mds_service(read_worker_set_, write_worker_set_, file_system_set_, gc_processor_, std::move(fs_stats));
  CHECK(brpc_server_.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  DebugServiceImpl debug_service(file_system_set_);
  CHECK(brpc_server_.AddService(&debug_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add debug service error.";

  FsStatServiceImpl fs_stat_service;
  CHECK(brpc_server_.AddService(&fs_stat_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add fsstat service error.";

  brpc::ServerOptions option;
  CHECK(brpc_server_.Start(GetListenAddr().c_str(), &option) == 0) << "start brpc server error.";

  while (!brpc::IsAskedToQuit() && !stop_.load()) {
    bthread_usleep(1000000L);
  }
}

void Server::Stop() {
  // Only stop once
  bool expected = false;
  if (!stop_.compare_exchange_strong(expected, true)) {
    return;
  }

  brpc_server_.Stop(0);
  brpc_server_.Join();
  operation_processor_->Destroy();
  heartbeat_->Destroy();
  crontab_manager_.Destroy();
  read_worker_set_->Destroy();
  write_worker_set_->Destroy();
  monitor_->Destroy();
}

}  // namespace mdsv2
}  // namespace dingofs
