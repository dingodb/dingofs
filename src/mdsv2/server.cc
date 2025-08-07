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

#include <fmt/format.h>
#include <gflags/gflags_declare.h>

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

DEFINE_string(mdsmonitor_lock_name, "/lock/mds/monitor", "mds monitor lock name");
DEFINE_string(gc_lock_name, "/lock/mds/gc", "gc lock name");

DEFINE_string(pid_file_name, "pid", "pid file name");

DECLARE_string(worker_set_type);

const std::string kQuotaWorkerSetName = "QUOTA_WORKER_SET";
DEFINE_uint32(quota_worker_num, 24, "quota worker number");
DEFINE_uint32(quota_worker_max_pending_num, 4096, "quota worker max pending number");

Server::~Server() {}  // NOLINT

Server& Server::GetInstance() {
  static Server instance;
  return instance;
}

static LogLevel GetDingoLogLevel(const std::string& log_level) {
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

  if (!app_option_.Parse(path)) {
    DINGO_LOG(ERROR) << "parse config file fail.";
    return false;
  }

  const auto& server_option = app_option_.server();
  if (server_option.id() == 0) {
    DINGO_LOG(ERROR) << "mds server id is 0, please set a valid id.";
    return false;
  }
  if (server_option.host().empty()) {
    DINGO_LOG(ERROR) << "mds server host is empty, please set a valid host.";
    return false;
  }
  if (server_option.host() == "0.0.0.0") {
    DINGO_LOG(ERROR) << "mds server host is 0.0.0.0, can't set it.";
    return false;
  }
  if (server_option.port() == 0) {
    DINGO_LOG(ERROR) << "mds server port is 0, please set a valid port.";
    return false;
  }

  const auto& log_option = app_option_.log();
  if (log_option.level().empty()) {
    DINGO_LOG(ERROR) << "mds log level is empty, please set a valid log level.";
    return false;
  }
  if (log_option.path().empty()) {
    DINGO_LOG(ERROR) << "mds log path is empty, please set a valid log path.";
    return false;
  }

  return true;
}

bool Server::InitLog() {
  DINGO_LOG(INFO) << "init log.";

  const auto& log_option = app_option_.log();

  DINGO_LOG(INFO) << fmt::format("Init log: {} {}", log_option.level(), log_option.path());

  DingoLogger::InitLogger(log_option.path(), "mdsv2", GetDingoLogLevel(log_option.level()));

  DingoLogVersion();
  return true;
}

bool Server::InitMDSMeta() {
  DINGO_LOG(INFO) << "init mds meta.";

  const auto& server_option = app_option_.server();

  self_mds_meta_.SetID(server_option.id());

  self_mds_meta_.SetHost(server_option.host());
  self_mds_meta_.SetPort(server_option.port());
  self_mds_meta_.SetState(MDSMeta::State::kNormal);

  DINGO_LOG(INFO) << fmt::format("init mds meta, self: {}.", self_mds_meta_.ToString());

  mds_meta_map_ = MDSMetaMap::New();
  CHECK(mds_meta_map_ != nullptr) << "new MDSMetaMap fail.";

  mds_meta_map_->UpsertMDSMeta(self_mds_meta_);

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

  auto fs_id_generator = NewFsIdGenerator(coordinator_client_, kv_storage_);
  CHECK(fs_id_generator != nullptr) << "new fs AutoIncrementIdGenerator fail.";
  CHECK(fs_id_generator->Init()) << "init fs AutoIncrementIdGenerator fail.";

  auto slice_id_generator = NewSliceIdGenerator(coordinator_client_, kv_storage_);
  CHECK(slice_id_generator != nullptr) << "new slice AutoIncrementIdGenerator fail.";
  CHECK(slice_id_generator->Init()) << "init slice AutoIncrementIdGenerator fail.";

  quota_worker_set_ =
      ExecqWorkerSet::NewUnique(kQuotaWorkerSetName, FLAGS_quota_worker_num, FLAGS_quota_worker_max_pending_num);
  CHECK(quota_worker_set_ != nullptr) << "new quota worker set fail.";
  CHECK(quota_worker_set_->Init()) << "init service read worker set fail.";

  file_system_set_ =
      FileSystemSet::New(coordinator_client_, std::move(fs_id_generator), slice_id_generator, kv_storage_,
                         self_mds_meta_, mds_meta_map_, operation_processor_, quota_worker_set_, notify_buddy_);
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

bool Server::InitNotifyBuddy() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  notify_buddy_ = notify::NotifyBuddy::New(mds_meta_map_, self_mds_meta_.ID());

  return notify_buddy_->Init();
}

bool Server::InitMonitor() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is nullptr.";
  CHECK(self_mds_meta_.ID() > 0) << "mds id is invalid.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(notify_buddy_ != nullptr) << "notify_buddy is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_mdsmonitor_lock_name, self_mds_meta_.ID());
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
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";
  CHECK(file_system_set_ != nullptr) << "file system set is nullptr.";

  auto dist_lock = StoreDistributionLock::New(kv_storage_, FLAGS_gc_lock_name, self_mds_meta_.ID());
  CHECK(dist_lock != nullptr) << "gc dist lock is nullptr.";

  gc_processor_ = GcProcessor::New(file_system_set_, operation_processor_, dist_lock);

  CHECK(gc_processor_->Init()) << "init GcProcessor fail.";

  return true;
}

bool Server::InitCrontab() {
  DINGO_LOG(INFO) << "init crontab.";

  const auto& crontab_option = app_option_.crontab();

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      crontab_option.heartbeat_interval_s() * 1000,
      true,
      [](void*) { Server::GetInstance().GetHeartbeat()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "FSINFO_SYNC",
      crontab_option.fsinfosync_interval_s() * 1000,
      true,
      [](void*) { FsInfoSync::TriggerFsInfoSync(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "MDS_MONITOR",
      crontab_option.mdsmonitor_interval_s() * 1000,
      true,
      [](void*) { Server::GetInstance().GetMonitor()->Run(); },
  });

  // Add quota sync crontab
  crontab_configs_.push_back({
      "QUOTA_SYNC",
      crontab_option.quota_sync_interval_s() * 1000,
      true,
      [](void*) { Server::GetInstance().GetQuotaSynchronizer()->Run(); },
  });

  // Add fs info sync crontab
  crontab_configs_.push_back({
      "GC",
      crontab_option.gc_interval_s() * 1000,
      true,
      [](void*) { Server::GetInstance().GetGcProcessor()->Run(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

bool Server::InitService() {
  CHECK(operation_processor_ != nullptr) << "operation_processor is nullptr.";

  // mds service
  auto fs_stats = FsStats::New(operation_processor_);
  CHECK(fs_stats != nullptr) << "fsstats is nullptr.";

  const auto& mds_option = app_option_.server().service().meta();
  mds_service_ = MDSServiceImpl::New(mds_option, file_system_set_, gc_processor_, std::move(fs_stats));
  CHECK(mds_service_ != nullptr) << "new MDSServiceImpl fail.";

  if (!mds_service_->Init()) {
    DINGO_LOG(ERROR) << "init MDSServiceImpl fail.";
    return false;
  }

  // debug service
  debug_service_ = DebugServiceImpl::New(file_system_set_);
  CHECK(debug_service_ != nullptr) << "new DebugServiceImpl fail.";

  // fs stat service
  fs_stat_service_ = FsStatServiceImpl::New();
  CHECK(fs_stat_service_ != nullptr) << "new FsStatServiceImpl fail.";

  return true;
}

std::string Server::GetPidFilePath() {
  const auto& log_option = app_option_.log();

  return log_option.path() + "/" + FLAGS_pid_file_name;
}

std::string Server::GetListenAddr() {
  const auto& server_option = app_option_.server();

  std::string host = server_option.listen_host().empty() ? server_option.host() : server_option.listen_host();

  return fmt::format("{}:{}", host, server_option.port());
}

MDSMeta& Server::GetMDSMeta() { return self_mds_meta_; }

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

MDSServiceImplUPtr& Server::GetMDSService() {
  CHECK(mds_service_ != nullptr) << "mds_service is nullptr.";
  return mds_service_;
}

DebugServiceImplUPtr& Server::GetDebugService() {
  CHECK(debug_service_ != nullptr) << "debug_service is nullptr.";
  return debug_service_;
}

FsStatServiceImplUPtr& Server::GetFsStatService() {
  CHECK(fs_stat_service_ != nullptr) << "fs_stat_service is nullptr.";
  return fs_stat_service_;
}

void Server::Run() {
  CHECK(brpc_server_.AddService(mds_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  CHECK(brpc_server_.AddService(debug_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0)
      << "add debug service error.";

  CHECK(brpc_server_.AddService(fs_stat_service_.get(), brpc::SERVER_DOESNT_OWN_SERVICE) == 0)
      << "add fsstat service error.";

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

  mds_service_->Destroy();

  brpc_server_.Stop(0);
  brpc_server_.Join();
  operation_processor_->Destroy();
  heartbeat_->Destroy();
  crontab_manager_.Destroy();
  monitor_->Destroy();
}

}  // namespace mdsv2
}  // namespace dingofs
