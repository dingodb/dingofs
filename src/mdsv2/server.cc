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
#include <gflags/gflags.h>
#include <openssl/asn1.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>
#include <utility>

#include "brpc/server.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/version.h"
#include "mdsv2/coordinator/dingo_coordinator_client.h"
#include "mdsv2/service/debug_service.h"
#include "mdsv2/service/fsinfo_sync.h"
#include "mdsv2/service/heartbeat.h"
#include "mdsv2/service/mds_service.h"
#include "mdsv2/storage/dingodb_storage.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_int32(heartbeat_interval_s, 10, "heartbeat interval seconds");
DEFINE_int32(fsinfosync_interval_s, 10, "fs info sync interval seconds");
DEFINE_int32(mdsmonitor_interval_s, 10, "mds monitor interval seconds");
DEFINE_string(mdsmonitor_lock_name, "/lock/mds/monitor", "mds monitor lock name");

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
  mds_meta_.SetState(MDSMeta::State::kInit);

  DINGO_LOG(INFO) << fmt::format("MDS meta {}.", mds_meta_.ToString());

  mds_meta_map_ = MDSMetaMap::New();
  CHECK(mds_meta_map_ != nullptr) << "new MDSMetaMap fail.";

  mds_meta_map_->UpsertMDSMeta(mds_meta_);

  return true;
}

bool Server::InitCoordinatorClient(const std::string& coor_url) {
  DINGO_LOG(INFO) << fmt::format("init coordinator client, url({}).", coor_url);

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

bool Server::InitRenamer() {
  renamer_ = Renamer::New();
  CHECK(renamer_ != nullptr) << "new Renamer fail.";

  return renamer_->Init();
}

bool Server::InitMutationMerger() {
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";

  mutation_processor_ = MutationProcessor::New(kv_storage_);
  CHECK(mutation_processor_ != nullptr) << "new MutationProcessor fail.";

  return mutation_processor_->Init();
}

bool Server::InitFileSystem() {
  DINGO_LOG(INFO) << "init filesystem.";

  CHECK(coordinator_client_ != nullptr) << "coordinator client is nullptr.";
  CHECK(kv_storage_ != nullptr) << "kv storage is nullptr.";
  CHECK(mds_meta_map_ != nullptr) << "mds_meta_map is nullptr.";

  auto fs_id_generator = AutoIncrementIdGenerator::New(coordinator_client_, kFsTableId, kFsIdStartId, kFsIdBatchSize);
  CHECK(fs_id_generator != nullptr) << "new AutoIncrementIdGenerator fail.";
  CHECK(fs_id_generator->Init()) << "init AutoIncrementIdGenerator fail.";

  file_system_set_ = FileSystemSet::New(coordinator_client_, std::move(fs_id_generator), kv_storage_, mds_meta_,
                                        mds_meta_map_, renamer_, mutation_processor_);
  CHECK(file_system_set_ != nullptr) << "new FileSystem fail.";

  return file_system_set_->Init();
}

bool Server::InitHeartbeat() {
  DINGO_LOG(INFO) << "init heartbeat.";
  return heartbeat_.Init();
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

bool Server::InitMDSMonitor() {
  CHECK(coordinator_client_ != nullptr) << "coordinator client is nullptr.";
  CHECK(mds_meta_.ID() > 0) << "mds id is invalid.";

  auto dist_lock =
      std::make_unique<CoorDistributionLock>(coordinator_client_, FLAGS_mdsmonitor_lock_name, mds_meta_.ID());

  mds_monitor_ = MDSMonitor::New(std::move(dist_lock));
  CHECK(mds_monitor_ != nullptr) << "new MDSMonitor fail.";

  return true;
}

bool Server::InitCrontab() {
  DINGO_LOG(INFO) << "init crontab.";

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      FLAGS_heartbeat_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerHeartbeat(); },
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
      [](void*) { Server::GetInstance().GetMDSMonitor()->MonitorMDS(); },
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

MDSMetaMapPtr Server::GetMDSMetaMap() {
  CHECK(mds_meta_map_ != nullptr) << "mds meta map is nullptr.";
  return mds_meta_map_;
}

void Server::Run() {
  brpc::Server brpc_server;

  MDSServiceImpl mds_service(read_worker_set_, write_worker_set_, file_system_set_);
  CHECK(brpc_server.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  DebugServiceImpl debug_service(file_system_set_);
  CHECK(brpc_server.AddService(&debug_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add debug service error.";

  brpc::ServerOptions option;
  CHECK(brpc_server.Start(GetListenAddr().c_str(), &option) == 0) << "start brpc server error.";

  brpc_server.RunUntilAskedToQuit();
  LOG(INFO) << "stop 0000";

  brpc_server.Stop(0);

  brpc_server.Join();
}

void Server::Stop() {
  LOG(INFO) << "stop 0001";
  renamer_->Destroy();

  LOG(INFO) << "stop 0002";
  mutation_processor_->Destroy();

  LOG(INFO) << "stop 0003";
  heartbeat_.Destroy();

  LOG(INFO) << "stop 0004";
  crontab_manager_.Destroy();

  LOG(INFO) << "stop 0005";
  read_worker_set_->Destroy();

  LOG(INFO) << "stop 0006";
  write_worker_set_->Destroy();
  LOG(INFO) << "stop 0007";
}

}  // namespace mdsv2

}  // namespace dingofs
