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

#include "curvefs/src/mdsv2/server.h"

#include <fmt/format.h>
#include <openssl/asn1.h>

#include <cstdint>
#include <fstream>
#include <memory>
#include <string>

#include "brpc/server.h"
#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/logging.h"
#include "curvefs/src/mdsv2/common/version.h"
#include "curvefs/src/mdsv2/service/heartbeat.h"
#include "curvefs/src/mdsv2/service/mds_service.h"
#include "curvefs/src/mdsv2/storage/dingodb_storage.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {

namespace mdsv2 {

DEFINE_int32(heartbeat_interval_s, 10, "heartbeat interval seconds");

DEFINE_uint32(read_worker_num, 128, "read service worker num");
DEFINE_uint64(read_worker_max_pending_num, 1024, "read service worker num");
DEFINE_bool(read_worker_set_use_pthread, false, "read worker set use pthread");

DEFINE_uint32(write_worker_num, 128, "write service worker num");
DEFINE_uint64(write_worker_max_pending_num, 1024, "write service worker num");
DEFINE_bool(write_worker_set_use_pthread, false, "write worker set use pthread");

const std::string kReadWorkerSetName = "READ_WORKER_SET";
const std::string kWriteWorkerSetName = "WRITE_WORKER_SET";

static const int64_t kFsTableId = 1000;
static const int64_t kFsIdBatchSize = 8;
static const int64_t kFsIdStartId = 20000;

Server::~Server() { delete kv_storage_; }

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

  return true;
}

static bool IsValidFileAddr(const std::string& coor_url) { return coor_url.substr(0, 7) == "file://"; }
static bool IsValidListAddr(const std::string& coor_url) { return coor_url.substr(0, 7) == "list://"; }

static std::string ParseFileUrl(const std::string& coor_url) {
  CHECK(coor_url.substr(0, 7) == "file://") << "Invalid coor_url: " << coor_url;

  std::string file_path = coor_url.substr(7);

  std::ifstream file(file_path);
  if (!file.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("Open file({}) failed, maybe not exist!", file_path);
    return {};
  }

  std::string addrs;
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    if (line.find('#') == 0) {
      continue;
    }

    addrs += line + ",";
  }

  return addrs.empty() ? "" : addrs.substr(0, addrs.size() - 1);
}

static std::string ParseListUrl(const std::string& coor_url) {
  CHECK(coor_url.substr(0, 7) == "list://") << "Invalid coor_url: " << coor_url;

  return coor_url.substr(7);
}

std::string ParseCoorAddr(const std::string& coor_url) {
  std::string coor_addrs;
  if (IsValidFileAddr(coor_url)) {
    coor_addrs = ParseFileUrl(coor_url);

  } else if (IsValidListAddr(coor_url)) {
    coor_addrs = ParseListUrl(coor_url);

  } else {
    DINGO_LOG(ERROR) << "Invalid coor_url: " << coor_url;
  }

  return coor_addrs;
}

bool Server::InitCoordinatorClient(const std::string& coor_url) {
  DINGO_LOG(INFO) << fmt::format("init coordinator client, url({}).", coor_url);

  std::string coor_addrs = ParseCoorAddr(coor_url);
  if (coor_addrs.empty()) {
    return false;
  }

  return coordinator_client_.Init(coor_addrs);
}

bool Server::InitFsIdGenerator() {
  fs_id_generator_ = AutoIncrementIdGenerator::New(coordinator_client_, kFsTableId, kFsIdStartId, kFsIdBatchSize);
  CHECK(fs_id_generator_ != nullptr) << "new AutoIncrementIdGenerator fail.";
  return fs_id_generator_->Init();
}

bool Server::InitStorage(const std::string& store_url) {
  DINGO_LOG(INFO) << fmt::format("init storage, url({}).", store_url);

  kv_storage_ = new DingodbStorage();
  CHECK(kv_storage_ != nullptr) << "new DingodbStorage fail.";

  std::string store_addrs = ParseCoorAddr(store_url);
  if (store_addrs.empty()) {
    return false;
  }

  return kv_storage_->Init(store_addrs);
}

bool Server::InitFileSystem() {
  DINGO_LOG(INFO) << "init filesystem.";
  file_system_ = FileSystem::New(kv_storage_);
  CHECK(file_system_ != nullptr) << "new FileSystem fail.";

  return file_system_->Init();
}

bool Server::InitHeartbeat() {
  DINGO_LOG(INFO) << "init heartbeat.";
  return heartbeat_.Init();
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

bool Server::InitCrontab() {
  DINGO_LOG(INFO) << "init crontab.";

  // Add heartbeat crontab
  crontab_configs_.push_back({
      "HEARTBEA",
      FLAGS_heartbeat_interval_s * 1000,
      true,
      [](void*) { Heartbeat::TriggerHeartbeat(); },
  });

  crontab_manager_.AddCrontab(crontab_configs_);

  return true;
}

std::string Server::GetListenAddr() {
  std::string addr;
  conf_.GetValueFatalIfFail("listen.addr", &addr);
  return addr;
}

MDSMeta& Server::GetMDSMeta() { return mds_meta_; }

void Server::Run() {
  brpc::Server brpc_server;

  MDSServiceImpl mds_service(read_worker_set_, write_worker_set_, file_system_, fs_id_generator_);

  CHECK(brpc_server.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  brpc::ServerOptions option;
  CHECK(brpc_server.Start(GetListenAddr().c_str(), &option) == 0) << "start brpc server error.";

  brpc_server.RunUntilAskedToQuit();

  brpc_server.Stop(0);
  brpc_server.Join();
}

void Server::Stop() {
  heartbeat_.Destroy();
  crontab_manager_.Destroy();
}

}  // namespace mdsv2

}  // namespace dingofs
