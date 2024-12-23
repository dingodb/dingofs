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

#include <string>

#include "brpc/server.h"
#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/logging.h"
#include "curvefs/src/mdsv2/common/version.h"
#include "curvefs/src/mdsv2/service/heartbeat.h"
#include "curvefs/src/mdsv2/service/mds_service.h"
#include "glog/logging.h"

namespace dingofs {

namespace mdsv2 {

DEFINE_int32(heartbeat_interval_s, 10, "heartbeat interval seconds");

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
  conf_.SetConfigPath(path);
  return conf_.LoadConfig();
}

bool Server::InitLog() {
  std::string log_dir;
  conf_.GetValueFatalIfFail("log.dir", &log_dir);

  std::string log_level;
  conf_.GetValueFatalIfFail("log.level", &log_level);

  DingoLogger::InitLogger(log_dir, "mdsv2", GetDingoLogLevel(log_level));

  DingoLogVerion();
  return true;
}

bool Server::InitCrontab() {
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

void Server::Run() {
  brpc::Server brpc_server;

  MDSServiceImpl mds_service;

  CHECK(brpc_server.AddService(&mds_service, brpc::SERVER_DOESNT_OWN_SERVICE) == 0) << "add mds service error.";

  brpc::ServerOptions option;
  CHECK(brpc_server.Start(GetListenAddr().c_str(), &option) == 0) << "start brpc server error.";

  brpc_server.RunUntilAskedToQuit();

  brpc_server.Stop(0);
  brpc_server.Join();
}

void Server::Stop() {}

}  // namespace mdsv2

}  // namespace dingofs
