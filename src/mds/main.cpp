/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <csignal>

#include "aws/s3_access_log.h"
#include "common/dynamic_vlog.h"
#include "mds/mds.h"
#include "stub/common/version.h"
#include "utils/configuration.h"

using ::dingofs::common::FLAGS_vlog_level;
using ::dingofs::utils::Configuration;

DEFINE_string(confPath, "dingofs/conf/mds.conf", "mds confPath");
DEFINE_string(mdsAddr, "127.0.0.1:6700", "mds listen addr");

void LoadConfigFromCmdline(Configuration* conf) {
  google::CommandLineFlagInfo info;
  if (GetCommandLineFlagInfo("mdsAddr", &info) && !info.is_default) {
    conf->SetStringValue("mds.listen.addr", FLAGS_mdsAddr);
  }

  if (GetCommandLineFlagInfo("v", &info) && !info.is_default) {
    conf->SetIntValue("mds.loglevel", FLAGS_v);
  }
}

static void InstallSigHandler() { CHECK(SIG_ERR != signal(SIGPIPE, SIG_IGN)); }

int main(int argc, char** argv) {
  // config initialization
  google::ParseCommandLineFlags(&argc, &argv, false);

  if (dingofs::stub::common::FLAGS_show_version && argc == 1) {
    dingofs::stub::common::ShowVerion();
  }

  std::string confPath = FLAGS_confPath;
  auto conf = std::make_shared<Configuration>();
  conf->SetConfigPath(confPath);
  LOG_IF(FATAL, !conf->LoadConfig())
      << "load mds configuration fail, conf path = " << confPath;
  conf->GetValueFatalIfFail("mds.loglevel", &FLAGS_v);
  LoadConfigFromCmdline(conf.get());
  FLAGS_vlog_level = FLAGS_v;
  if (FLAGS_log_dir.empty()) {
    if (!conf->GetStringValue("mds.common.logDir", &FLAGS_log_dir)) {
      LOG(WARNING) << "no mds.common.logDir in " << confPath
                   << ", will log to /tmp";
    }
  }

  InstallSigHandler();

  // initialize logging module
  google::InitGoogleLogging(argv[0]);

  dingofs::stub::common::LogVerion();

  dingofs::aws::InitS3AccessLog(FLAGS_log_dir);

  conf->PrintConfig();

  dingofs::mds::MDS mds;

  // initialize MDS options
  mds.InitOptions(conf);

  mds.StartDummyServer();

  mds.StartCompaginLeader();

  // Initialize other modules after winning election
  mds.Init();

  // start mds server and wait CTRL+C to quit
  mds.Run();

  // stop server and background threads
  mds.Stop();

  dingofs::aws::S3Adapter::Shutdown();
  google::ShutdownGoogleLogging();
  return 0;
}
