/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "options/common/dynamic_vlog.h"
#include "utils/configuration.h"
#include "utils/gflags_helper.h"

static int InitLog(const char* argv0, std::string conf_path) {
  dingofs::utils::Configuration conf;
  conf.SetConfigPath(conf_path);
  if (!conf.LoadConfig()) {
    LOG(ERROR) << "LoadConfig failed, confPath = " << conf_path;
    return 1;
  }

  // set log dir
  if (FLAGS_log_dir.empty()) {
    if (!conf.GetStringValue("client.common.logDir", &FLAGS_log_dir)) {
      LOG(WARNING) << "no client.common.logDir in " << conf_path
                   << ", will log to /tmp";
    }
  }

  dingofs::utils::GflagsLoadValueFromConfIfCmdNotSet dummy;
  dummy.Load(&conf, "v", "client.loglevel", &FLAGS_v);
  dingofs::common::FLAGS_vlog_level = FLAGS_v;

  FLAGS_logbufsecs = 0;
  // initialize logging module
  google::InitGoogleLogging(argv0);

  return 0;
}