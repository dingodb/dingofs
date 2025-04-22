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

#include "common/dynamic_vlog.h"
#include "options/client/app.h"
#include "utils/configuration.h"
#include "utils/gflags_helper.h"

static int InitLog(const char* argv0, std::string conf_path) {
  dingofs::options::client::AppOption option;

  if (!option.Parse(conf_path)) {
    LOG(ERROR) << "Parse config file failed, confpath = " << conf_path;
    return 1;
  }

  const auto& global_option = option.global_option();

  // initialize logging module
  FLAGS_log_dir = global_option.log_dir();
  FLAGS_v = global_option.vlog_level();
  FLAGS_logbufsecs = 0;
  google::InitGoogleLogging(argv0);

  return 0;
}