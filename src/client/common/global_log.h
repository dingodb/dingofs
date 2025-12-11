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

#ifndef DINGOFS_SRC_CLIENT_COMMON_GLOBAL_LOG_H_
#define DINGOFS_SRC_CLIENT_COMMON_GLOBAL_LOG_H_

#include "common/options/client.h"
#include "glog/logging.h"

static int InitLog(const char* argv0) {
  // set log dir
  FLAGS_log_dir = dingofs::client::FLAGS_client_log_dir;
  FLAGS_v = dingofs::client::FLAGS_client_log_level;

  FLAGS_logbufsecs = 4;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;

  // initialize logging module
  google::InitGoogleLogging(argv0);
  LOG(INFO) << "current verbose logging level is: " << FLAGS_v
            << ", log dir is: " << FLAGS_log_dir;

  return 0;
}

#endif  // DINGOFS_SRC_CLIENT_COMMON_GLOBAL_LOG_H_
