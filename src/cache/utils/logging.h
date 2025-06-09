/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2025-06-11
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_LOGGER_H_
#define DINGOFS_SRC_CACHE_UTILS_LOGGER_H_

#include <glog/logging.h>

#include "blockaccess/block_access_log.h"
#include "cache/common/common.h"
#include "cache/config/config.h"
#include "cache/utils/access_log.h"

namespace dingofs {
namespace cache {

inline void InitLogging(const char* argv0) {
  FLAGS_log_dir = FLAGS_logdir;
  FLAGS_v = FLAGS_loglevel;
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 80;
  FLAGS_stop_logging_if_full_disk = true;
  google::InitGoogleLogging(argv0);
  LOG(INFO) << "Init glog logger success: log_dir = " << FLAGS_log_dir;

  CHECK(InitCacheAccessLog(FLAGS_log_dir)) << "Init access log failed.";
  LOG(INFO) << "Init cache access logger success: log_dir = " << FLAGS_log_dir;

  CHECK(blockaccess::InitBlockAccessLog(FLAGS_log_dir))
      << "Init block access log failed.";
  LOG(INFO) << "Init block access logger success: log_dir = " << FLAGS_log_dir;
}

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_LOGGER_H_
