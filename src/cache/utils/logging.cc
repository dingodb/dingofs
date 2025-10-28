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

/*
 * Project: DingoFS
 * Created Date: 2025-06-21
 * Author: Jingli Chen (Wine93)
 */

#include "cache/utils/logging.h"

#include <absl/strings/str_format.h>
#include <brpc/reloadable_flags.h>
#include <glog/logging.h>

#include "common/blockaccess/block_access_log.h"

namespace dingofs {
namespace cache {

DEFINE_string(logdir, "/tmp",
              "Specified logging directory for glog and access log");

DEFINE_int32(loglevel, 0, "Sets glog logging level: 0, 1, 2 and etc");

DEFINE_bool(cache_trace_logging, true,
            "Whether to enable trace logging for cache");
DEFINE_validator(cache_trace_logging, brpc::PassValidate);

using MessageHandler = std::function<std::string()>;

static std::shared_ptr<spdlog::logger> logger;
static bool initialized = false;

void InitLogging(const char* argv0) {
  FLAGS_log_dir = FLAGS_logdir;
  FLAGS_v = FLAGS_loglevel;
  FLAGS_logbufsecs = 0;
  FLAGS_max_log_size = 80;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;
  google::InitGoogleLogging(argv0);
  LOG(INFO) << "Init glog logger success: log_dir = " << FLAGS_log_dir;

  CHECK(InitCacheTraceLog(FLAGS_log_dir)) << "Init cache access log failed.";
  LOG(INFO) << "Init cache access logger success: log_dir = " << FLAGS_log_dir;

  CHECK(blockaccess::InitBlockAccessLog(FLAGS_log_dir))
      << "Init block access log failed.";

  LOG(INFO) << "Init block access logger success: log_dir = " << FLAGS_log_dir;
}

bool InitCacheTraceLog(const std::string& log_dir) {
  if (!initialized) {
    std::string filename =
        absl::StrFormat("%s/cache_trace_%d.log", log_dir, getpid());
    logger = spdlog::daily_logger_mt("cache", filename, 0, 0);
    logger->set_level(spdlog::level::trace);
    spdlog::flush_every(std::chrono::seconds(1));
    initialized = true;
  }
  return initialized;
}

void ShutdownCacheTraceLog() {
  if (initialized) {
    spdlog::shutdown();
    initialized = false;
  }
}

void LogTrace(const std::string& message) {
  if (initialized && FLAGS_cache_trace_logging) {
    logger->trace(message);
  }
}

}  // namespace cache
}  // namespace dingofs
