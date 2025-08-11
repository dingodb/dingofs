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

#include "common/logging.h"

#include <absl/strings/str_format.h>
#include <brpc/reloadable_flags.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>

namespace dingofs {

static std::shared_ptr<spdlog::logger> logger;
static bool initialized = false;

bool InitTraceLog(const std::string& log_dir) {
  if (!initialized) {
    std::string filename =
        absl::StrFormat("%s/fuse_trace_%d.log", log_dir, getpid());
    logger = spdlog::daily_logger_mt("fuse", filename, 0, 0);
    logger->set_level(spdlog::level::trace);
    spdlog::flush_every(std::chrono::seconds(1));
    initialized = true;
  }
  return initialized;
}

void ShutdownTraceLog() {
  if (initialized) {
    spdlog::shutdown();
    initialized = false;
  }
}

void LogTrace(const std::string& message) {
  if (initialized) {
    logger->trace(message);
  }
}

}  // namespace dingofs
