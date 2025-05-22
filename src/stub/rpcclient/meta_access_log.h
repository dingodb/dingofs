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

#ifndef DINGODB_SRC_STUB_RPCCLIENT_META_ACCESS_LOG_H_
#define DINGODB_SRC_STUB_RPCCLIENT_META_ACCESS_LOG_H_

#include <butil/time.h>
#include <gflags/gflags.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <cstdint>
#include <string>

#include "common/dynamic_config.h"

namespace dingofs {
namespace stub {

extern std::shared_ptr<spdlog::logger> meta_access_logger;

bool InitMetaAccessLog(const std::string& prefix);

struct MetaAccessLogGuard {
  using MessageHandler = std::function<std::string()>;

  explicit MetaAccessLogGuard(int64_t p_start_us, MessageHandler handler)
      : start_us(p_start_us), handler(handler) {}

  ~MetaAccessLogGuard() {
    if (!dingofs::common::FLAGS_meta_access_logging) {
      return;
    }

    int64_t duration_us = butil::cpuwide_time_us() - start_us;
    if (duration_us > dingofs::common::FLAGS_meta_access_log_threshold_us) {
      meta_access_logger->info("{0} <{1:.6f}>", handler(), (duration_us) / 1e6);
    }
  }

  MessageHandler handler;
  int64_t start_us = 0;
};

}  // namespace stub
}  // namespace dingofs

#endif  // DINGODB_SRC_STUB_RPCCLIENT_META_ACCESS_LOG_H_
