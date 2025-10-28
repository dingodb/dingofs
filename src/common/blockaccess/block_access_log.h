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

#ifndef DINGOFS_COMMON_BLOCK_ACCESS_ACCESS_LOG_H_
#define DINGOFS_COMMON_BLOCK_ACCESS_ACCESS_LOG_H_

#include <butil/time.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace blockaccess {

extern std::shared_ptr<spdlog::logger> block_access_logger;
extern bool initialized;

bool InitBlockAccessLog(const std::string& prefix);

struct BlockAccessLogGuard {
  using MessageHandler = std::function<std::string()>;

  explicit BlockAccessLogGuard(int64_t p_start_us, MessageHandler handler)
      : start_us(p_start_us), handler(handler) {}

  ~BlockAccessLogGuard() {
    if (initialized) {
      block_access_logger->info("{0} <{1:.6f}>", handler(),
                                (butil::cpuwide_time_us() - start_us) / 1e6);
    }
  }

  MessageHandler handler;
  int64_t start_us = 0;
};

}  // namespace blockaccess
}  // namespace dingofs

#endif  // DINGOFS_COMMON_BLOCK_ACCESS_ACCESS_LOG_H_