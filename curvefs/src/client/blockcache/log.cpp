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
 * Created Date: 2024-08-28
 * Author: Jingli Chen (Wine93)
 */

#include "curvefs/src/client/blockcache/log.h"

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/spdlog.h>
#include <unistd.h>

#include "curvefs/src/client/blockcache/cache_store.h"
#include "curvefs/src/client/common/dynamic_config.h"

namespace curvefs {
namespace client {
namespace blockcache {

USING_FLAG(block_cache_logging);

using ::curvefs::base::string::StrFormat;
using ::curvefs::client::common::FLAGS_block_cache_logging;
using MessageHandler = std::function<std::string()>;

static std::shared_ptr<spdlog::logger> logger;

bool InitBlockCacheLog(const std::string& prefix) {
  std::string filename = StrFormat("%s/block_cache_%d.log", prefix, getpid());
  logger = spdlog::daily_logger_mt("trace", filename, 0, 0);
  spdlog::flush_every(std::chrono::seconds(1));
  return true;
}

LogGuard::LogGuard(MessageHandler handler)
    : enable_(FLAGS_block_cache_logging), handler_(handler) {
  if (!enable_) {
    return;
  }

  timer_.start();
}

LogGuard::~LogGuard() {
  if (!enable_) {
    return;
  }

  timer_.stop();
  logger->info("{0} <{1:.6f}>", handler_(), timer_.u_elapsed() / 1e6);
}

}  // namespace blockcache
}  // namespace client
}  // namespace curvefs
