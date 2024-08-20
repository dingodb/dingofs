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
 * Created Date: 2024-08-21
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_CLIENT_BCACHE_PERF_LOG_H_
#define CURVEFS_SRC_CLIENT_BCACHE_PERF_LOG_H_

#include <unistd.h>
#include <butil/time.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/daily_file_sink.h>

#include <string>
#include <memory>

#include "absl/strings/str_format.h"
#include "curvefs/src/client/common/config.h"

namespace curvefs {
namespace client {
namespace common {

DECLARE_bool(block_cache_logging);

}
namespace bcache {

using ::absl::StrFormat;
using ::curvefs::client::common::FLAGS_block_cache_logging;
using MessageHandler = std::function<std::string()>;

static std::shared_ptr<spdlog::logger> Logger;

bool InitBcacheLog(const std::string& prefix) {
    std::string filename = StrFormat("%s/block_cache_%d.log", prefix, getpid());
    Logger = spdlog::daily_logger_mt("perf", filename, 0, 0);
    spdlog::flush_every(std::chrono::seconds(1));
    return true;
}

struct PerfLogGuard {
    explicit PerfLogGuard(MessageHandler handler)
        : enable(FLAGS_block_cache_logging),
          handler(handler) {
        if (!enable) {
            return;
        }

        timer.start();
    }

    ~PerfLogGuard() {
        if (!enable) {
            return;
        }

        timer.stop();
        Logger->info("{0} <{1:.6f}>", handler(), timer.u_elapsed() / 1e6);
    }

    bool enable;
    MessageHandler handler;
    butil::Timer timer;
};

}  // namespace bcache
}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_BCACHE_PERF_LOG_H_
