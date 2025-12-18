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

#ifndef DINGOFS_CLIENT_VFS_META_LOG_H_
#define DINGOFS_CLIENT_VFS_META_LOG_H_

#include <unistd.h>

#include <cstdint>

#include "butil/time.h"
#include "common/options/client.h"
#include "spdlog/logger.h"

namespace dingofs {
namespace client {
namespace vfs {

extern std::shared_ptr<spdlog::logger> meta_logger;

bool InitMetaLog(const std::string& log_dir);

class MetaLogGuard {
 public:
  using MessageHandler = std::function<std::string()>;

  explicit MetaLogGuard(uint64_t start_us, MessageHandler handler)
      : enabled_(FLAGS_vfs_meta_access_logging),
        start_us(start_us),
        handler_(std::move(handler)) {
    if (!enabled_) {
      return;
    }
  }

  explicit MetaLogGuard(MessageHandler handler)
      : MetaLogGuard(butil::cpuwide_time_us(), std::move(handler)) {}

  ~MetaLogGuard() {
    if (!enabled_) {
      return;
    }

    int64_t duration = butil::cpuwide_time_us() - start_us;
    if (duration > FLAGS_vfs_meta_access_log_threshold_us) {
      meta_logger->warn("{0} <{1:.6f}>", handler_(), duration / 1e6);
    }
  }

 private:
  bool enabled_;
  int64_t start_us = 0;
  MessageHandler handler_;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_META_LOG_H_