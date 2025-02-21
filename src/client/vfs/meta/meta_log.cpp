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

#include "client/vfs/meta/meta_log.h"

namespace dingofs {
namespace client {
namespace vfs {

std::shared_ptr<spdlog::logger> meta_logger;

bool InitMetaLog(const std::string& log_dir) {
  std::string filename = fmt::format("{}/meta_{}.log", log_dir, getpid());
  meta_logger = spdlog::daily_logger_mt("vfs_meta_system", filename, 0, 0);
  spdlog::flush_every(std::chrono::seconds(1));
  return true;
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs