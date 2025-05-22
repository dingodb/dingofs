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

#include "aws/s3_access_log.h"

#include <absl/strings/str_format.h>

namespace dingofs {
namespace aws {

std::shared_ptr<spdlog::logger> s3_logger;

bool InitS3AccessLog(const std::string& prefix) {
  std::string filename =
      absl::StrFormat("%s/s3_access_%d.log", prefix, getpid());
  s3_logger = spdlog::daily_logger_mt("s3_access", filename, 0, 0);
  spdlog::flush_every(std::chrono::seconds(1));
  return true;
}

}  // namespace aws
}  // namespace dingofs