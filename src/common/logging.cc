// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "common/logging.h"

#include <cstdint>

#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {

void Logger::InitLogger(const std::string& log_dir, const std::string& role,
                        const LogLevel& level) {
  FLAGS_logbufsecs = 10;
  FLAGS_max_log_size = 256;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  ChangeGlogLevelUsingDingoLevel(level, DINGO_DEBUG);

  const std::string program_name = fmt::format("./{}", role);
  google::InitGoogleLogging(program_name.c_str());
  // google::InitGoogleLogging(program_name.c_str(), &CustomLogFormatPrefix);
  google::SetLogDestination(
      google::GLOG_INFO, fmt::format("{}/{}.info.log.", log_dir, role).c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      fmt::format("{}/{}.warn.log.", log_dir, role).c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      fmt::format("{}/{}.error.log.", log_dir, role).c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      fmt::format("{}/{}.fatal.log.", log_dir, role).c_str());
}

void Logger::SetMinLogLevel(int level) { FLAGS_minloglevel = level; }

int Logger::GetMinLogLevel() { return FLAGS_minloglevel; }

void Logger::SetMinVerboseLevel(int v) { FLAGS_v = v; }

int Logger::GetMinVerboseLevel() { return FLAGS_v; }

int Logger::GetLogBuffSecs() { return FLAGS_logbufsecs; }

void Logger::SetLogBuffSecs(uint32_t secs) { FLAGS_logbufsecs = secs; }

int32_t Logger::GetMaxLogSize() { return FLAGS_max_log_size; }
void Logger::SetMaxLogSize(uint32_t max_log_size) {
  FLAGS_max_log_size = max_log_size;
}

bool Logger::GetStoppingWhenDiskFull() {
  return FLAGS_stop_logging_if_full_disk;
}

void Logger::SetStoppingWhenDiskFull(bool is_stop) {
  FLAGS_stop_logging_if_full_disk = is_stop;
}

void Logger::ChangeGlogLevelUsingDingoLevel(const LogLevel& log_level,
                                            uint32_t verbose) {
  if (log_level == kDEBUG) {
    Logger::SetMinLogLevel(0);
    Logger::SetMinVerboseLevel(verbose == 0 ? DINGO_DEBUG : verbose);
  } else {
    Logger::SetMinLogLevel(static_cast<int>(log_level) - 1);
    Logger::SetMinVerboseLevel(1);
  }
}

}  // namespace dingofs
