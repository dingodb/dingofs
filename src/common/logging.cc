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

#include "common/directory.h"
#include "common/helper.h"
#include "fmt/core.h"
#include "glog/logging.h"

namespace dingofs {

DEFINE_string(log_level, "INFO", "log level");
DEFINE_validator(log_level, [](const char* /*name*/, const std::string& value) {
  Logger::ChangeGlogLevel(value);
  return true;
});

DEFINE_int32(log_v, 0, "log level");
DEFINE_validator(log_v, [](const char* /*name*/, int32_t value) {
  FLAGS_v = value;
  return true;
});

static uint32_t kLogBufSecs = 10;
static uint32_t kMaxLogSize = 256;  // in MB

static bool IsEqualIgnoreCase(const std::string& str1,
                              const std::string& str2) {
  if (str1.size() != str2.size()) {
    return false;
  }
  return std::equal(str1.begin(), str1.end(), str2.begin(),
                    [](const char c1, const char c2) {
                      return std::tolower(c1) == std::tolower(c2);
                    });
}

static LogLevel ToLogLevel(const std::string& log_level) {
  if (IsEqualIgnoreCase(log_level, "DEBUG")) {
    return LogLevel::kDEBUG;
  } else if (IsEqualIgnoreCase(log_level, "INFO")) {
    return LogLevel::kINFO;
  } else if (IsEqualIgnoreCase(log_level, "WARNING")) {
    return LogLevel::kWARNING;
  } else if (IsEqualIgnoreCase(log_level, "ERROR")) {
    return LogLevel::kERROR;
  } else if (IsEqualIgnoreCase(log_level, "FATAL")) {
    return LogLevel::kFATAL;
  } else {
    return LogLevel::kINFO;
  }
}

void Logger::Init(const std::string& role) {
  ::FLAGS_log_dir = dingofs::Helper::ExpandPath(
      ::FLAGS_log_dir.empty() ? GetDefaultDir(kLogDir) : ::FLAGS_log_dir);

  const std::string log_dir = ::FLAGS_log_dir;

  CHECK(dingofs::Helper::CreateDirectory(log_dir))
      << fmt::format("create log directory failed, log_dir: {}", log_dir);

  FLAGS_logbufsecs = kLogBufSecs;
  FLAGS_max_log_size = kMaxLogSize;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;

  ChangeGlogLevel(FLAGS_log_level);

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

std::string Logger::LogDir() { return ::FLAGS_log_dir; }
std::string Logger::LogLevel() { return FLAGS_log_level; }

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

void Logger::ChangeGlogLevel(const std::string& level) {
  const enum LogLevel& log_level = ToLogLevel(level);
  if (log_level == LogLevel::kDEBUG) {
    Logger::SetMinLogLevel(0);
  } else {
    Logger::SetMinLogLevel(static_cast<int>(log_level) - 1);
  }
}

void Logger::ChangeGlogLevel(enum LogLevel level, uint32_t verbose) {
  if (level == LogLevel::kDEBUG) {
    Logger::SetMinLogLevel(0);
  } else {
    Logger::SetMinLogLevel(static_cast<int>(level) - 1);
  }

  FLAGS_v = verbose;
}

void Logger::FlushLogs() { google::FlushLogFiles(google::INFO); }

}  // namespace dingofs
