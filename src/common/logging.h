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

#ifndef DINGOFS_COMMON_LOGGING_H_
#define DINGOFS_COMMON_LOGGING_H_

#include <cstdint>
#include <string>

#include "glog/logging.h"

namespace dingofs {

DECLARE_string(log_dir);
DECLARE_string(log_level);
DECLARE_int32(log_v);

enum class LogLevel : uint8_t {
  kDEBUG = 0,
  kINFO = 1,
  kWARNING = 2,
  kERROR = 3,
  kFATAL = 4
};

// define the debug log level.
// The larger the number, the more comprehensive information is displayed.
#define DINGO_DEBUG 79

#define LOG_DEBUG VLOG(DINGO_DEBUG)

#define LOG_IF_DEBUG(condition) VLOG_IF(DINGO_DEBUG, condition)

class Logger {
 public:
  static void Init(const std::string& role);

  static std::string LogDir();
  static std::string LogLevel();

  // LogLevel and Log Verbose
  static void SetMinLogLevel(int level);
  static int GetMinLogLevel();

  static void SetMinVerboseLevel(int v);
  static int GetMinVerboseLevel();

  static int GetLogBuffSecs();
  static void SetLogBuffSecs(uint32_t secs);

  static int32_t GetMaxLogSize();
  static void SetMaxLogSize(uint32_t max_log_size);

  static bool GetStoppingWhenDiskFull();
  static void SetStoppingWhenDiskFull(bool is_stop);

  static void ChangeGlogLevel(const std::string& level);
  static void ChangeGlogLevel(enum LogLevel level, uint32_t verbose);
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_LOGGING_H_
