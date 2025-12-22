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

enum LogLevel : uint8_t {
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
#define LOG_INFO LOG(INFO)
#define LOG_WARNING LOG(WARNING)
#define LOG_ERROR LOG(ERROR)
#define LOG_FATAL LOG(FATAL)

#define LOG_IF_DEBUG(condition) VLOG_IF(DINGO_DEBUG, condition)
#define LOG_IF_INFO(condition) LOG_IF(INFO, condition)
#define LOG_IF_WARNING(condition) LOG_IF(WARNING, condition)
#define LOG_IF_ERROR(condition) LOG_IF(ERROR, condition)
#define LOG_IF_FATAL(condition) LOG_IF(FATAL, condition)

class Logger {
 public:
  static void InitLogger(const std::string& log_dir, const std::string& role,
                         const LogLevel& level);
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
  static void ChangeGlogLevelUsingDingoLevel(const LogLevel& level,
                                             uint32_t verbose);
  // static void CustomLogFormatPrefix(std::ostream& s, const
  // google::LogMessageInfo& l, void*);
};

}  // namespace dingofs

#endif  // DINGOFS_COMMON_LOGGING_H_
