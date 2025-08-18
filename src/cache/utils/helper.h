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

/*
 * Project: DingoFS
 * Created Date: 2025-04-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_UTILS_HELPER_H_
#define DINGOFS_SRC_CACHE_UTILS_HELPER_H_

#include <absl/strings/str_format.h>
#include <google/protobuf/message.h>
#include <sys/stat.h>

#include <sstream>
#include <string>

#include "cache/storage/filesystem.h"
#include "common/io_buffer.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

class Helper {
 public:
  // error
  template <typename... Args>
  static std::string Errorf(int code, const char* format, const Args&... args) {
    std::ostringstream message;
    message << absl::StrFormat(format, args...) << ": " << ::strerror(code);
    return message.str();
  }

  template <typename... Args>
  static std::string Errorf(const char* format, const Args&... args) {
    return Errorf(errno, format, args...);
  }

  // string
  static std::string ToLowerCase(const std::string& str);
  static std::string RedString(const std::string& str);

  // sys conf
  static int GetProcessCores();
  static uint64_t GetSysPageSize();
  static uint64_t GetIOAlignedBlockSize();
  static bool IsAligned(uint64_t n, uint64_t m);

  // time
  static int64_t TimestampNs();
  static int64_t TimestampUs();
  static int64_t TimestampMs();
  static int64_t Timestamp();
  static std::string NowTime();
  static std::string FormatMsTime(int64_t timestamp, const std::string& format);

  // filepath
  static std::string ParentDir(const std::string& path);
  static std::string Filename(const std::string& path);
  static bool HasSuffix(const std::string& path, const std::string& suffix);
  static std::string PathJoin(const std::vector<std::string>& subpaths);
  static std::string TempFilepath(const std::string& filepath);
  static bool IsTempFilepath(const std::string& filepath);

  // filesystem
  static Status Walk(const std::string& dir, WalkFunc walk_func);
  static Status MkDirs(const std::string& dir);
  static bool FileExists(const std::string& filepath);
  static Status ReadFile(const std::string& filepath, std::string* content);
  static Status WriteFile(const std::string& filepath,
                          const std::string& content);
  static Status RemoveFile(const std::string& filepath);
  static Status StatFS(const std::string& dir, FSStat* stat);
  static bool IsFile(const struct stat* stat);
  static bool IsDir(const struct stat* stat);
  static bool IsLink(const struct stat* stat);
  static std::string StrMode(uint16_t mode);

  // validator
  static bool NonEmptyString(const char* /*name*/, const std::string& value);

  // others
  static std::vector<uint64_t> NormalizeByGcd(
      const std::vector<uint64_t>& nums);

  static void DeleteBuffer(void* data);

  static bool IsAligned(const IOBuffer& buffer);

  static double Divide(uint64_t a, uint64_t b);

  static bool ProtoToJson(const google::protobuf::Message& message,
                          std::string& json);

  // split "/dir1:10;/dir2:200":
  // [ {"/dir1", 10}, {"/dir2", 200} ]
  static void SplitUniteCacheDir(
      const std::string& cache_dir, uint64_t default_cache_size_mb,
      std::vector<std::pair<std::string, uint64_t>>* cache_dirs);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_HELPER_H_
