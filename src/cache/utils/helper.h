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
#include <sys/stat.h>

#include <sstream>
#include <string>

#include "cache/common/common.h"
#include "cache/storage/filesystem_base.h"

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

  // sys conf
  static uint64_t GetSysPageSize();
  static uint64_t GetIOAlignedBlockSize();
  static bool IsAligned(uint64_t n, uint64_t m);

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

  static std::string TempFilepath(const std::string& filepath);
  static bool IsTempFilepath(const std::string& filepath);

  // utils
  static std::vector<uint64_t> NormalizeByGcd(
      const std::vector<uint64_t>& nums);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_HELPER_H_
