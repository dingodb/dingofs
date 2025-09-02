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
 * Created Date: 2025-08-07
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_FLAG_H_
#define DINGOFS_SRC_CACHE_COMMON_FLAG_H_

#include <gflags/gflags.h>

namespace dingofs {
namespace cache {

struct FlagsInfo {
  bool show_help{false};
  bool show_version{false};
  bool create_template{false};
  std::vector<gflags::CommandLineFlagInfo> gflags;
};

class FlagsHelper {
 public:
  static FlagsInfo Parse(int argc, char** argv);

  static std::string GenHelp(const FlagsInfo& flags);
  static std::string GenTemplate(const FlagsInfo& flags);
  static std::string GenCurrentFlags(const FlagsInfo& flags);

 private:
  struct Row {
    std::string name;
    std::string description;
    std::string default_value;
  };

  static std::vector<gflags::CommandLineFlagInfo> GetAllGFlags(
      const std::string& pattern);
  static std::vector<Row> Normalize(const FlagsInfo& flags);
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_FLAG_H_
