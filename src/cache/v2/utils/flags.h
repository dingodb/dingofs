/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_UTILS_FLAGS_H_
#define DINGOFS_CACHE_V2_UTILS_FLAGS_H_

#include <gflags/gflags.h>

#include <string>
#include <string_view>
#include <vector>

namespace dingofs {
namespace cache {
namespace v2 {

struct FlagSection {
  std::string_view name;
  std::vector<std::string_view> flags;
  std::string_view file;
};

class FlagParser {
 public:
  struct Usage {
    std::string program;
    std::string usage;
    std::string examples;
    std::vector<FlagSection> sections;
    std::vector<std::string_view> essential;
    std::vector<std::string_view> required;
    std::string_view uuid_flag;
  };

  static bool Parse(int* argc, char*** argv, const Usage& usage);

  static std::string_view SectionOf(const Usage& usage,
                                    const gflags::CommandLineFlagInfo& flag);
  static std::vector<gflags::CommandLineFlagInfo> Collect(const Usage& usage);
  static std::vector<gflags::CommandLineFlagInfo> CollectEssential(
      const Usage& usage);
  static std::string GenHelp(const Usage& usage,
                             const std::vector<gflags::CommandLineFlagInfo>&);
  static std::string GenTemplate(
      const Usage& usage,
      const std::vector<gflags::CommandLineFlagInfo>& flags);
  static std::string GenCurrentFlags(
      const std::vector<gflags::CommandLineFlagInfo>& flags);
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_FLAGS_H_
