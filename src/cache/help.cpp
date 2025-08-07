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

#include "cache/help.h"

#include <absl/strings/str_format.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <sstream>
#include <vector>

namespace dingofs {
namespace cache {

struct Option {
  std::string name;
  std::string description;
  std::string default_value;
};

static std::vector<gflags::CommandLineFlagInfo> GetCacheFlags() {
  std::vector<gflags::CommandLineFlagInfo> flags;
  std::vector<gflags::CommandLineFlagInfo> cache_flags;

  gflags::GetAllFlags(&flags);
  for (const auto& flag : flags) {
    if (flag.description.empty()) {  // DEPRECATED in future
      continue;
    }

    if (flag.filename.find("cache") != std::string::npos) {
      cache_flags.push_back(flag);
    }
  }
  return cache_flags;
}

static bool CmpFlag(const gflags::CommandLineFlagInfo& a,
                    const gflags::CommandLineFlagInfo& b) {
  return a.name < b.name;
}

static std::vector<Option> Normalize(
    std::vector<gflags::CommandLineFlagInfo> flags) {
  std::vector<Option> options;
  Option option;
  for (auto& flag : flags) {
    if (flag.type == "bool") {
      option.name = flag.name;
    } else {
      option.name = flag.name + "=" + flag.type;
    }
    option.description = flag.description;
    option.default_value = flag.default_value;

    options.emplace_back(option);
  }
  return options;
}

static size_t GetMaxNameLength(const std::vector<Option>& options) {
  size_t max_name = 0;
  std::for_each(options.begin(), options.end(), [&](const Option& option) {
    max_name = std::max(max_name, option.name.size());
  });
  return max_name;
}

std::string Usage() {
  auto flags = GetCacheFlags();
  std::sort(flags.begin(), flags.end(), CmpFlag);
  auto options = Normalize(flags);
  size_t max_name_length = GetMaxNameLength(options);

  std::ostringstream os;
  os << "dingo-cache " << GIT_TAG_NAME << "\n";
  os << "usage: dingo-cache --group_name=<group_name> --listen_ip=<listen_ip> "
        "[OPTIONS]\n";
  for (const auto& option : options) {
    os << "  --" << std::left << std::setw(max_name_length) << option.name
       << std::right << "  " << option.description
       << " (default: " << option.default_value << ")\n";
  }

  os << "\n";
  os << "dingo-cache was written by dingofs team "
        "<https://github.com/dingodb/dingofs>";
  return os.str();
}

}  // namespace cache
}  // namespace dingofs
