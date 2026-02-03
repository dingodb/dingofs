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

#ifndef DINGOFS_SRC_COMMON_FLAG_H_
#define DINGOFS_SRC_COMMON_FLAG_H_

#include <gflags/gflags.h>

#include <iostream>
#include <unordered_map>

#include "common/directory.h"
#include "common/version.h"

namespace dingofs {

// brpc flags default value
static const std::unordered_map<std::string, std::string>
    kBrpcFlagDefaultValueMap = {{"log_dir", GetDefaultDir(kLogDir)},
                                {"max_connection_pool_size", "10"},
                                {"connect_timeout_as_unreachable", "500"}};

struct FlagExtraInfo {
  std::string program;                // program name
  std::string usage;                  // usage info
  std::string examples;               // examples
  std::vector<std::string> patterns;  // used for filtering gflags
};

struct FlagsInfo {
  bool show_help{false};
  bool show_version{false};
  bool create_template{false};
  bool has_flagfile{false};
  std::vector<gflags::CommandLineFlagInfo> gflags;
  FlagExtraInfo extra_info;
};

static FlagsInfo g_flags;

class FlagsHelper {
 public:
  static FlagsInfo Parse(int* argc, char*** argv,
                         const FlagExtraInfo& extra_info);

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
      const std::string& program, const std::vector<std::string>& patterns);
  static std::vector<Row> Normalize(const FlagsInfo& flags);
};

static int ParseFlags(int* argc, char*** argv, const FlagExtraInfo& extras) {
  g_flags = FlagsHelper::Parse(argc, argv, extras);
  if (g_flags.show_help) {
    std::cout << FlagsHelper::GenHelp(g_flags) << "\n";
    return 1;
  } else if (g_flags.show_version) {
    std::cout << dingofs::DingoShortVersionString() << "\n";
    return 1;
  } else if (g_flags.create_template) {
    std::cout << FlagsHelper::GenTemplate(g_flags);
    return 1;
  }

  return 0;
}

static std::string GenCurrentFlags() {
  return FlagsHelper::GenCurrentFlags(g_flags);
}

static void ResetBrpcFlagDefaultValue() {
  for (const auto& [name, value] : kBrpcFlagDefaultValueMap) {
    gflags::CommandLineFlagInfo flag_info;
    if (!gflags::GetCommandLineFlagInfo(name.c_str(), &flag_info)) {
      continue;
    }
    if (flag_info.is_default) {
      gflags::SetCommandLineOption(name.c_str(), value.c_str());
    }
  }
}

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_FLAG_H_
