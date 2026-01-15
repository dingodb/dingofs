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

#include "common/flag.h"

#include <absl/strings/str_format.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <set>
#include <sstream>
#include <vector>

#include "common/const.h"
#include "common/helper.h"
#include "common/version.h"
#include "utils/uuid.h"

namespace dingofs {

static std::string RedString(const std::string& str) {
  return absl::StrFormat("\x1B[31m%s\033[0m", str);
}

FlagsInfo FlagsHelper::Parse(int* argc, char*** argv,
                             const FlagExtraInfo& extra_info) {
  FlagsInfo flags;
  for (int i = 1; i < *argc; i++) {
    if (strcmp((*argv)[i], "-v") == 0 || strcmp((*argv)[i], "--version") == 0) {
      flags.show_version = true;
    } else if (strcmp((*argv)[i], "-h") == 0 ||
               strcmp((*argv)[i], "--help") == 0) {
      flags.show_help = true;
    } else if (strcmp((*argv)[i], "-t") == 0 ||
               strcmp((*argv)[i], "--tmpl") == 0) {
      flags.create_template = true;
    }
  }

  if (!flags.show_help && !flags.show_version && !flags.create_template) {
    gflags::ParseCommandLineNonHelpFlags(argc, argv, true);
  }
  flags.gflags = GetAllGFlags(extra_info.program, extra_info.patterns);
  flags.extra_info = extra_info;
  return flags;
}

std::string FlagsHelper::GenHelp(const FlagsInfo& flags) {
  auto rows = Normalize(flags);
  size_t max_name_length = 0;
  std::for_each(rows.begin(), rows.end(), [&](const Row& row) {
    max_name_length = std::max(max_name_length, row.name.size());
  });

  std::ostringstream os;
  // print program name and version
  os << flags.extra_info.program << " " << dingofs::DingoShortVersionString()
     << "\n\n";
  os << "Usage:\n";
  os << flags.extra_info.usage << "\n";
  os << "\n";
  os << "Examples:\n";
  os << flags.extra_info.examples << "\n";
  os << "Options:\n";
  for (const auto& row : rows) {
    os << "  --" << std::left << std::setw(max_name_length) << row.name
       << std::right << "  " << row.description << " " << row.default_value
       << "\n";
  }

  os << "\n";
  os << flags.extra_info.program
     << " was written by dingofs team <https://github.com/dingodb/dingofs>";
  return os.str();
}

std::string FlagsHelper::GenTemplate(const FlagsInfo& flags) {
  std::ostringstream os;
  for (const auto& gflag : flags.gflags) {
    auto key = gflag.name;
    auto value = gflag.default_value;
    if (key == "id") {
      value = utils::GenerateUUID();
    }
    os << "--" << key << "=" << value << "\n";
  }
  return os.str();
}

std::string FlagsHelper::GenCurrentFlags(const FlagsInfo& flags) {
  std::ostringstream os;
  os << "Current flags:\n";
  for (const auto& gflag : flags.gflags) {
    os << "  --" << gflag.name << "=" << gflag.current_value << "\n";
  }
  return os.str();
}

static std::set<std::string> kGflagWhiteList = {
    "log_dir", "log_level", "log_v", "connect_timeout_as_unreachable",
    "max_connection_pool_size"};

static std::set<std::string> kClientGflagBlackList = {"mds_addrs"};

std::vector<gflags::CommandLineFlagInfo> FlagsHelper::GetAllGFlags(
    const std::string& program, const std::vector<std::string>& patterns) {
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  std::vector<gflags::CommandLineFlagInfo> flags_out;

  gflags::GetAllFlags(&all_flags);
  for (auto& flag : all_flags) {
    if (flag.description.empty()) {  // hiden the flag
      continue;
    }

    // show brpc default value
    auto it = kBrpcFlagDefaultValueMap.find(flag.name);
    if (it != kBrpcFlagDefaultValueMap.end()) {
      flag.default_value = it->second;
    }

    if (kGflagWhiteList.count(flag.name) > 0) {
      flags_out.push_back(flag);
      continue;
    }

    if (kClientGflagBlackList.count(flag.name) > 0 &&
        program == "dingo-client") {
      continue;
    }

    for (const auto& pattern : patterns) {
      if (flag.filename.find(pattern) != std::string::npos) {
        flags_out.push_back(flag);
        break;
      }
    }
  }

  std::sort(flags_out.begin(), flags_out.end(),
            [](const auto& a, const auto& b) { return a.name < b.name; });
  return flags_out;
}

std::vector<FlagsHelper::Row> FlagsHelper::Normalize(const FlagsInfo& flags) {
  Row row;
  std::vector<Row> rows;
  for (const auto& gflag : flags.gflags) {
    row.name = gflag.name + "=" + gflag.type;
    row.description = dingofs::Helper::ToLowerCase(gflag.description);

    if (gflag.default_value.empty() && gflag.has_validator_fn) {
      row.default_value = RedString("[required]");
    } else if (!gflag.default_value.empty()) {
      row.default_value = absl::StrFormat("(default: %s)", gflag.default_value);
    } else {
      row.default_value = "";
    }
    rows.push_back(row);
  }
  return rows;
}

}  // namespace dingofs
