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

#include "cache/common/flag.h"

#include <absl/strings/str_format.h>
#include <gflags/gflags.h>

#include <algorithm>
#include <sstream>
#include <vector>

#include "cache/utils/helper.h"
#include "utils/string.h"

namespace dingofs {
namespace cache {

FlagsInfo FlagsHelper::Parse(int argc, char** argv) {
  FlagsInfo flags;
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "--version") == 0) {
      flags.show_version = true;
    } else if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      flags.show_help = true;
    } else if (strcmp(argv[i], "-t") == 0 || strcmp(argv[i], "--tmpl") == 0) {
      flags.create_template = true;
    }
  }

  if (!flags.show_help && !flags.show_version && !flags.create_template) {
    gflags::ParseCommandLineNonHelpFlags(&argc, &argv, false);
  }
  flags.gflags = GetAllGFlags("cache");
  return flags;
}

std::string FlagsHelper::GenHelp(const FlagsInfo& flags) {
  auto rows = Normalize(flags);
  size_t max_name_length = 0;
  std::for_each(rows.begin(), rows.end(), [&](const Row& row) {
    max_name_length = std::max(max_name_length, row.name.size());
  });

  std::ostringstream os;
  os << "dingo-cache " << GIT_TAG_NAME << "\n";
  os << "usage: dingo-cache [OPTIONS]\n";
  for (const auto& row : rows) {
    os << "  --" << std::left << std::setw(max_name_length) << row.name
       << std::right << "  " << row.description << " " << row.default_value
       << "\n";
  }

  os << "\n";
  os << "dingo-cache was written by dingofs team "
        "<https://github.com/dingodb/dingofs>";
  return os.str();
}

std::string FlagsHelper::GenTemplate(const FlagsInfo& flags) {
  std::ostringstream os;
  for (const auto& gflag : flags.gflags) {
    auto key = gflag.name;
    auto value = gflag.default_value;
    if (key == "id") {
      value = utils::GenUuid();
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

std::vector<gflags::CommandLineFlagInfo> FlagsHelper::GetAllGFlags(
    const std::string& pattern) {
  std::vector<gflags::CommandLineFlagInfo> all_flags;
  std::vector<gflags::CommandLineFlagInfo> flags_out;

  gflags::GetAllFlags(&all_flags);
  for (const auto& flag : all_flags) {
    if (flag.description.empty()) {  // hiden the flag
      continue;
    }

    if (flag.filename.find(pattern) != std::string::npos) {
      flags_out.push_back(flag);
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
    row.description = gflag.description;

    if (gflag.default_value.empty() && gflag.has_validator_fn) {
      row.default_value = Helper::RedString("[required]");
    } else if (!gflag.default_value.empty()) {
      row.default_value = absl::StrFormat("(default: %s)", gflag.default_value);
    } else {
      row.default_value = "";
    }
    rows.push_back(row);
  }
  return rows;
}

}  // namespace cache
}  // namespace dingofs
