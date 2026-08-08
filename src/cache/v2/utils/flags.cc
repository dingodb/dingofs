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

#include "cache/v2/utils/flags.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

#include "cache/v2/utils/string.h"
#include "common/version.h"
#include "utils/uuid.h"

namespace dingofs {
namespace cache {
namespace v2 {

DEFINE_string(conf, "", "config file path");

static bool Contains(const std::vector<std::string_view>& names,
                     const std::string& name) {
  return std::ranges::find(names, name) != names.end();
}

static const char* MatchFlag(const char* arg, std::string_view name) {
  if (*arg++ != '-') {
    return nullptr;
  }
  if (*arg == '-') {
    ++arg;
  }
  if (std::strncmp(arg, name.data(), name.size()) != 0) {
    return nullptr;
  }
  const char* rest = arg + name.size();
  return *rest == '\0' || *rest == '=' ? rest : nullptr;
}

static bool IsFlag(const char* arg, std::string_view name) {
  const char* rest = MatchFlag(arg, name);
  return rest != nullptr && *rest == '\0';
}

static bool IsFlagPrefixed(const char* arg, std::string_view prefix) {
  if (*arg++ != '-') {
    return false;
  }
  if (*arg == '-') {
    ++arg;
  }
  return std::strncmp(arg, prefix.data(), prefix.size()) == 0;
}

static size_t SectionRank(const FlagParser::Usage& usage,
                          const gflags::CommandLineFlagInfo& flag) {
  for (size_t i = 0; i < usage.sections.size(); ++i) {
    const FlagSection& section = usage.sections[i];
    if (Contains(section.flags, flag.name) ||
        (!section.file.empty() &&
         flag.filename.find(section.file) != std::string::npos)) {
      return i;
    }
  }
  return usage.sections.size();
}

static std::string RequiredMark() {
  static const bool tty = ::isatty(STDOUT_FILENO) != 0;
  return tty ? RedString("[required]") : "[required]";
}

bool FlagParser::Parse(int* argc, char*** argv, const Usage& usage) {
  bool show_help = false;
  bool show_full_help = false;
  bool show_version = false;
  bool create_template = false;

  for (int i = 1; i < *argc; i++) {
    const char* arg = (*argv)[i];
    if (const char* rest = MatchFlag(arg, "conf"); rest != nullptr) {
      if (*rest == '=') {
        FLAGS_conf = rest + 1;
      } else if (i + 1 < *argc) {
        FLAGS_conf = (*argv)[++i];
      }
    } else if (IsFlag(arg, "version")) {
      show_version = true;
    } else if (IsFlag(arg, "h") || IsFlag(arg, "help") ||
               IsFlag(arg, "helpshort")) {
      show_help = true;
    } else if (IsFlag(arg, "hh") || IsFlagPrefixed(arg, "help")) {
      show_full_help = true;
    } else if (IsFlag(arg, "t") || IsFlag(arg, "tmpl")) {
      create_template = true;
    }
  }

  gflags::SetUsageMessage(usage.usage);
  gflags::SetVersionString(DingoShortVersionString());

  if (show_version) {
    std::cout << DingoShortVersionString() << "\n";
  } else if (show_full_help) {
    std::cout << GenHelp(usage, Collect(usage)) << "\n";
  } else if (show_help) {
    const size_t all = Collect(usage).size();
    std::cout << GenHelp(usage, CollectEssential(usage)) << "\n\n"
              << "This is the reduced help; use -hh to list all " << all
              << " options.\n";
  } else if (create_template) {
    std::cout << GenTemplate(usage, Collect(usage));
  } else {
    if (!FLAGS_conf.empty()) {
      gflags::ReadFromFlagsFile(FLAGS_conf, usage.program.c_str(), true);
    }
    gflags::ParseCommandLineNonHelpFlags(argc, argv, true);
    return true;
  }
  return false;
}

std::string_view FlagParser::SectionOf(
    const Usage& usage, const gflags::CommandLineFlagInfo& flag) {
  const size_t rank = SectionRank(usage, flag);
  return rank < usage.sections.size() ? usage.sections[rank].name
                                      : std::string_view{};
}

std::vector<gflags::CommandLineFlagInfo> FlagParser::Collect(
    const Usage& usage) {
  std::vector<gflags::CommandLineFlagInfo> all;
  gflags::GetAllFlags(&all);

  std::vector<std::pair<size_t, gflags::CommandLineFlagInfo>> ours;
  for (gflags::CommandLineFlagInfo& flag : all) {
    if (flag.description.empty()) {
      continue;
    }
    const size_t rank = SectionRank(usage, flag);
    if (rank == usage.sections.size()) {
      CHECK(flag.filename.find("cache/v2") == std::string::npos)
          << "--" << flag.name << " is in no section; add it to "
          << usage.program << "'s section table";
      continue;
    }
    ours.emplace_back(rank, std::move(flag));
  }

  std::ranges::sort(ours, [](const auto& a, const auto& b) {
    return a.first != b.first ? a.first < b.first
                              : a.second.name < b.second.name;
  });

  std::vector<gflags::CommandLineFlagInfo> out;
  out.reserve(ours.size());
  for (auto& [rank, flag] : ours) {
    out.push_back(std::move(flag));
  }
  return out;
}

std::vector<gflags::CommandLineFlagInfo> FlagParser::CollectEssential(
    const Usage& usage) {
  std::vector<gflags::CommandLineFlagInfo> out;
  for (auto& flag : Collect(usage)) {
    if (Contains(usage.essential, flag.name)) {
      out.push_back(std::move(flag));
    }
  }
  return out;
}

std::string FlagParser::GenHelp(
    const Usage& usage, const std::vector<gflags::CommandLineFlagInfo>& flags) {
  size_t width = 0;
  for (const auto& flag : flags) {
    width = std::max(width, flag.name.size() + flag.type.size() + 1);
  }

  std::ostringstream os;
  os << usage.program << " " << DingoShortVersionString() << "\n\n";
  os << "Usage:\n" << usage.usage << "\n\n";
  os << "Examples:\n" << usage.examples;

  std::string_view section;
  for (const auto& flag : flags) {
    if (SectionOf(usage, flag) != section) {
      section = SectionOf(usage, flag);
      os << "\n" << section << ":\n";
    }
    os << "  --" << std::left << std::setw(static_cast<int>(width))
       << (flag.name + "=" + flag.type) << std::right << "  "
       << flag.description;
    if (Contains(usage.required, flag.name)) {
      os << " " << RequiredMark();
    } else if (!flag.default_value.empty()) {
      os << " (default: " << flag.default_value << ")";
    }
    os << "\n";
  }

  os << "\n"
     << usage.program
     << " was written by dingofs team <https://github.com/dingodb/dingofs>";
  return os.str();
}

std::string FlagParser::GenTemplate(
    const Usage& usage, const std::vector<gflags::CommandLineFlagInfo>& flags) {
  std::ostringstream os;
  std::string_view section;
  for (const auto& flag : flags) {
    if (flag.name == "conf") {
      continue;
    }
    if (SectionOf(usage, flag) != section) {
      section = SectionOf(usage, flag);
      os << (os.tellp() > 0 ? "\n" : "") << "# " << section << "\n";
    }
    std::string value = flag.default_value;
    if (flag.name == usage.uuid_flag) {
      value = utils::GenerateUUID();
    }
    const bool fill_in = value.empty() && Contains(usage.required, flag.name);
    os << (fill_in ? "# --" : "--") << flag.name << "=" << value << "\n";
  }
  return os.str();
}

std::string FlagParser::GenCurrentFlags(
    const std::vector<gflags::CommandLineFlagInfo>& flags) {
  std::ostringstream os;
  os << "Current flags:\n";
  for (const auto& flag : flags) {
    os << "  --" << flag.name << "=" << flag.current_value << "\n";
  }
  return os.str();
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
