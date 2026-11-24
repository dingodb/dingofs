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

#include "blockcache/utils/flags.h"

#include <glog/logging.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <iostream>
#include <sstream>
#include <utility>

#include "blockcache/utils/string.h"
#include "common/helper.h"
#include "common/version.h"
#include "utils/uuid.h"

namespace dingofs {
namespace blockcache {

DEFINE_string(conf, "", "config file path");

static constexpr size_t kIndent = 2;
static constexpr size_t kGap = 2;
static constexpr size_t kMaxHeadSize = 30;
static constexpr size_t kDefaultWidth = 80;
static constexpr size_t kMinWidth = 60;
static constexpr size_t kMaxWidth = 100;
static constexpr std::string_view kRequired = "[required]";

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

static const char* MatchConf(const char* arg) {
  const char* rest = MatchFlag(arg, "conf");
  return rest != nullptr ? rest : MatchFlag(arg, "flagfile");
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
    const auto under = [&flag](std::string_view dir) {
      return flag.filename.find(dir) != std::string::npos;
    };
    if (Contains(section.flags, flag.name) ||
        std::ranges::any_of(section.dirs, under)) {
      return i;
    }
  }
  return usage.sections.size();
}

static std::string RequiredMark() {
  static const bool tty = ::isatty(STDOUT_FILENO) != 0;
  const std::string mark(kRequired);
  return tty ? RedString(mark) : mark;
}

static size_t GetHelpWidth() {
  size_t width = 0;
  if (const char* columns = std::getenv("COLUMNS"); columns != nullptr) {
    std::string_view str = columns;
    if (!ConsumeUint(&str, '\0', &width)) {
      width = 0;
    }
  }
  winsize size{};
  if (width == 0 && ::ioctl(STDOUT_FILENO, TIOCGWINSZ, &size) == 0) {
    width = size.ws_col;
  }
  return std::clamp(width != 0 ? width : kDefaultWidth, kMinWidth, kMaxWidth);
}

static std::string FormatValue(const std::string& type,
                               const std::string& value) {
  if (type != "double") {
    return value;
  }
  double number = 0;
  const char* end = value.data() + value.size();
  const auto [ptr, ec] = std::from_chars(value.data(), end, number);
  if (ec != std::errc() || ptr != end) {
    return value;
  }
  char buf[32];
  return {buf, std::to_chars(buf, buf + sizeof(buf), number).ptr};
}

static size_t GetColumn(const std::string& text) {
  return text.size() - (text.rfind('\n') + 1);
}

static void AppendWord(std::string_view word, size_t indent, size_t width,
                       std::string* out) {
  size_t column = GetColumn(*out);
  if (column > indent && column + 1 + word.size() >= width) {
    *out += '\n';
    column = 0;
  }
  out->append(column > indent ? 1 : indent - column, ' ').append(word);
}

static void AppendText(std::string_view text, size_t indent, size_t width,
                       std::string* out) {
  for (size_t begin = 0;;) {
    const size_t end = std::min(text.find('\n', begin), text.size());
    for (const std::string& word :
         SplitStr(text.substr(begin, end - begin), ' ')) {
      AppendWord(word, indent, width, out);
    }
    if (end == text.size()) {
      return;
    }
    *out += '\n';
    begin = end + 1;
  }
}

static std::string GetHead(const gflags::CommandLineFlagInfo& flag) {
  return "--" + flag.name + "=" + flag.type;
}

static std::string FormatFlag(const FlagParser::Usage& usage,
                              const gflags::CommandLineFlagInfo& flag,
                              size_t column, size_t width) {
  std::string out = std::string(kIndent, ' ') + GetHead(flag);
  if (out.size() + kGap > column) {
    out += '\n';
  }
  AppendText(flag.description, column, width, &out);
  if (Contains(usage.required, flag.name)) {
    AppendWord(kRequired, column, width, &out);
    out.replace(out.size() - kRequired.size(), kRequired.size(),
                RequiredMark());
  } else if (!flag.default_value.empty()) {
    AppendWord("(default: " + FormatValue(flag.type, flag.default_value) + ")",
               column, width, &out);
  }
  return out;
}

static void PrintHelp(const std::string& help, std::string_view note,
                      size_t width) {
  std::string out = help + "\n\n";
  AppendText(note, 0, width, &out);
  std::cout << out << "\n";
}

static std::string NormalizeName(std::string_view name) {
  std::string out;
  for (const char c : name) {
    if (c != '_' && c != '-') {
      out += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
  }
  return out;
}

static double GetSimilarity(std::string_view a, std::string_view b) {
  if (a.empty() || b.empty()) {
    return a == b ? 1 : 0;
  }
  const size_t window =
      std::max<size_t>(std::max(a.size(), b.size()) / 2, 1) - 1;
  std::vector<bool> a_matched(a.size());
  std::vector<bool> b_matched(b.size());
  size_t matches = 0;
  for (size_t i = 0; i < a.size(); i++) {
    const size_t last = std::min(b.size(), i + window + 1);
    for (size_t j = i > window ? i - window : 0; j < last; j++) {
      if (!b_matched[j] && a[i] == b[j]) {
        a_matched[i] = true;
        b_matched[j] = true;
        matches++;
        break;
      }
    }
  }
  if (matches == 0) {
    return 0;
  }
  size_t transposed = 0;
  for (size_t i = 0, j = 0; i < a.size(); i++) {
    if (a_matched[i]) {
      while (!b_matched[j]) {
        j++;
      }
      transposed += a[i] != b[j++] ? 1 : 0;
    }
  }
  const double m = static_cast<double>(matches);
  return (m / a.size() + m / b.size() + (m - transposed / 2.0) / m) / 3;
}

static std::vector<std::string> GetSimilarFlags(const FlagParser::Usage& usage,
                                                const std::string& name) {
  const std::string typed = NormalizeName(name);
  const bool negated = typed.starts_with("no");
  std::vector<std::pair<double, std::string>> scores;
  for (const auto& flag : FlagParser::Collect(usage)) {
    const std::string candidate =
        flag.type == "bool" && negated ? "no" + flag.name : flag.name;
    scores.emplace_back(GetSimilarity(typed, NormalizeName(candidate)),
                        candidate);
  }
  std::ranges::sort(scores, std::greater<>());

  std::vector<std::string> flags;
  for (const auto& [similarity, flag] : scores) {
    if (similarity <= 0.7 || similarity < scores[0].first - 0.03 ||
        flags.size() == 3) {
      break;
    }
    flags.push_back("--" + flag);
  }
  return flags;
}

static std::string FormatHint(const std::vector<std::string>& choices) {
  if (choices.empty()) {
    return "";
  }
  std::string hint = " (did you mean " + choices[0];
  for (size_t i = 1; i < choices.size(); i++) {
    hint += (i + 1 < choices.size() ? ", " : " or ") + choices[i];
  }
  return hint + "?)";
}

static std::string GetFlagType(const std::string& name) {
  gflags::CommandLineFlagInfo info;
  if (gflags::GetCommandLineFlagInfo(name.c_str(), &info)) {
    return info.type;
  }
  if (name.starts_with("no") &&
      gflags::GetCommandLineFlagInfo(name.c_str() + 2, &info) &&
      info.type == "bool") {
    return info.type;
  }
  return "";
}

static bool IsNegatedBool(const std::string& name) {
  gflags::CommandLineFlagInfo info;
  return name.starts_with("no") &&
         !gflags::GetCommandLineFlagInfo(name.c_str(), &info) &&
         GetFlagType(name) == "bool";
}

static bool IsBoolValue(const std::string& arg) {
  static const std::vector<std::string_view> kValues = {
      "true", "false", "yes", "no", "t", "f", "y", "n", "1", "0"};
  return Contains(kValues, Helper::ToLowerCase(arg));
}

static std::string GetArgHint(const std::string& arg,
                              const std::string& bare_bool) {
  if (!bare_bool.empty() && IsBoolValue(arg)) {
    return FormatHint({"--" + bare_bool + "=" + arg});
  }
  const size_t eq = arg.find('=');
  if (eq != std::string::npos && !GetFlagType(arg.substr(0, eq)).empty()) {
    return FormatHint({"--" + arg});
  }
  return "";
}

static bool IsFlagArg(const std::string& arg) {
  return arg.size() > 1 && arg[0] == '-' &&
         std::isdigit(static_cast<unsigned char>(arg[1])) == 0 && arg[1] != '.';
}

struct Arg {
  std::string text;
  std::string where;
};

static void ReadConf(const std::string& path, std::vector<Arg>* args,
                     std::vector<std::string>* errors) {
  std::ifstream file(path);
  if (!file) {
    errors->push_back("cannot read conf file '" + path +
                      "': " + std::strerror(errno));
    return;
  }
  std::string line;
  for (size_t number = 1; std::getline(file, line); number++) {
    std::string text = TrimWhitespace(line);
    if (!text.empty() && text[0] != '#') {
      args->push_back({.text = std::move(text),
                       .where = path + ":" + std::to_string(number) + ": "});
    }
  }
  if (file.bad()) {
    errors->push_back("cannot read conf file '" + path +
                      "': " + std::strerror(errno));
  }
}

static void CheckArgs(const FlagParser::Usage& usage,
                      const std::vector<Arg>& args,
                      std::vector<std::string>* errors) {
  bool flags_ended = false;
  std::string bare_bool;
  for (size_t i = 1; i < args.size(); i++) {
    const auto& [text, where] = args[i];
    const bool in_conf = !where.empty();
    if (!in_conf && !flags_ended && text == "--") {
      flags_ended = true;
      continue;
    }
    const bool is_flag = !flags_ended && text.size() > 1 && text[0] == '-';
    const size_t begin = text.starts_with("--") ? 2 : 1;
    const size_t eq = text.find('=');
    const std::string name = is_flag ? text.substr(begin, eq - begin) : "";
    if (name.empty()) {
      errors->push_back(
          where +
          (in_conf ? "expected --name=value, got '" : "unexpected argument '") +
          text + "'" + GetArgHint(text, bare_bool));
      bare_bool.clear();
      continue;
    }
    if (in_conf && (text.find(" #") != std::string::npos ||
                    text.find("\t#") != std::string::npos)) {
      errors->push_back(where + "a comment needs a line of its own");
      continue;
    }
    if (const size_t blank = name.find_first_of(" \t");
        blank != std::string::npos) {
      const std::string flag = name.substr(0, blank);
      const size_t value = text.find_first_not_of(" \t=", begin + blank);
      errors->push_back(
          where + "expected --name=value, got '" + text + "'" +
          (GetFlagType(flag).empty() || value == std::string::npos
               ? ""
               : FormatHint({"--" + flag + "=" + text.substr(value)})));
      bare_bool.clear();
      continue;
    }
    if (in_conf && (name == "conf" || name == "flagfile")) {
      errors->push_back(where + "--" + name + " cannot be set in a conf file");
      continue;
    }

    const std::string type = GetFlagType(name);
    const bool has_value = eq != std::string::npos;
    const bool takes_next = !has_value && !in_conf && i + 1 < args.size() &&
                            !IsFlagArg(args[i + 1].text);
    bare_bool = type == "bool" && !has_value && !in_conf ? name : "";
    if (type.empty()) {
      errors->push_back(where + "unknown flag '--" + name + "'" +
                        FormatHint(GetSimilarFlags(usage, name)));
      i += takes_next ? 1 : 0;
    } else if (type != "bool" && !has_value) {
      if (takes_next) {
        i++;
      } else {
        errors->push_back(where + "flag '--" + name + "' is missing its value");
      }
    } else if (has_value && IsNegatedBool(name)) {
      errors->push_back(where + "flag '--" + name + "' takes no value");
    }
  }
}

static bool SetFlags(int* argc, char*** argv, const FlagParser::Usage& usage,
                     const std::vector<std::string>& confs) {
  std::vector<Arg> args = {{.text = (*argv)[0]}};
  std::vector<std::string> errors;
  for (const std::string& conf : confs) {
    ReadConf(conf, &args, &errors);
  }
  for (int i = 1; i < *argc; i++) {
    const char* rest = MatchFlag((*argv)[i], "flagfile");
    args.push_back({.text = rest != nullptr ? "--conf" + std::string(rest)
                                            : std::string((*argv)[i])});
  }
  CheckArgs(usage, args, &errors);
  if (!errors.empty()) {
    for (const std::string& error : errors) {
      std::cerr << "ERROR: " << error << "\n";
    }
    std::cerr << "Try '" << usage.program << " -h', or '" << usage.program
              << " --help=WORD' to search the options.\n";
    return false;
  }

  std::vector<char*> flags;
  flags.reserve(args.size() + 1);
  for (Arg& arg : args) {
    flags.push_back(arg.text.data());
  }
  flags.push_back(nullptr);
  int count = static_cast<int>(args.size());
  char** rest = flags.data();
  gflags::ParseCommandLineNonHelpFlags(&count, &rest, true);
  *argc = 1;
  return true;
}

std::optional<int> FlagParser::Parse(int* argc, char*** argv,
                                     const Usage& usage) {
  bool show_help = false;
  bool show_full_help = false;
  bool show_version = false;
  bool create_template = false;
  const char* keyword = nullptr;
  std::vector<std::string> confs;

  for (int i = 1; i < *argc; i++) {
    const char* arg = (*argv)[i];
    if (const char* rest = MatchConf(arg); rest != nullptr) {
      std::string value;
      if (*rest == '=') {
        value = rest + 1;
      } else if (i + 1 < *argc && !IsFlagArg((*argv)[i + 1])) {
        value = (*argv)[++i];
      }
      for (std::string& conf : SplitStr(value, ',')) {
        if (!conf.empty()) {
          confs.push_back(std::move(conf));
        }
      }
    } else if (IsFlag(arg, "version")) {
      show_version = true;
    } else if (IsFlag(arg, "h") || IsFlag(arg, "help") ||
               IsFlag(arg, "helpshort")) {
      show_help = true;
    } else if (const char* rest = MatchFlag(arg, "help"); rest != nullptr) {
      keyword = rest + 1;
    } else if (IsFlag(arg, "hh") || IsFlagPrefixed(arg, "help")) {
      show_full_help = true;
    } else if (IsFlag(arg, "t") || IsFlag(arg, "tmpl")) {
      create_template = true;
    }
  }

  gflags::SetUsageMessage(usage.usage);
  gflags::SetVersionString(DingoShortVersionString());

  const size_t width = GetHelpWidth();
  const auto all = [&usage] {
    return "-hh lists all " + std::to_string(Collect(usage).size()) +
           " options";
  };
  if (show_version) {
    std::cout << DingoShortVersionString() << "\n";
  } else if (show_full_help) {
    std::cout << GenHelp(usage, Collect(usage), width) << "\n";
  } else if (keyword != nullptr) {
    const auto flags = CollectMatching(usage, keyword);
    const std::string word = "'" + std::string(keyword) + "'";
    PrintHelp(GenHelp(usage, flags, width),
              (flags.empty() ? "No option matches " + word
                             : "These are the options matching " + word) +
                  "; " + all() + ".",
              width);
  } else if (show_help) {
    PrintHelp(
        GenHelp(usage, CollectEssential(usage), width),
        "This is the reduced help; " + all() + ", --help=WORD searches them.",
        width);
  } else if (create_template) {
    std::cout << GenTemplate(usage, Collect(usage));
  } else if (SetFlags(argc, argv, usage, confs)) {
    return std::nullopt;
  } else {
    return 1;
  }
  return 0;
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
      CHECK(flag.filename.find("blockcache") == std::string::npos)
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

std::vector<gflags::CommandLineFlagInfo> FlagParser::CollectMatching(
    const Usage& usage, std::string_view word) {
  const std::string lower_word = Helper::ToLowerCase(std::string(word));
  const auto holds = [&lower_word](std::string_view text) {
    return Helper::ToLowerCase(std::string(text)).find(lower_word) !=
           std::string::npos;
  };
  const auto names = [&lower_word](std::string_view section) {
    return (" " + Helper::ToLowerCase(std::string(section)) + " ")
               .find(" " + lower_word + " ") != std::string::npos;
  };
  std::vector<gflags::CommandLineFlagInfo> out;
  for (auto& flag : Collect(usage)) {
    if (holds(flag.name) || holds(flag.description) ||
        names(SectionOf(usage, flag))) {
      out.push_back(std::move(flag));
    }
  }
  return out;
}

std::string FlagParser::GenHelp(
    const Usage& usage, const std::vector<gflags::CommandLineFlagInfo>& flags,
    size_t width) {
  size_t head_size = 0;
  for (const auto& flag : flags) {
    head_size = std::max(head_size, GetHead(flag).size());
  }
  const size_t column = kIndent + std::min(head_size, kMaxHeadSize) + kGap;

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
    os << FormatFlag(usage, flag, column, width) << "\n";
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
    std::string value = FormatValue(flag.type, flag.default_value);
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
    os << "  --" << flag.name << "="
       << FormatValue(flag.type, flag.current_value) << "\n";
  }
  return os.str();
}

}  // namespace blockcache
}  // namespace dingofs
