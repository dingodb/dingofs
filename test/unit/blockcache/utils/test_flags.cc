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

/*
 * Project: DingoFS
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <optional>
#include <regex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "blockcache/utils/flags.h"
#include "blockcache/utils/string.h"
#include "common/options/cache.h"
#include "common/version.h"
#include "test/unit/blockcache/base/bthread.h"
#include "test/unit/blockcache/base/scoped_env.h"
#include "test/unit/blockcache/base/temp_dir.h"

namespace dingofs {
namespace blockcache {

using ::testing::Contains;
using ::testing::HasSubstr;
using ::testing::Not;

DECLARE_string(conf);

DEFINE_bool(flag_parser_test_undocumented, false, "");

const FlagParser::Usage kUsage = {
    .program = "dingo-cache",
    .usage = "  dingo-cache [OPTIONS] --id <uuid>",
    .examples =
        "  $ dingo-cache --id=044d4698-7bd4-4e44-9e94-aee6312ff06f\n"
        "  $ dingo-cache --conf cache.conf\n",
    .sections = {{.name = "RUNTIME OPTIONS", .dirs = {"blockcache/core/"}},
                 {.name = "NODE OPTIONS",
                  .flags = {"conf"},
                  .dirs = {"blockcache/node/"}},
                 {.name = "OTHER OPTIONS",
                  .flags = {"log_dir"},
                  .dirs = {"blockcache"}}},
    .essential = {"shards", "cpuset", "conf", "log_dir"},
    .required = {"id"},
    .uuid_flag = "id",
};

const std::vector<gflags::CommandLineFlagInfo> kFlags = {
    {.name = "shards",
     .type = "uint32",
     .description = "reactor shards, 0 means one per core",
     .current_value = "16",
     .default_value = "8",
     .filename = "/dingofs/src/blockcache/core/runtime/runtime.cc"},
    {.name = "conf",
     .type = "string",
     .description = "config file path",
     .current_value = "/etc/dingofs/cache.conf",
     .default_value = "",
     .filename = "/dingofs/src/blockcache/utils/flags.cc"},
    {.name = "id",
     .type = "string",
     .description = "cache node id",
     .current_value = "044d4698-7bd4-4e44-9e94-aee6312ff06f",
     .default_value = "",
     .filename = "/dingofs/src/blockcache/node/node.cc"},
    {.name = "listen_port",
     .type = "uint32",
     .description = "port to listen on",
     .current_value = "9301",
     .default_value = "9300",
     .filename = "/dingofs/src/blockcache/node/node.cc"},
    {.name = "log_dir",
     .type = "string",
     .description = "log directory",
     .current_value = "/var/log/dingofs",
     .default_value = "",
     .filename = "/glog/src/flags.cc"},
};

const gflags::CommandLineFlagInfo kRatioFlag = {
    .name = "free_space_ratio",
    .type = "double",
    .description = "min free space (ratio)",
    .current_value = "0.30000000000000004",
    .default_value = "0.10000000000000001",
    .filename = "/dingofs/src/blockcache/store/cache_manager.cc"};

const std::string kTryHelp =
    "Try 'dingo-cache -h', or 'dingo-cache --help=WORD' to search the "
    "options.\n";

struct ParseResult {
  std::optional<int> code;
  std::string out;
  std::string err;
  std::vector<std::string> rest;
};

static ParseResult Parse(std::vector<std::string> args) {
  args.insert(args.begin(), "dingo-cache");
  std::vector<char*> argv;
  argv.reserve(args.size() + 1);
  for (std::string& arg : args) {
    argv.push_back(arg.data());
  }
  argv.push_back(nullptr);
  int argc = static_cast<int>(args.size());
  char** rest = argv.data();

  SyncBthreadConcurrency();
  testing::internal::CaptureStdout();
  testing::internal::CaptureStderr();
  const std::optional<int> code = FlagParser::Parse(&argc, &rest, kUsage);
  std::string err = testing::internal::GetCapturedStderr();
  return {.code = code,
          .out = testing::internal::GetCapturedStdout(),
          .err = std::move(err),
          .rest = std::vector<std::string>(rest, rest + argc)};
}

static std::vector<std::string> GetNames(
    const std::vector<gflags::CommandLineFlagInfo>& flags) {
  std::vector<std::string> names;
  names.reserve(flags.size());
  for (const auto& flag : flags) {
    names.push_back(flag.name);
  }
  return names;
}

static const gflags::CommandLineFlagInfo* Find(
    const std::vector<gflags::CommandLineFlagInfo>& flags,
    std::string_view name) {
  const auto it =
      std::ranges::find(flags, name, &gflags::CommandLineFlagInfo::name);
  return it != flags.end() ? &*it : nullptr;
}

static std::string Plain(std::string text) {
  const std::string red = RedString("[required]");
  for (size_t pos = text.find(red); pos != std::string::npos;
       pos = text.find(red, pos)) {
    text.replace(pos, red.size(), "[required]");
  }
  return text;
}

static std::string MaskUuids(const std::string& text) {
  static const std::regex kUuid(
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
  return std::regex_replace(text, kUuid, "<uuid>");
}

TEST(FlagParserTest, Parse) {
  gflags::FlagSaver flag_saver;

  {
    const ParseResult result =
        Parse({"--shards=16", "--cpuset", "0-31", "--pin_cpu", "--nobind_all"});
    EXPECT_EQ(result.code, std::nullopt);
    EXPECT_TRUE(result.out.empty());
    EXPECT_EQ(FLAGS_shards, 16);
    EXPECT_EQ(FLAGS_cpuset, "0-31");
    EXPECT_TRUE(FLAGS_pin_cpu);
    EXPECT_FALSE(FLAGS_bind_all);
    EXPECT_EQ(result.rest, std::vector<std::string>{"dingo-cache"});
  }

  {
    const ParseResult result = Parse({"--v", "-1", "--"});
    EXPECT_EQ(result.code, std::nullopt);
    EXPECT_EQ(FLAGS_v, -1);
    EXPECT_EQ(result.rest, std::vector<std::string>{"dingo-cache"});
  }

  {
    TempDir dir;
    const std::string conf = (dir.path() / "cache.conf").string();
    std::ofstream(conf) << "# RUNTIME OPTIONS\n"
                           "--shards=4\n"
                           "  --cpuset=0-7\r\n"
                           "\n"
                           "--poll_mode=true\n";
    const std::vector<std::vector<std::string>> spellings = {
        {"--conf", conf, "--shards=2"},    {"--conf=" + conf, "--shards=2"},
        {"-conf", conf, "--shards=2"},     {"--shards=2", "--conf", conf},
        {"-flagfile", conf, "--shards=2"}, {"--flagfile=" + conf, "--shards=2"},
    };
    for (const auto& args : spellings) {
      gflags::FlagSaver flags_of_this_case;
      const ParseResult result = Parse(args);
      EXPECT_EQ(result.code, std::nullopt) << args[0];
      EXPECT_EQ(FLAGS_conf, conf);
      EXPECT_EQ(FLAGS_cpuset, "0-7");
      EXPECT_TRUE(FLAGS_poll_mode);
      EXPECT_EQ(FLAGS_shards, 2);
      EXPECT_EQ(result.rest, std::vector<std::string>{"dingo-cache"});
    }
  }

  {
    const ParseResult result = Parse({"--conf=", "--shards=3"});
    EXPECT_EQ(result.code, std::nullopt);
    EXPECT_TRUE(FLAGS_conf.empty());
    EXPECT_EQ(FLAGS_shards, 3);
  }

  {
    TempDir dir;
    const std::string first = (dir.path() / "first.conf").string();
    const std::string second = (dir.path() / "second.conf").string();
    std::ofstream(first) << "--shards=4\n--cpuset=0-7\n--pin_cpu=true\n";
    std::ofstream(second) << "--shards=6\n--cpuset=8-15\n";
    const std::vector<std::vector<std::string>> listings = {
        {"--conf", first, "--conf=" + second, "--cpuset=16-23"},
        {"--flagfile=" + first + "," + second, "--cpuset=16-23"},
        {"--conf=" + first + "," + second, "--cpuset=16-23"},
    };
    for (const auto& args : listings) {
      gflags::FlagSaver flags_of_this_case;
      const ParseResult result = Parse(args);
      EXPECT_EQ(result.code, std::nullopt) << args[0];
      EXPECT_EQ(FLAGS_shards, 6) << args[0];
      EXPECT_EQ(FLAGS_cpuset, "16-23") << args[0];
      EXPECT_TRUE(FLAGS_pin_cpu) << args[0];
    }
  }
}

TEST(FlagParserTest, ParseReportsMistakes) {
  struct Case {
    std::vector<std::string> args;
    std::string errors;
  };
  const Case cases[] = {
      {.args = {"--shard=4"},
       .errors = "ERROR: unknown flag '--shard' (did you mean --shards?)\n"},
      {.args = {"--zzzzzzzz"}, .errors = "ERROR: unknown flag '--zzzzzzzz'\n"},
      {.args = {"--pooll_mode"},
       .errors =
           "ERROR: unknown flag '--pooll_mode' (did you mean --poll_mode?)\n"},
      {.args = {"--no-pin-cpu"},
       .errors =
           "ERROR: unknown flag '--no-pin-cpu' (did you mean --nopin_cpu?)\n"},
      {.args = {"--PIN-CPU"},
       .errors = "ERROR: unknown flag '--PIN-CPU' (did you mean --pin_cpu?)\n"},
      {.args = {"--offload_cpu_min"},
       .errors = "ERROR: unknown flag '--offload_cpu_min' (did you mean "
                 "--offload_cpu_min_bytes or --offload_cpu_spin_us?)\n"},
      {.args = {"--rdma_max"},
       .errors = "ERROR: unknown flag '--rdma_max' (did you mean "
                 "--rdma_max_inline_data, --rdma_max_connections or "
                 "--rdma_max_inflight_rpcs?)\n"},
      {.args = {"--cpusets", "0-7"},
       .errors = "ERROR: unknown flag '--cpusets' (did you mean --cpuset?)\n"},
      {.args = {"cache.log"},
       .errors = "ERROR: unexpected argument 'cache.log'\n"},
      {.args = {"--pin_cpu", "false"},
       .errors = "ERROR: unexpected argument 'false' (did you mean "
                 "--pin_cpu=false?)\n"},
      {.args = {"shards=4"},
       .errors = "ERROR: unexpected argument 'shards=4' (did you mean "
                 "--shards=4?)\n"},
      {.args = {"--", "--shards=4"},
       .errors = "ERROR: unexpected argument '--shards=4'\n"},
      {.args = {"--pin_cpu false"},
       .errors = "ERROR: expected --name=value, got '--pin_cpu false' (did you "
                 "mean --pin_cpu=false?)\n"},
      {.args = {"h", "t", "version"},
       .errors = "ERROR: unexpected argument 'h'\n"
                 "ERROR: unexpected argument 't'\n"
                 "ERROR: unexpected argument 'version'\n"},
      {.args = {"--log_dir", "--pin_cpu"},
       .errors = "ERROR: flag '--log_dir' is missing its value\n"},
      {.args = {"--cpuset"},
       .errors = "ERROR: flag '--cpuset' is missing its value\n"},
      {.args = {"--conf", "--shards=4"},
       .errors = "ERROR: flag '--conf' is missing its value\n"},
      {.args = {"--nopin_cpu=false"},
       .errors = "ERROR: flag '--nopin_cpu' takes no value\n"},
      {.args = {"--shard=4", "false", "--cpuset"},
       .errors = "ERROR: unknown flag '--shard' (did you mean --shards?)\n"
                 "ERROR: unexpected argument 'false'\n"
                 "ERROR: flag '--cpuset' is missing its value\n"},
  };
  for (const Case& c : cases) {
    const ParseResult result = Parse(c.args);
    EXPECT_EQ(result.code, 1) << c.args[0];
    EXPECT_EQ(result.err, c.errors + kTryHelp) << c.args[0];
  }

  {
    const ParseResult result = Parse({"--disk_state"});
    EXPECT_THAT(result.err,
                HasSubstr("(did you mean --disk_state_tick_duration_s, "));
    EXPECT_THAT(result.err, Not(HasSubstr("--disk_state_check_duration_ms")));
  }
}

TEST(FlagParserTest, ParseReportsConfMistakes) {
  gflags::FlagSaver flag_saver;
  TempDir dir;

  {
    const std::string conf = (dir.path() / "cache.conf").string();
    std::ofstream(conf) << "# RUNTIME OPTIONS\n"
                           "--shards=4\n"
                           "shards=4\n"
                           "--cpusets=0-7\n"
                           "--poll_mode=true  # busy-poll\n"
                           "--cpuset\n"
                           "0-7\n"
                           "--pin_cpu false\n"
                           "--shards = 4\n"
                           "--conf=other.conf\n"
                           "\n"
                           "--\n";
    const std::string where = "ERROR: " + conf + ":";
    const std::string errors =
        where +
        "3: expected --name=value, got 'shards=4' (did you mean "
        "--shards=4?)\n" +
        where + "4: unknown flag '--cpusets' (did you mean --cpuset?)\n" +
        where + "5: a comment needs a line of its own\n" + where +
        "6: flag '--cpuset' is missing its value\n" + where +
        "7: expected --name=value, got '0-7'\n" + where +
        "8: expected --name=value, got '--pin_cpu false' (did you mean "
        "--pin_cpu=false?)\n" +
        where +
        "9: expected --name=value, got '--shards = 4' (did you mean "
        "--shards=4?)\n" +
        where + "10: --conf cannot be set in a conf file\n" + where +
        "12: expected --name=value, got '--'\n"
        "ERROR: unexpected argument 'cache.log'\n" +
        kTryHelp;
    for (const std::string& spelling : {"--conf", "-flagfile"}) {
      const ParseResult result = Parse({spelling, conf, "cache.log"});
      EXPECT_EQ(result.code, 1) << spelling;
      EXPECT_EQ(result.err, errors) << spelling;
    }
  }

  {
    const std::string conf = (dir.path() / "missing.conf").string();
    const ParseResult result = Parse({"--conf=" + conf});
    EXPECT_EQ(result.code, 1);
    EXPECT_EQ(result.err, "ERROR: cannot read conf file '" + conf +
                              "': No such file or directory\n" + kTryHelp);
  }

  {
    const ParseResult result = Parse({"--conf=" + dir.path().string()});
    EXPECT_EQ(result.code, 1);
    EXPECT_EQ(result.err, "ERROR: cannot read conf file '" +
                              dir.path().string() + "': Is a directory\n" +
                              kTryHelp);
  }
}

TEST(FlagParserTest, ParseVersionAndHelp) {
  gflags::FlagSaver flag_saver;
  const ScopedEnv columns("COLUMNS", nullptr);
  const std::string all = std::to_string(FlagParser::Collect(kUsage).size());
  const std::string version = DingoShortVersionString() + "\n";
  const std::string help =
      FlagParser::GenHelp(kUsage, FlagParser::CollectEssential(kUsage), 80) +
      "\n\nThis is the reduced help; -hh lists all " + all +
      " options, --help=WORD searches them.\n";
  const std::string full_help =
      FlagParser::GenHelp(kUsage, FlagParser::Collect(kUsage), 80) + "\n";
  const std::string search =
      FlagParser::GenHelp(kUsage, FlagParser::CollectMatching(kUsage, "shard"),
                          80) +
      "\n\nThese are the options matching 'shard'; -hh lists all " + all +
      " options.\n";
  const std::string no_match =
      FlagParser::GenHelp(kUsage, {}, 80) +
      "\n\nNo option matches 'zzzzzzzz'; -hh lists all " + all + " options.\n";
  const std::string tmpl =
      FlagParser::GenTemplate(kUsage, FlagParser::Collect(kUsage));

  {
    struct Case {
      std::vector<std::string> args;
      std::string out;
    };
    const Case cases[] = {
        {.args = {"--version"}, .out = version},
        {.args = {"-version"}, .out = version},
        {.args = {"-h"}, .out = help},
        {.args = {"--help"}, .out = help},
        {.args = {"--helpshort"}, .out = help},
        {.args = {"-hh"}, .out = full_help},
        {.args = {"--helpfull"}, .out = full_help},
        {.args = {"--helpon=cache"}, .out = full_help},
        {.args = {"--help=shard"}, .out = search},
        {.args = {"-help=zzzzzzzz"}, .out = no_match},
        {.args = {"-t"}, .out = tmpl},
        {.args = {"--tmpl"}, .out = tmpl},
    };
    for (const Case& c : cases) {
      const ParseResult result = Parse(c.args);
      EXPECT_EQ(result.code, 0) << c.args[0];
      EXPECT_EQ(MaskUuids(result.out), MaskUuids(c.out)) << c.args[0];
    }
  }

  {
    EXPECT_EQ(Parse({"--tmpl", "-hh", "--version"}).out, version);
    EXPECT_EQ(Parse({"--tmpl", "--help", "--help=shard", "-hh"}).out,
              full_help);
    EXPECT_EQ(Parse({"--tmpl", "--help", "--help=shard"}).out, search);
    EXPECT_EQ(Parse({"--tmpl", "-h"}).out, help);
  }

  {
    const ParseResult result = Parse({"--shards=3", "--version", "cache.log"});
    EXPECT_EQ(result.code, 0);
    EXPECT_NE(FLAGS_shards, 3);
    EXPECT_EQ(result.rest.size(), 4);
  }
}

TEST(FlagParserTest, ParseHelpWidth) {
  const auto full_help = [](size_t width) {
    return FlagParser::GenHelp(kUsage, FlagParser::Collect(kUsage), width) +
           "\n";
  };
  ASSERT_NE(full_help(60), full_help(100));

  const std::pair<const char*, size_t> cases[] = {
      {nullptr, 80}, {"", 80},   {"0", 80},  {"wide", 80},
      {"72x", 80},   {"72", 72}, {"20", 60}, {"500", 100}};
  for (const auto& [columns, width] : cases) {
    const ScopedEnv env("COLUMNS", columns);
    EXPECT_EQ(Parse({"-hh"}).out, full_help(width))
        << (columns != nullptr ? columns : "unset");
  }

  {
    const ScopedEnv env("COLUMNS", "60");
    EXPECT_EQ(
        Parse({"-h"}).out,
        FlagParser::GenHelp(kUsage, FlagParser::CollectEssential(kUsage), 60) +
            "\n\nThis is the reduced help; -hh lists all " +
            std::to_string(FlagParser::Collect(kUsage).size()) +
            " options,\n--help=WORD searches them.\n");
  }
}

TEST(FlagParserTest, SectionOf) {
  EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(kFlags, "shards")),
            "RUNTIME OPTIONS");
  EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(kFlags, "conf")),
            "NODE OPTIONS");
  EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(kFlags, "listen_port")),
            "NODE OPTIONS");
  EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(kFlags, "log_dir")),
            "OTHER OPTIONS");
  EXPECT_EQ(FlagParser::SectionOf(
                kUsage, {.name = "cache_dir",
                         .filename = "/dingofs/src/blockcache/store/disk.cc"}),
            "OTHER OPTIONS");

  {
    EXPECT_EQ(FlagParser::SectionOf(
                  kUsage, {.name = "v", .filename = "/glog/src/vlog_is_on.cc"}),
              "");
  }
}

TEST(FlagParserTest, Collect) {
  const std::vector<gflags::CommandLineFlagInfo> flags =
      FlagParser::Collect(kUsage);
  ASSERT_FALSE(flags.empty());

  {
    const auto rank = [](const gflags::CommandLineFlagInfo& flag) {
      const std::string_view section = FlagParser::SectionOf(kUsage, flag);
      return std::ranges::find(kUsage.sections, section, &FlagSection::name) -
             kUsage.sections.begin();
    };
    for (size_t i = 1; i < flags.size(); i++) {
      const auto& prev = flags[i - 1];
      const auto& flag = flags[i];
      EXPECT_TRUE(rank(prev) < rank(flag) ||
                  (rank(prev) == rank(flag) && prev.name < flag.name))
          << prev.name << " before " << flag.name;
    }
  }

  {
    for (const std::string_view name : {"shards", "cpuset", "pin_cpu"}) {
      const gflags::CommandLineFlagInfo* flag = Find(flags, name);
      ASSERT_NE(flag, nullptr) << name;
      EXPECT_EQ(FlagParser::SectionOf(kUsage, *flag), "RUNTIME OPTIONS");
    }
    ASSERT_NE(Find(flags, "conf"), nullptr);
    EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(flags, "conf")),
              "NODE OPTIONS");
    ASSERT_NE(Find(flags, "log_dir"), nullptr);
    EXPECT_EQ(FlagParser::SectionOf(kUsage, *Find(flags, "log_dir")),
              "OTHER OPTIONS");
  }

  {
    EXPECT_EQ(Find(flags, "v"), nullptr);
    EXPECT_EQ(Find(flags, "logtostderr"), nullptr);
    EXPECT_EQ(Find(flags, "flag_parser_test_undocumented"), nullptr);
    for (const auto& flag : flags) {
      EXPECT_FALSE(flag.description.empty()) << flag.name;
    }
  }
}

TEST(FlagParserTest, CollectEssential) {
  const std::vector<std::string> essential = {"cpuset", "shards", "conf",
                                              "log_dir"};
  EXPECT_EQ(GetNames(FlagParser::CollectEssential(kUsage)), essential);

  {
    FlagParser::Usage usage = kUsage;
    usage.essential.emplace_back("no_such_flag");
    EXPECT_EQ(GetNames(FlagParser::CollectEssential(usage)), essential);
  }
}

TEST(FlagParserTest, CollectMatching) {
  const std::vector<std::string> all = GetNames(FlagParser::Collect(kUsage));

  {
    const std::vector<std::string> names =
        GetNames(FlagParser::CollectMatching(kUsage, "SHARD"));
    EXPECT_THAT(names, Contains("shards"));
    EXPECT_THAT(names, Contains("cpuset"));
    EXPECT_THAT(names, Not(Contains("log_dir")));

    std::vector<size_t> ranks;
    ranks.reserve(names.size());
    for (const std::string& name : names) {
      ranks.push_back(std::ranges::find(all, name) - all.begin());
    }
    EXPECT_TRUE(std::ranges::is_sorted(ranks));
  }

  {
    std::vector<std::string> runtime;
    for (const auto& flag : FlagParser::Collect(kUsage)) {
      if (FlagParser::SectionOf(kUsage, flag) == "RUNTIME OPTIONS") {
        runtime.push_back(flag.name);
      }
    }
    ASSERT_FALSE(runtime.empty());
    EXPECT_EQ(GetNames(FlagParser::CollectMatching(kUsage, "runtime options")),
              runtime);
  }

  {
    EXPECT_EQ(GetNames(FlagParser::CollectMatching(kUsage, "options")), all);
    EXPECT_THAT(GetNames(FlagParser::CollectMatching(kUsage, "untime")),
                Not(Contains("shards")));
  }

  {
    EXPECT_TRUE(FlagParser::CollectMatching(kUsage, "zzzzzzzz").empty());
  }
}

TEST(FlagParserTest, GenHelp) {
  const std::string header = "dingo-cache " + DingoShortVersionString() +
                             "\n"
                             "\n"
                             "Usage:\n"
                             "  dingo-cache [OPTIONS] --id <uuid>\n"
                             "\n"
                             "Examples:\n"
                             "  $ dingo-cache "
                             "--id=044d4698-7bd4-4e44-9e94-aee6312ff06f\n"
                             "  $ dingo-cache --conf cache.conf\n";
  const std::string footer =
      "\n"
      "dingo-cache was written by dingofs team "
      "<https://github.com/dingodb/dingofs>";

  {
    EXPECT_EQ(
        Plain(FlagParser::GenHelp(kUsage, kFlags, 80)),
        header +
            "\n"
            "RUNTIME OPTIONS:\n"
            "  --shards=uint32       reactor shards, 0 means one per core "
            "(default: 8)\n"
            "\n"
            "NODE OPTIONS:\n"
            "  --conf=string         config file path\n"
            "  --id=string           cache node id [required]\n"
            "  --listen_port=uint32  port to listen on (default: 9300)\n"
            "\n"
            "OTHER OPTIONS:\n"
            "  --log_dir=string      log directory\n" +
            footer);
  }

  {
    FlagParser::Usage usage = kUsage;
    usage.required = {"listen_port"};
    const std::string help = Plain(FlagParser::GenHelp(usage, kFlags, 80));
    EXPECT_THAT(
        help,
        HasSubstr("  --listen_port=uint32  port to listen on [required]\n"));
    EXPECT_THAT(help, HasSubstr("  --id=string           cache node id\n"));
  }

  {
    EXPECT_THAT(FlagParser::GenHelp(kUsage, {kRatioFlag}, 80),
                HasSubstr("  --free_space_ratio=double  min free space "
                          "(ratio) (default: 0.1)\n"));
  }

  {
    EXPECT_EQ(FlagParser::GenHelp(kUsage, {}, 80), header + footer);
  }
}

TEST(FlagParserTest, GenHelpWraps) {
  const std::string core = "/dingofs/src/blockcache/core/runtime/runtime.cc";
  const std::string node = "/dingofs/src/blockcache/node/node.cc";
  const std::vector<gflags::CommandLineFlagInfo> flags = {
      {.name = "idle_poll_us",
       .type = "uint32",
       .description =
           "microseconds a reactor spins on an empty queue before it sleeps",
       .default_value = "200",
       .filename = core},
      {.name = "offload_queue_capacity_limit",
       .type = "uint32",
       .description = "queued jobs beyond limits make submits answer busy",
       .default_value = "4096",
       .filename = core},
      {.name = "id",
       .type = "string",
       .description = "cache node uuid",
       .filename = node},
      {.name = "path",
       .type = "string",
       .description = "where blocks go:\nfirst the dir,\n\nthen the file",
       .default_value = "/a/default/path/far/too/long/for/one/line",
       .filename = node},
      {.name = "log_dir",
       .type = "string",
       .description = "log directory",
       .filename = "/glog/src/flags.cc"},
  };
  const std::string help = Plain(FlagParser::GenHelp(kUsage, flags, 60));

  const auto line = [](std::string head, std::string_view text) {
    head.resize(34, ' ');
    return head + std::string(text) + "\n";
  };
  const size_t begin = help.find("RUNTIME OPTIONS:");
  ASSERT_NE(begin, std::string::npos);
  EXPECT_EQ(
      help.substr(begin, help.rfind("\n\n") + 1 - begin),
      "RUNTIME OPTIONS:\n" +
          line("  --idle_poll_us=uint32", "microseconds a reactor") +
          line("", "spins on an empty queue") + line("", "before it sleeps") +
          line("", "(default: 200)") +
          "  --offload_queue_capacity_limit=uint32\n" +
          line("", "queued jobs beyond limits") +
          line("", "make submits answer busy") + line("", "(default: 4096)") +
          "\n"
          "NODE OPTIONS:\n" +
          line("  --id=string", "cache node uuid") + line("", "[required]") +
          line("  --path=string", "where blocks go:") +
          line("", "first the dir,") + "\n" + line("", "then the file") +
          line("", "(default: /a/default/path/far/too/long/for/one/line)") +
          "\n"
          "OTHER OPTIONS:\n" +
          line("  --log_dir=string", "log directory"));
}

TEST(FlagParserTest, GenTemplate) {
  {
    const std::string tmpl = FlagParser::GenTemplate(kUsage, kFlags);
    EXPECT_EQ(MaskUuids(tmpl),
              "# RUNTIME OPTIONS\n"
              "--shards=8\n"
              "\n"
              "# NODE OPTIONS\n"
              "--id=<uuid>\n"
              "--listen_port=9300\n"
              "\n"
              "# OTHER OPTIONS\n"
              "--log_dir=\n");
    EXPECT_NE(MaskUuids(tmpl), tmpl);
    EXPECT_NE(FlagParser::GenTemplate(kUsage, kFlags), tmpl);
  }

  {
    FlagParser::Usage usage = kUsage;
    usage.uuid_flag = "";
    EXPECT_EQ(FlagParser::GenTemplate(usage, kFlags),
              "# RUNTIME OPTIONS\n"
              "--shards=8\n"
              "\n"
              "# NODE OPTIONS\n"
              "# --id=\n"
              "--listen_port=9300\n"
              "\n"
              "# OTHER OPTIONS\n"
              "--log_dir=\n");
  }

  {
    EXPECT_EQ(FlagParser::GenTemplate(kUsage, {kRatioFlag}),
              "# OTHER OPTIONS\n"
              "--free_space_ratio=0.1\n");
  }

  {
    EXPECT_EQ(FlagParser::GenTemplate(kUsage, {*Find(kFlags, "conf")}), "");
  }
}

TEST(FlagParserTest, GenCurrentFlags) {
  EXPECT_EQ(FlagParser::GenCurrentFlags(kFlags),
            "Current flags:\n"
            "  --shards=16\n"
            "  --conf=/etc/dingofs/cache.conf\n"
            "  --id=044d4698-7bd4-4e44-9e94-aee6312ff06f\n"
            "  --listen_port=9301\n"
            "  --log_dir=/var/log/dingofs\n");
  EXPECT_EQ(FlagParser::GenCurrentFlags({}), "Current flags:\n");

  {
    EXPECT_EQ(
        FlagParser::GenCurrentFlags({kRatioFlag,
                                     {.name = "ratio_text",
                                      .type = "string",
                                      .current_value = "0.10000000000000001"}}),
        "Current flags:\n"
        "  --free_space_ratio=0.30000000000000004\n"
        "  --ratio_text=0.10000000000000001\n");
  }
}

}  // namespace blockcache
}  // namespace dingofs
