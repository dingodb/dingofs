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
 * Created Date: 2026-09-23
 * Author: AI
 */

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <vector>

#include "common/options/cache.h"
#include "test/unit/blockcache/base/coverage.h"

static void InitLog(const std::string& log_dir) {
  std::filesystem::create_directories(log_dir);

  FLAGS_logbufsecs = 10;
  FLAGS_stop_logging_if_full_disk = true;
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logbuflevel = google::GLOG_INFO;
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
  FLAGS_alsologtostderr = false;

  const std::string program_name = "blockcache_unit_test";

  google::InitGoogleLogging(program_name.c_str());
  google::SetLogDestination(
      google::GLOG_INFO,
      fmt::format("{}/{}.info.log", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_WARNING,
      fmt::format("{}/{}.warn.log", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_ERROR,
      fmt::format("{}/{}.error.log", log_dir, program_name).c_str());
  google::SetLogDestination(
      google::GLOG_FATAL,
      fmt::format("{}/{}.fatal.log", log_dir, program_name).c_str());
  google::SetStderrLogging(google::GLOG_FATAL);
}

int main(int argc, char** argv) {
  InitLog("./log");

  // gtest and gflags take the flags they know out of argv, so the coverage
  // child gets this copy: the tests it runs are the ones asked for.
  std::vector<char*> command_line(argv, argv + argc);

  testing::InitGoogleTest(&argc, argv);

  dingofs::blockcache::FLAGS_id = "8d0e9a52-3f4b-4c1e-a7d6-2b9c8e5f1a03";
  dingofs::blockcache::FLAGS_listen_ip = "127.0.0.1";
  google::ParseCommandLineFlags(&argc, &argv, true);

  return dingofs::blockcache::RunTestsWithCoverage(
      {.target_name = "test_blockcache",
       .source_filter = "src/blockcache/",
       .excludes = {"src/blockcache/tools/.*", "src/blockcache/node/main.cc"},
       .test_dir = "test/unit/blockcache/"},
      static_cast<int>(command_line.size()), command_line.data(),
      [] { return RUN_ALL_TESTS(); });
}
