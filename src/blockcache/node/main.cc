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

#include <fmt/format.h>
#include <glog/logging.h>
#include <unistd.h>

#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <string>
#include <system_error>
#include <vector>

#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/node/cli.h"
#include "blockcache/node/node.h"
#include "blockcache/utils/daemonize.h"
#include "blockcache/utils/flags.h"
#include "common/blockaccess/block_access_log.h"
#include "common/logging.h"
#include "common/options/cache.h"

using dingofs::Logger;
using dingofs::Status;
using dingofs::blockaccess::InitBlockAccessLog;
using dingofs::blockcache::CacheNode;
using dingofs::blockcache::Detach;
using dingofs::blockcache::FlagParser;
using dingofs::blockcache::FLAGS_daemonize;
using dingofs::blockcache::kNodeUsage;
using dingofs::blockcache::NotifyReady;
using dingofs::blockcache::StartDaemon;
using dingofs::blockcache::StartProcessRuntime;
using dingofs::blockcache::StopProcessRuntime;
using dingofs::blockcache::TakeReadyFd;

static std::optional<int> ParseOptions(int argc, char** argv) {
  return FlagParser::Parse(&argc, &argv, kNodeUsage);
}

static void InitLogger() {
  Logger::Init("dingo-cache");
  LOG(INFO) << FlagParser::GenCurrentFlags(FlagParser::Collect(kNodeUsage));

  CHECK(InitBlockAccessLog(FLAGS_log_dir)) << "Fail to init block access log";
}

static int Daemonize(std::vector<std::string> args) {
  std::error_code ec;
  args[0] = std::filesystem::read_symlink("/proc/self/exe", ec).string();
  if (ec) {
    std::cerr << "Fail to read /proc/self/exe: " << ec.message() << "\n";
    return 1;
  }
  return StartDaemon(args);
}

static bool RunNode(int ready_fd) {
  CacheNode node;
  const Status status = node.Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start cache node, see the logs in "
               << Logger::LogDir();
    return false;
  }

  if (FLAGS_daemonize &&
      !Detach(fmt::format("{}/{}.stdout", Logger::LogDir(), getpid()))) {
    return false;
  }
  NotifyReady(ready_fd);
  node.RunUntilAskedToQuit();
  node.Shutdown();
  return true;
}

int main(int argc, char** argv) {
  const int ready_fd = TakeReadyFd();
  const std::vector<std::string> args(argv, argv + argc);
  if (const std::optional<int> code = ParseOptions(argc, argv)) {
    return *code;
  }
  if (FLAGS_daemonize && ready_fd < 0) {
    return Daemonize(args);
  }

  InitLogger();
  const Status status = StartProcessRuntime();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start the process runtime: " << status.ToString();
    return -1;
  }
  bool succ = RunNode(ready_fd);
  StopProcessRuntime();

  return succ ? 0 : -1;
}
