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

#include <glog/logging.h>

#include <memory>
#include <string>
#include <vector>

#include "blockcache/core/runtime/bootstrap.h"
#include "blockcache/node/cli.h"
#include "blockcache/node/node.h"
#include "blockcache/utils/daemon.h"
#include "blockcache/utils/flags.h"
#include "common/logging.h"
#include "common/options/cache.h"

using dingofs::Logger;
using dingofs::Status;
using dingofs::blockcache::CacheNode;
using dingofs::blockcache::DaemonizeAndWait;
using dingofs::blockcache::FlagParser;
using dingofs::blockcache::FLAGS_daemonize;
using dingofs::blockcache::kNodeUsage;
using dingofs::blockcache::ReportDaemonReady;
using dingofs::blockcache::StartProcessRuntime;
using dingofs::blockcache::StopProcessRuntime;

static bool ParseOptions(int argc, char** argv) {
  return FlagParser::Parse(&argc, &argv, kNodeUsage);
}

static void InitLogger() {
  Logger::Init("dingo-cache");
  LOG(INFO) << FlagParser::GenCurrentFlags(FlagParser::Collect(kNodeUsage));
}

static bool RunNode() {
  CacheNode node;
  const Status status = node.Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start cache node: " << status.ToString();
    return false;
  }

  ReportDaemonReady();
  node.RunUntilAskedToQuit();
  node.Shutdown();
  return true;
}

int main(int argc, char** argv) {
  const std::vector<std::string> original_args(argv + 1, argv + argc);
  if (!ParseOptions(argc, argv)) {
    return 0;
  }
  if (FLAGS_daemonize) {
    return DaemonizeAndWait(original_args);
  }

  InitLogger();
  StartProcessRuntime();
  bool succ = RunNode();
  StopProcessRuntime();

  return succ ? 0 : -1;
}
