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

#include "cache/v2/common/flag_decls.h"
#include "cache/v2/core/memory/buffer.h"
#include "cache/v2/core/runtime/runtime.h"
#include "cache/v2/core/runtime/worker_pool.h"
#include "cache/v2/node/cli.h"
#include "cache/v2/node/node.h"
#include "cache/v2/utils/flags.h"
#include "common/logging.h"
#include "utils/daemonize.h"

using dingofs::Logger;
using dingofs::Status;
using dingofs::cache::v2::BufferPool;
using dingofs::cache::v2::CacheNode;
using dingofs::cache::v2::FlagParser;
using dingofs::cache::v2::FLAGS_cpuset;
using dingofs::cache::v2::FLAGS_daemonize;
using dingofs::cache::v2::FLAGS_poll_mode;
using dingofs::cache::v2::FLAGS_shards;
using dingofs::cache::v2::kUsage;
using dingofs::cache::v2::Runtime;
using dingofs::cache::v2::RuntimeOption;
using dingofs::cache::v2::RuntimeUPtr;
using dingofs::cache::v2::WorkerPool;
using dingofs::cache::v2::WorkerPoolUPtr;
using dingofs::utils::DaemonizeExec;

static RuntimeUPtr& GetGlobalRuntime() {
  static RuntimeUPtr runtime;
  return runtime;
}

static WorkerPoolUPtr& GetGlobalWorkerPool() {
  static WorkerPoolUPtr workers;
  return workers;
}

static void GlobalInitOrDie() {
  RuntimeOption option;
  option.shard_count = FLAGS_shards;
  option.cpuset = FLAGS_cpuset;
  option.reactor.poll_mode = FLAGS_poll_mode;
  GetGlobalRuntime() = std::make_unique<Runtime>(option);
  GetGlobalRuntime()->Start();

  GetGlobalWorkerPool() = std::make_unique<WorkerPool>();
  GetGlobalWorkerPool()->Start();
}

static void GlobalShutdown() {
  GetGlobalWorkerPool()->Shutdown();

  BufferPool::ShutdownOnAllShards();

  GetGlobalRuntime()->Shutdown();
  GetGlobalRuntime()->Join();
}

static bool ParseOptions(int argc, char** argv) {
  return FlagParser::Parse(&argc, &argv, kUsage);
}

static bool Daemonize(const std::vector<std::string>& args) {
  if (!FLAGS_daemonize) {
    return true;
  }
  return DaemonizeExec(args);
}

static void InitLogger() {
  Logger::Init("dingo-cache");
  LOG(INFO) << FlagParser::GenCurrentFlags(FlagParser::Collect(kUsage));
}

static bool RunNode() {
  CacheNode node;
  const Status status = node.Start();
  if (!status.ok()) {
    LOG(ERROR) << "Fail to start cache node: " << status.ToString();
    return false;
  }

  node.RunUntilAskedToQuit();
  node.Shutdown();
  return true;
}

int main(int argc, char** argv) {
  if (!ParseOptions(argc, argv)) {
    return 0;
  } else if (!Daemonize({argv + 1, argv + argc})) {
    return -1;
  }

  InitLogger();
  GlobalInitOrDie();
  bool succ = RunNode();
  GlobalShutdown();

  return succ ? 0 : -1;
}
