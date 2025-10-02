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
 * Created Date: 2025-06-08
 * Author: Jingli Chen (Wine93)
 */

#include "cache/dingo_cache.h"

#include <iostream>

#include "cache/cachegroup/cache_group_node_server.h"
#include "cache/common/flag.h"
#include "cache/common/version.h"
#include "cache/utils/logging.h"
#include "cache/utils/offload_thread_pool.h"
#include "options/cache/option.h"

namespace dingofs {
namespace cache {

FlagsInfo DingoCache::flags;

int DingoCache::ParseFlags(int argc, char** argv) {
  flags = FlagsHelper::Parse(argc, argv);
  if (flags.show_help) {
    std::cout << FlagsHelper::GenHelp(flags) << "\n";
    return 1;
  } else if (flags.show_version) {
    std::cout << Version() << "\n";
    return 1;
  } else if (flags.create_template) {
    std::cout << FlagsHelper::GenTemplate(flags);
    return 1;
  }

  // validate flags
  if (FLAGS_mds_addrs.empty()) {
    std::cerr << "mds_addrs is empty, please set it by --mds_addrs\n";
    return -1;
  } else if (FLAGS_cache_store != "disk") {
    std::cerr
        << "MUST using disk cache store, please set it by --cache_store\n";
    return -1;
  } else if (!FLAGS_enable_stage) {
    std::cerr << "MUST enable stage, please set it by --enable_stage\n";
    return -1;
  } else if (!FLAGS_enable_cache) {
    std::cerr << "MUST enable cache, please set it by --enable_cache\n";
    return -1;
  }
  return 0;
}

void DingoCache::InitGlog() { InitLogging("dingo-cache"); }

void DingoCache::LogFlags() {
  LOG(INFO) << FlagsHelper::GenCurrentFlags(flags);
}

void DingoCache::InitThreadPool() { OffloadThreadPool::GetInstance().Start(); }

void DingoCache::GlobalInitOrDie() {
  InitGlog();
  LogFlags();
  InitThreadPool();
}

int DingoCache::StartServer() {
  CacheGroupNodeServerImpl server;
  auto status = server.Start();
  if (!status.ok()) {
    return -1;
  }

  server.Shutdown();
  return 0;
}

int DingoCache::Run(int argc, char** argv) {
  int rc = ParseFlags(argc, argv);
  if (rc != 0) {
    return rc;
  }

  GlobalInitOrDie();
  return StartServer();
}

}  // namespace cache
}  // namespace dingofs
