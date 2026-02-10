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

#include "cache/cachegroup/server.h"
#include "common/flag.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/options/cache.h"
#include "common/options/common.h"
#include "utils/daemonize.h"

namespace dingofs {
namespace cache {

static dingofs::FlagExtraInfo extras = {
    .program = "dingo-cache",
    .usage = "  dingo-cache [OPTIONS] --id <cache_node_uuid>",
    .examples =
        R"(  $ dingo-cache --id=85a4b352-4097-4868-9cd6-9ec5e53db1b6
  $ dingo-cache --conf cache.conf --daemonize
)",
    .patterns = {"src/cache", "options/common"},
};

int DingoCache::ParseFlags(int argc, char** argv) {
  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) {
    return -1;
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

  // TODO: so ugly implementation :(
  // refactor ASAP!!!

  // reset brpc flag default value if not set
  dingofs::ResetBrpcFlagDefaultValue();

  // print config info
  std::vector<std::pair<std::string, std::string>> configs;
  configs.emplace_back("id", fmt::format("[{}]", FLAGS_id));
  // config
  configs.emplace_back("config", fmt::format("[{}]", dingofs::FLAGS_conf));
  // log
  configs.emplace_back(
      "log", fmt::format("[{} {} {}(verbose)]",
                         ::FLAGS_log_dir.empty() ? GetDefaultDir(kLogDir)
                                                 : ::FLAGS_log_dir,
                         dingofs::FLAGS_log_level, dingofs::FLAGS_log_v));
  // mds
  configs.emplace_back("mds", fmt::format("[{}]", FLAGS_mds_addrs));
  // cache
  if (dingofs::cache::FLAGS_enable_cache) {
    configs.emplace_back("cache",
                         fmt::format("[{} {} {}%(ratio)]", FLAGS_cache_store,
                                     Helper::GenCacheConfigInfo(),
                                     FLAGS_free_space_ratio * 100));
  }

  Helper::PrintConfigInfo(configs);

  return 0;
}

void DingoCache::GlobalInitOrDie() {
  Logger::Init("dingo-cache");
  LOG(INFO) << GenCurrentFlags();
}

int DingoCache::StartServer() {
  Server server;
  auto status = server.Start();
  if (!status.ok()) {
    return -1;
  }

  server.Shutdown();
  return 0;
}

int DingoCache::Run(int argc, char** argv) {
  std::vector<std::string> orig_args;
  orig_args.reserve(argc);
  for (int i = 1; i < argc; ++i) {
    orig_args.emplace_back(argv[i]);
  }

  int rc = ParseFlags(argc, argv);
  if (rc != 0) {
    return rc;
  }

  GlobalInitOrDie();

  // run in daemon mode
  if (dingofs::FLAGS_daemonize) {
    if (!dingofs::utils::DaemonizeExec(orig_args)) {
      std::cerr << "failed to daemonize process.\n";
      return -1;
    }
  }

  return StartServer();
}

}  // namespace cache
}  // namespace dingofs
