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

#include <gflags/gflags.h>

#include <csignal>
#include <cstdlib>
#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "client/fuse/fuse_op.h"
#include "client/fuse/fuse_server.h"
#include "common/blockaccess/accesser_common.h"
#include "common/flag.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/options/common.h"
#include "common/types.h"
#include "fmt/format.h"
#include "utils/daemonize.h"

using FuseServer = dingofs::client::fuse::FuseServer;

static FuseServer* fuse_server = nullptr;

// signal handler
static void HandleSignal(int sig) {
  printf("received signal %d, exit...\n", sig);
  if (sig == SIGHUP && fuse_server != nullptr) {
    fuse_server->Shutdown();
  }
}

static int InstallSignal(int sig, void (*handler)(int)) {
  struct sigaction sa;

  memset(&sa, 0, sizeof(struct sigaction));
  sa.sa_handler = handler;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = 0;

  if (sigaction(sig, &sa, nullptr) == -1) {
    perror("dingo-client: cannot set signal handler.");
    return -1;
  }

  return 0;
}

static std::vector<std::pair<std::string, std::string>> GenConfigs(
    const std::string& meta,
    const dingofs::blockaccess::BlockAccessOptions& options) {
  using dingofs::blockaccess::AccesserType;

  std::vector<std::pair<std::string, std::string>> configs;

  // config
  configs.emplace_back("config", fmt::format("[{}]", dingofs::FLAGS_conf));
  // log
  configs.emplace_back(
      "log", fmt::format("[{} {} {}(verbose)]",
                         dingofs::Helper::ExpandPath(
                             ::FLAGS_log_dir.empty() ? dingofs::kDefaultLogDir
                                                     : ::FLAGS_log_dir),
                         dingofs::FLAGS_log_level, dingofs::FLAGS_log_v));
  // meta
  configs.emplace_back("meta", fmt::format("[{}]", meta));
  // storage
  if (options.type == AccesserType::kS3) {
    configs.emplace_back("storage",
                         fmt::format("[s3://{}/{}]",
                                     dingofs::Helper::RemoveHttpPrefix(
                                         options.s3_options.s3_info.endpoint),
                                     options.s3_options.s3_info.bucket_name));

  } else if (options.type == AccesserType::kRados) {
    configs.emplace_back(
        "storage",
        fmt::format("[rados://{}/{}]", options.rados_options.mon_host,
                    options.rados_options.pool_name));

  } else if (options.type == AccesserType::kLocalFile) {
    configs.emplace_back(
        "storage", fmt::format("[local://{}]", options.file_options.path));
  }
  // cache
  if (!dingofs::cache::FLAGS_cache_group.empty()) {
    configs.emplace_back("cache",
                         fmt::format("[{} {}]", dingofs::cache::FLAGS_mds_addrs,
                                     dingofs::cache::FLAGS_cache_group));
  } else {
    if (dingofs::cache::FLAGS_enable_cache) {
      configs.emplace_back(
          "cache",
          fmt::format("[{} {}MB {}%(ratio)]", dingofs::cache::FLAGS_cache_dir,
                      dingofs::cache::FLAGS_cache_size_mb,
                      dingofs::cache::FLAGS_free_space_ratio * 100));
    }
  }
  // monitor
  auto hostname = dingofs::Helper::GetHostName();
  configs.emplace_back(
      "monitor",
      fmt::format("[{}:{}]", dingofs::Helper::GetIpByHostName(hostname),
                  dingofs::client::FLAGS_vfs_dummy_server_port));

  return configs;
}

static dingofs::FlagExtraInfo extras = {
    .program = "dingo-client",
    .usage = "  dingo-client [OPTIONS] <meta-url> <mountpoint>",
    .examples =
        R"(  $ dingo-client local://myfs /mnt/dingofs
  $ dingo-client mds://10.220.69.10:7400/myfs /mnt/dingofs
  $ dingo-client --conf client.conf mds://10.220.32.1:6700/myfs /mnt/dingofs
)",
    .patterns = {"src/client", "cache/common", "cache/storage",
                 "cache/tiercache", "cache/blockcache", "cache/remotecache",
                 "options/blockaccess", "options/client", "options/common"},
};

int main(int argc, char* argv[]) {
  // install singal handler
  InstallSignal(SIGHUP, HandleSignal);

  //  parse gflags
  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) {
    return EXIT_FAILURE;
  }

  // read gflags from conf file
  if (!dingofs::FLAGS_conf.empty()) {
    CHECK(dingofs::Helper::IsExistPath(dingofs::FLAGS_conf))
        << fmt::format("config file {} not exist.", dingofs::FLAGS_conf);
    gflags::ReadFromFlagsFile(dingofs::FLAGS_conf, argv[0], true);
  }

  // reset brpc flag default value if not set
  dingofs::ResetBrpcFlagDefaultValue();

  // after parsing:
  // argv[0] is program name
  // argv[1] is meta url
  // argv[2] is mount point
  if (argc < 3) {
    std::cerr << "missing meta url or mount point.\n";
    std::cerr << "Usage: " << extras.usage << '\n';
    std::cerr << "\n";
    std::cerr << "Examples:\n" << extras.examples << '\n';

    std::cerr << "For more help see: dingo-client --help\n";
    return EXIT_FAILURE;
  }

  dingofs::MetaSystemType metasystem_type;
  std::string mds_addrs;
  std::string fs_name;
  std::string storage_info;
  if (!dingofs::Helper::ParseMetaURL(argv[1], metasystem_type, mds_addrs,
                                     fs_name, storage_info)) {
    std::cerr << "meta url is invalid: " << argv[1] << '\n';
    return EXIT_FAILURE;
  }
  if (fs_name.empty()) {
    std::cerr << "meta url is invalid, missing fs name. \n";
    std::cerr << "Usage: " << extras.usage << '\n';
    std::cerr << "\n";
    std::cerr << "Examples:\n" << extras.examples << '\n';

    return EXIT_FAILURE;
  }

  // used for remote cache
  dingofs::cache::FLAGS_mds_addrs = mds_addrs;

  // run in daemon mode
  if (dingofs::FLAGS_daemonize && !dingofs::utils::Daemonize(false, true)) {
    std::cerr << "failed to daemonize process.\n";
    return EXIT_FAILURE;
  }

  struct MountOption mount_option{.mount_point = argv[2],
                                  .fs_name = fs_name,
                                  .metasystem_type = metasystem_type,
                                  .mds_addrs = mds_addrs,
                                  .storage_info = storage_info};

  fuse_server = new FuseServer();
  if (fuse_server == nullptr) return EXIT_FAILURE;
  auto defer_free = ::absl::MakeCleanup([&]() { delete fuse_server; });

  // init fuse
  if (fuse_server->Init(argv[0], &mount_option) == 1) return EXIT_FAILURE;
  auto defer_uninit = ::absl::MakeCleanup([&]() { UnInitFuseClient(); });

  // init global log
  dingofs::Logger::Init("dingo-client");

  // print current gflags
  LOG(INFO) << dingofs::GenCurrentFlags();

  // create fuse session
  if (fuse_server->CreateSession() == 1) return EXIT_FAILURE;
  auto defer_destory =
      ::absl::MakeCleanup([&]() { fuse_server->DestroySsesion(); });

  // mount filesystem
  if (fuse_server->SessionMount() == 1) return EXIT_FAILURE;
  auto defer_unmount =
      ::absl::MakeCleanup([&]() { fuse_server->SessionUnmount(); });

  // init fuse client
  dingofs::blockaccess::BlockAccessOptions block_options;
  if (InitFuseClient(argv[0], &mount_option, &block_options) != 0) {
    LOG(ERROR) << "init fuse client fail";
    return EXIT_FAILURE;
  }

  // print config info
  dingofs::Helper::PrintConfigInfo(GenConfigs(argv[1], block_options));

  std::cout << fmt::format("\n{} is ready at {}\n\n", mount_option.fs_name,
                           mount_option.mount_point);

  if (fuse_server->Serve() == 1) return EXIT_FAILURE;

  return EXIT_SUCCESS;
}
