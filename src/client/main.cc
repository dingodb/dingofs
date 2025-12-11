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

#include <csignal>
#include <cstdlib>

#include "absl/cleanup/cleanup.h"
#include "client/common/global_log.h"
#include "client/fuse/fuse_op.h"
#include "client/fuse/fuse_server.h"
#include "common/flag.h"
#include "common/helper.h"
#include "common/options/cache.h"
#include "common/options/client.h"

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
    perror("dingo-fuse: cannot set signal handler.");
    return -1;
  }

  return 0;
}

static dingofs::FlagExtraInfo extras = {
    .program = "dingo-fuse",
    .usage = "dingo-fuse [OPTIONS] <meta-url> <mountpoint>",
    .examples =
        R"(dingo-fuse 10.220.69.10:7400/dingofs /mnt/dingofs
dingo-fuse --client_log_dir=/mnt/logs 10.220.32.1:6700/dingofs /mnt/dingofs
dingo-fuse --flagfile client.conf 10.220.32.1:6700/dingofs /mnt/dingofs
)",
    .patterns = {"src/client", "cache/tiercache", "cache/blockcache",
                 "cache/remotecache", "options/blockaccess", "options/client"},
};

int main(int argc, char* argv[]) {
  // install singal handler
  InstallSignal(SIGHUP, HandleSignal);

  // parse gflags
  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) {
    return EXIT_FAILURE;
  }

  // after parsing:
  // argv[0] is program name
  // argv[1] is meta url
  // argv[2] is mount point
  if (argc < 3) {
    std::cerr << "missing meta url or mount point.\n";
    std::cerr << "Usage: " << extras.usage << '\n';
    return EXIT_FAILURE;
  }

  std::string mds_addrs;
  std::string fs_name;
  if (!dingofs::Helper::ParseMetaURL(argv[1], mds_addrs, fs_name)) {
    std::cerr << "meta url is invalid: " << argv[1] << '\n';
    return EXIT_FAILURE;
  }
  // used for remote cache
  dingofs::cache::FLAGS_mds_addrs = mds_addrs;

  struct MountOption mount_option{.mount_point = argv[2],
                                  .fs_name = fs_name,
                                  .fs_type = dingofs::client::FLAGS_fstype,
                                  .mds_addrs = mds_addrs};

  fuse_server = new FuseServer();
  if (fuse_server == nullptr) return EXIT_FAILURE;
  auto defer_free = ::absl::MakeCleanup([&]() { delete fuse_server; });

  // init fuse
  if (fuse_server->Init(argv[0], &mount_option) == 1) return EXIT_FAILURE;
  auto defer_uninit = ::absl::MakeCleanup([&]() { UnInitFuseClient(); });

  // init global log
  InitLog(argv[0]);

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
  if (InitFuseClient(argv[0], &mount_option) != 0) {
    LOG(ERROR) << "init fuse client fail";
    return EXIT_FAILURE;
  }

  if (fuse_server->Serve() == 1) return EXIT_FAILURE;

  return EXIT_SUCCESS;
}
