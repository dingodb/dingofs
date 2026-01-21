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

#include <glog/logging.h>

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>

#include "absl/cleanup/cleanup.h"
#include "client/fuse/fs_context.h"
#include "client/fuse/fuse_server.h"
#include "common/flag.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/options/cache.h"
#include "common/options/common.h"
#include "common/types.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "utils/daemonize.h"

using FuseServer = dingofs::client::fuse::FuseServer;
using FsContext = dingofs::client::fuse::FsContext;

static FuseServer* fuse_server = nullptr;

// signal handler
static void HandleSignal(int sig) {
  printf("received signal %s, exit...\n", strsignal(sig));
  CHECK(signal(sig, SIG_DFL) != nullptr);

  if (fuse_server == nullptr) {
    return;
  }

  if (sig == SIGHUP) {
    fuse_server->MarkThenShutdown();
  }
  if (sig == SIGSEGV || sig == SIGABRT) {
    fuse_server->SessionUnmount();
    CHECK(raise(sig) == 0);
  }

  // flush log
  dingofs::Logger::FlushLogs();
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
  std::vector<std::string> orig_args;
  orig_args.reserve(argc);
  for (int i = 1; i < argc; ++i) {
    orig_args.emplace_back(argv[i]);
  }

  // install singal handler
  InstallSignal(SIGHUP, HandleSignal);
  InstallSignal(SIGSEGV, HandleSignal);
  InstallSignal(SIGABRT, HandleSignal);

  //  parse gflags
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
    std::cerr << "\n";
    std::cerr << "Examples:\n" << extras.examples << '\n';

    std::cerr << "For more help see: dingo-client --help\n";
    return EXIT_FAILURE;
  }

  // check mountpoint path is accessable
  const char* orig_mountpoint = argv[2];
  struct stat sb;
  if (stat(orig_mountpoint, &sb) == -1) {
    if (errno == ENOTCONN) {  // mountpoint not umount last time
      std::cout << fmt::format(
          "{} cleaning mountpoint: {}, last time may not unmount normally.",
          dingofs::client::RedString("WARNING:"), orig_mountpoint);
      CHECK(dingofs::client::Umount(orig_mountpoint));
    } else {
      std::cerr << fmt::format("can't stat {}, errmsg: {}\n", orig_mountpoint,
                               strerror(errno));
      return EXIT_FAILURE;
    }
  }

  std::string mountpoint = dingofs::Helper::ToCanonicalPath(orig_mountpoint);
  if (mountpoint == "/") {
    std::cerr << "can not mount on the root directory\n";
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

  // read gflags from conf file
  if (!dingofs::FLAGS_conf.empty()) {
    CHECK(dingofs::Helper::IsExistPath(dingofs::FLAGS_conf))
        << fmt::format("config file {} not exist.", dingofs::FLAGS_conf);
    gflags::ReadFromFlagsFile(dingofs::FLAGS_conf, argv[0], true);
  }

  // reset brpc flag default value if not set
  dingofs::ResetBrpcFlagDefaultValue();

  // used for remote cache
  dingofs::cache::FLAGS_mds_addrs = mds_addrs;

  // init global log
  dingofs::Logger::Init("dingo-client");

  if (dingofs::FLAGS_daemonize && !dingofs::utils::DaemonizeExec(orig_args)) {
    std::cerr << "failed to daemonize process.\n";
    return EXIT_FAILURE;
  }

  struct MountOption mount_option{
      .mount_point = mountpoint,
      .fs_name = fs_name,
      .metasystem_type = metasystem_type,
      .mds_addrs = mds_addrs,
      .storage_info = storage_info,
      .meta_url = argv[1],
  };

  fuse_server = new FuseServer();
  if (fuse_server == nullptr) return EXIT_FAILURE;
  auto defer_free = ::absl::MakeCleanup([&]() { delete fuse_server; });

  // init fuse
  if (fuse_server->Init(argv[0], &mount_option) == 1) return EXIT_FAILURE;

  // print current gflags
  LOG(INFO) << dingofs::GenCurrentFlags();

  FsContext fs_context{
      .mount_option = &mount_option,
      .fuse_server = fuse_server,
  };

  // create fuse session
  if (fuse_server->CreateSession(&fs_context) == 1) return EXIT_FAILURE;
  auto defer_destory =
      ::absl::MakeCleanup([&]() { fuse_server->DestroySsesion(); });

  // mount filesystem
  if (fuse_server->SessionMount() == 1) return EXIT_FAILURE;
  auto defer_unmount =
      ::absl::MakeCleanup([&]() { fuse_server->SessionUnmount(); });

  if (fuse_server->Serve() == 1) return EXIT_FAILURE;

  return EXIT_SUCCESS;
}
