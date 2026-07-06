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
#include <unistd.h>

#include <atomic>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <thread>

#include "client/fuse/fs_context.h"
#include "client/fuse/fuse_server.h"
#include "common/flag.h"
#include "common/helper.h"
#include "common/logging.h"
#include "common/options/cache.h"
#include "common/options/client.h"
#include "common/options/common.h"
#include "common/types.h"
#include "dingo_eureka_version.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "utils/daemonize.h"
#include "utils/numa_binder.h"
#include "utils/scoped_cleanup.h"

using FuseServer = dingofs::client::fuse::FuseServer;
using FsContext = dingofs::client::fuse::FsContext;

static FuseServer* fuse_server = nullptr;

// SIGSEGV/SIGABRT = fatal synchronous signals (the process is crashing).
//
// Do the async-signal-safe minimum: write a short note via write(2), restore
// the default disposition, and re-raise so the process cores normally. Do NOT
// unmount, flush logs, or take any lock here -- umount may fork/exec fusermount
// and grab locks, which can deadlock/hang a crashed process and block the core
// dump. Mount cleanup is left to an external watchdog / the kernel FUSE abort.
static void HandleCrashSignal(int sig) {
  static const char msg[] = "dingo-client: fatal signal, re-raising for core\n";
  ssize_t n = write(STDERR_FILENO, msg, sizeof(msg) - 1);
  (void)n;
  signal(sig, SIG_DFL);
  raise(sig);
}

static void BlockGracefulSignals(sigset_t* set) {
  sigemptyset(set);
  sigaddset(set, SIGHUP);

  // Block SIGHUP in the process before worker threads are created. The signal
  // thread below consumes it synchronously with sigwait(), so the handover path
  // runs in a normal thread context instead of an async signal handler.
  CHECK_EQ(pthread_sigmask(SIG_BLOCK, set, nullptr), 0);
}

static std::thread StartGracefulSignalThread(const sigset_t& set,
                                             std::atomic<bool>* stopped) {
  return std::thread([set, stopped]() {
    while (!stopped->load()) {
      int sig = 0;
      // sigwait only fails with EINVAL (an invalid signal in the set), a
      // programming error here; fail loudly rather than spinning on a tight
      // retry loop that would peg a CPU and flood the log.
      CHECK_EQ(sigwait(&set, &sig), 0) << "sigwait failed";
      if (stopped->load()) break;

      if (sig == SIGHUP && fuse_server != nullptr) {
        fuse_server->TriggerHandover();
      }
    }
  });
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
  $ dingo-client mds://10.220.69.10:7400/myfs /mnt/dingofs --fuse_subdir=/tenant-1
)",
    .patterns = dingofs::kSdkFlagPatterns,
};

int main(int argc, char* argv[]) {
  std::vector<std::string> orig_args;
  orig_args.reserve(argc);
  for (int i = 1; i < argc; ++i) {
    orig_args.emplace_back(argv[i]);
  }

  sigset_t graceful_signal_set;
  BlockGracefulSignals(&graceful_signal_set);

  // install crash signal handlers
  InstallSignal(SIGSEGV, HandleCrashSignal);
  InstallSignal(SIGABRT, HandleCrashSignal);

  //  parse gflags
  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) {
    return EXIT_FAILURE;
  }

  // Pin process to NUMA node (no-op when --numa_node < 0). Must happen before
  // any worker threads or large allocations are created so the binding is
  // inherited by all descendants.
  dingofs::utils::BindNumaOrDie();

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
      std::cerr << fmt::format(
          "{} auto umount {}, last time not unmount properly\n",
          dingofs::client::RedString("WARNING:"), orig_mountpoint);
      int umount_rc = dingofs::client::Umount(orig_mountpoint);
      if (umount_rc != 0) {
        std::cerr << fmt::format("failed to umount {}, errmsg: {}\n",
                                 orig_mountpoint, strerror(umount_rc));
        return EXIT_FAILURE;
      }
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
      .subdir = dingofs::client::FLAGS_fuse_subdir,
  };

  fuse_server = new FuseServer();
  if (fuse_server == nullptr) return EXIT_FAILURE;
  // Null the global after delete; the graceful signal thread is joined before
  // this cleanup runs.
  auto defer_free = dingofs::MakeScopedCleanup([&]() {
    delete fuse_server;
    fuse_server = nullptr;
  });

  // init fuse
  if (fuse_server->Init(argv[0], &mount_option) == 1) return EXIT_FAILURE;

  // print current gflags
  LOG(INFO) << dingofs::GenCurrentFlags();
  // print version
  LOG(INFO) << dingofs::DingoShortVersionString();
  LOG(INFO) << dingofs::DingoVersionString();
  // print dingo eureka version
  LOG(INFO) << FormatDingoEurekaVersion();

  FsContext fs_context{
      .mount_option = &mount_option,
      .fuse_server = fuse_server,
  };

  // create fuse session
  if (fuse_server->CreateSession(&fs_context) == 1) return EXIT_FAILURE;
  auto defer_destory =
      dingofs::MakeScopedCleanup([&]() { fuse_server->DestroySession(); });

  // mount filesystem
  if (fuse_server->SessionMount() == 1) return EXIT_FAILURE;
  auto defer_unmount =
      dingofs::MakeScopedCleanup([&]() { fuse_server->SessionUnmount(); });

  fuse_server->ArmHandoverController();

  std::atomic<bool> graceful_signal_thread_stopped{false};

  auto graceful_signal_thread = StartGracefulSignalThread(
      graceful_signal_set, &graceful_signal_thread_stopped);

  auto defer_signal_thread = dingofs::MakeScopedCleanup([&]() {
    graceful_signal_thread_stopped.store(true);
    pthread_kill(graceful_signal_thread.native_handle(), SIGHUP);
    if (graceful_signal_thread.joinable()) graceful_signal_thread.join();
  });

  if (fuse_server->Serve() == 1) return EXIT_FAILURE;

  return EXIT_SUCCESS;
}
