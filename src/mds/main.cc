// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <csignal>
#include <iostream>
#include <string>

#include "backtrace.h"
#include "common/const.h"
#include "common/flag.h"
#include "common/options/common.h"
#include "common/options/mds.h"
#include "dlfcn.h"
#include "fmt/format.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "libunwind.h"
#include "mds/common/codec.h"
#include "mds/common/helper.h"
#include "mds/server.h"
#include "utils/daemonize.h"

DEFINE_string(storage_url, "file://./conf/coor_list", "storage url, e.g. file://<path> or list://<addr1>");

const int kMaxStacktraceSize = 128;

DEFINE_uint32(mds_cluster_id, 0, "cluster id");

struct StackTraceInfo {
  char* filename{nullptr};
  int lineno{0};
  char* function{nullptr};
  uintptr_t pc{0};
};

// Passed to backtrace callback function.
struct BacktraceData {
  struct StackTraceInfo* stack_traces{nullptr};
  size_t index{0};
  size_t max{0};
  int fail{0};
};

static int BacktraceCallback(void* vdata, uintptr_t pc, const char* filename, int lineno, const char* function) {
  struct BacktraceData* backtrace = (struct BacktraceData*)vdata;
  struct StackTraceInfo* stack_trace;

  if (backtrace->index >= backtrace->max) {
    std::cerr << "stack index beyond max.\n";
    backtrace->fail = 1;
    return 1;
  }

  stack_trace = &backtrace->stack_traces[backtrace->index];

  stack_trace->filename = (filename == nullptr) ? nullptr : strdup(filename);
  stack_trace->lineno = lineno;
  stack_trace->function = (function == nullptr) ? nullptr : strdup(function);
  stack_trace->pc = pc;

  ++backtrace->index;

  return 0;
}

// An error callback passed to backtrace.
static void ErrorCallback(void* vdata, const char* msg, int errnum) {
  struct BacktraceData* data = (struct BacktraceData*)vdata;

  std::cerr << msg;
  if (errnum > 0) {
    std::cerr << ": " << strerror(errnum) << "\n";
  }
  data->fail = 1;
}

// The signal handler
static void SignalHandler(int signo) {
  // flush log
  dingofs::Logger::FlushLogs();

  if (signo == SIGTERM) {
    dingofs::mds::Server& server = dingofs::mds::Server::GetInstance();
    server.Stop();

    _exit(0);
  }

  std::cerr << "received signal: " << signo << '\n';
  std::cerr << "stack trace:\n";
  LOG(ERROR) << "received signal " << signo;
  LOG(ERROR) << "stack trace:";

  struct backtrace_state* state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
  if (state == nullptr) {
    std::cerr << "state is null.\n";
    _exit(1);
  }

  struct StackTraceInfo stack_traces[kMaxStacktraceSize];
  struct BacktraceData data;

  data.stack_traces = &stack_traces[0];
  data.index = 0;
  data.max = kMaxStacktraceSize;
  data.fail = 0;

  if (backtrace_full(state, 0, BacktraceCallback, ErrorCallback, &data) != 0) {
    std::cerr << "backtrace_full fail." << '\n';
    LOG(ERROR) << "backtrace_full fail.";
  }

  for (size_t i = 0; i < data.index; ++i) {
    auto& stack_trace = stack_traces[i];
    int status;
    char* nameptr = stack_trace.function;
    char* demangled = abi::__cxa_demangle(stack_trace.function, nullptr, nullptr, &status);
    if (status == 0 && demangled) {
      nameptr = demangled;
    }

    Dl_info info = {};

    std::string error_msg;
    if (!dladdr((void*)stack_trace.pc, &info)) {
      error_msg = butil::string_printf("#%zu source[%s:%d] symbol[%s] pc[0x%0lx]", i, stack_trace.filename,
                                       stack_trace.lineno, nameptr, static_cast<uint64_t>(stack_trace.pc));

    } else {
      error_msg = butil::string_printf(
          "#%zu source[%s:%d] symbol[%s] pc[0x%0lx] fname[%s] fbase[0x%lx] sname[%s] saddr[0x%lx] ", i,
          stack_trace.filename, stack_trace.lineno, nameptr, static_cast<uint64_t>(stack_trace.pc), info.dli_fname,
          (uint64_t)info.dli_fbase, info.dli_sname, (uint64_t)info.dli_saddr);
    }

    LOG(ERROR) << error_msg;
    std::cerr << error_msg << '\n';

    if (demangled) {
      free(demangled);
    }
  }

  // call abort() to generate core dump
  if (signal(SIGABRT, SIG_DFL) == SIG_ERR) {
    std::cerr << "setup SIGABRT signal to SIG_DFL fail.\n";
  }

  abort();
}

static void SetupSignalHandler() {
  sighandler_t s;
  s = signal(SIGTERM, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGTERM signal fail.\n";
    exit(-1);
  }

  s = signal(SIGSEGV, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGSEGV signal fail.\n";
    exit(-1);
  }

  s = signal(SIGFPE, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGFPE signal fail.\n";
    exit(-1);
  }

  s = signal(SIGBUS, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGBUS signal fail.\n";
    exit(-1);
  }

  s = signal(SIGILL, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGILL signal fail.\n";
    exit(-1);
  }

  s = signal(SIGABRT, SignalHandler);
  if (s == SIG_ERR) {
    std::cout << "setup SIGABRT signal fail.\n";
    exit(-1);
  }

  // ignore SIGPIPE
  s = signal(SIGPIPE, SIG_IGN);
  if (s == SIG_ERR) {
    std::cout << "setup SIGPIPE signal fail.\n";
    exit(-1);
  }
}

static bool GeneratePidFile(const std::string& filepath) {
  int64_t pid = dingofs::mds::Helper::GetPid();
  if (pid <= 0) {
    LOG(ERROR) << "get pid fail.";
    return false;
  }

  LOG(INFO) << "pid file: " << filepath;

  return dingofs::mds::Helper::SaveFile(filepath, std::to_string(pid));
}

static bool CheckStorageUrl(const std::string& storage_url) {
  if (storage_url.empty()) {
    std::cerr << "storage url is empty.\n";
    return false;
  }

  auto storage_addr = dingofs::mds::Helper::ParseStorageAddr(storage_url);
  if (storage_addr.empty()) {
    std::cerr << "storage addr is invalid, please check your storage url: " << storage_url << '\n';
    return false;
  }

  return true;
}

static std::vector<std::pair<std::string, std::string>> GenConfigs() {
  std::vector<std::pair<std::string, std::string>> configs;
  // serverid
  configs.emplace_back("id", fmt::format("[{}]", dingofs::mds::FLAGS_mds_server_id));
  // config
  configs.emplace_back("config", fmt::format("[{}]", dingofs::FLAGS_conf));
  // log
  configs.emplace_back("log",
                       fmt::format("[{} {} {}(verbose)]",
                                   ::FLAGS_log_dir.empty() ? dingofs::GetDefaultDir(dingofs::kLogDir) : ::FLAGS_log_dir,
                                   dingofs::FLAGS_log_level, dingofs::FLAGS_log_v));
  // storage
  if (dingofs::mds::FLAGS_mds_storage_engine == "dummy") {
    configs.emplace_back("storage", fmt::format("[{}]", dingofs::mds::FLAGS_mds_storage_engine));
  } else {
    configs.emplace_back("storage", fmt::format("[{} {}]", dingofs::mds::FLAGS_mds_storage_engine, FLAGS_storage_url));
  }

  return configs;
}

static dingofs::FlagExtraInfo extras = {
    .program = "dingo-mds",
    .usage = "  dingo-mds [OPTIONS]",
    .examples =
        R"(  $ dingo-mds --mds_server_port=7801
  $ dingo-mds --conf=./conf/mds.conf
  $ dingo-mds --conf=./conf/mds.conf --storage_url=file://./conf/coor_list
  $ dingo-mds --conf=./conf/mds.conf --storage_url=list://127.0.0.1:22001
)",
    .patterns = {"src/mds", "options/common"},
};

int main(int argc, char* argv[]) {
  using dingofs::FLAGS_conf;

  std::vector<std::string> orig_args;
  orig_args.reserve(argc);
  for (int i = 1; i < argc; ++i) {
    orig_args.emplace_back(argv[i]);
  }

  int rc = dingofs::ParseFlags(&argc, &argv, extras);
  if (rc != 0) {
    return 1;
  }

#ifdef USE_TCMALLOC
  std::cout << "USE_TCMALLOC is ON\n";
#else
  std::cout << "USE_TCMALLOC is OFF\n";
#endif

  // reset brpc flag default value if not set
  dingofs::ResetBrpcFlagDefaultValue();

  dingofs::mds::MetaCodec::SetClusterID(FLAGS_mds_cluster_id);

  if (dingofs::mds::FLAGS_mds_storage_engine != "dummy" && !CheckStorageUrl(FLAGS_storage_url)) return -1;

  SetupSignalHandler();

  dingofs::Logger::Init("mds");
  // run in daemon mode
  if (dingofs::FLAGS_daemonize && !dingofs::utils::DaemonizeExec(orig_args)) {
    std::cerr << "fail to daemonize process.\n";
    return 1;
  }

  // print config info
  dingofs::Helper::PrintConfigInfo(GenConfigs());

  dingofs::mds::Server& server = dingofs::mds::Server::GetInstance();

  CHECK(server.InitLog()) << "init log error.";
  LOG(INFO) << dingofs::GenCurrentFlags();
  CHECK(server.InitConfig(FLAGS_conf)) << fmt::format("init config({}) error.", FLAGS_conf);
  CHECK(GeneratePidFile(server.GetPidFilePath())) << "generate pid file error.";
  CHECK(server.InitStorage(FLAGS_storage_url)) << "init storage error.";
  CHECK(server.InitOperationProcessor()) << "init operation processor error.";
  CHECK(server.InitCacheGroupMemberManager()) << "init cache group member manager error.";
  CHECK(server.InitHeartbeat()) << "init heartbeat error.";
  CHECK(server.InitMDSMeta()) << "init mds meta error.";
  CHECK(server.InitNotifyBuddy()) << "init notify buddy error.";
  CHECK(server.InitFileSystem()) << "init file system set error.";
  CHECK(server.InitFsInfoSync()) << "init fs info sync error.";
  CHECK(server.InitCacheMemberSynchronizer()) << "init cache member synchronizer error.";
  CHECK(server.InitMonitor()) << "init mds monitor error.";
  CHECK(server.InitGcProcessor()) << "init gc error.";
  CHECK(server.InitQuotaSynchronizer()) << "init quota synchronizer error.";
  CHECK(server.InitCrontab()) << "init crontab error.";
  CHECK(server.InitService()) << "init service error.";

  LOG(INFO) << "##################### init finish ######################";

  server.Run();

  server.Stop();

  return 0;
}