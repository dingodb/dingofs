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

#include "backtrace.h"
#include "dlfcn.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "libunwind.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/version.h"
#include "mdsv2/server.h"

DEFINE_string(conf, "./conf/mdsv2.conf", "mdsv2 config path");
DEFINE_string(coor_url, "file://./conf/coor_list", "coor service url, e.g. file://<path> or list://<addr1>,<addr2>");

// struct DingoStackTraceInfo {
//   char* filename;
//   int lineno;
//   char* function;
//   uintptr_t pc;
// };

// // Passed to backtrace callback function.
// struct DingoBacktraceData {
//   struct DingoStackTraceInfo* all;
//   size_t index;
//   size_t max;
//   int failed;
// };

// int BacktraceCallback(void* vdata, uintptr_t pc, const char* filename, int lineno, const char* function) {
//   struct DingoBacktraceData* data = (struct DingoBacktraceData*)vdata;
//   struct DingoStackTraceInfo* p;

//   if (data->index >= data->max) {
//     fprintf(stderr, "callback_one: callback called too many times\n");  // NOLINT
//     data->failed = 1;
//     return 1;
//   }

//   p = &data->all[data->index];

//   // filename
//   if (filename == nullptr)
//     p->filename = nullptr;
//   else {
//     p->filename = strdup(filename);
//     assert(p->filename != nullptr);
//   }

//   // lineno
//   p->lineno = lineno;

//   // function
//   if (function == nullptr)
//     p->function = nullptr;
//   else {
//     p->function = strdup(function);
//     assert(p->function != nullptr);
//   }

//   // pc
//   if (pc != 0) {
//     p->pc = pc;
//   }

//   ++data->index;

//   return 0;
// }

// // An error callback passed to backtrace.
// void ErrorCallback(void* vdata, const char* msg, int errnum) {
//   struct DingoBacktraceData* data = (struct DingoBacktraceData*)vdata;

//   fprintf(stderr, "%s", msg);                                 // NOLINT
//   if (errnum > 0) fprintf(stderr, ": %s", strerror(errnum));  // NOLINT
//   fprintf(stderr, "\n");                                      // NOLINT
//   data->failed = 1;
// }

// // The signal handler
// #define MAX_STACKTRACE_SIZE 128
// static void SignalHandler(int signo) {
//   printf("========== handle signal '%d' ==========\n", signo);

//   if (signo == SIGTERM) {
//     // clean temp directory
//     // dingofs::mdsv2::Helper::RemoveFileOrDirectory(dingodb::Server::GetInstance().PidFilePath());
//     // DINGO_LOG(WARNING) << "GRACEFUL SHUTDOWN, clean up checkpoint dir: "
//     //                    << ", clean up pid_file: " << dingodb::Server::GetInstance().PidFilePath();
//     _exit(0);
//   }

//   std::cerr << "Received signal " << signo << '\n';
//   std::cerr << "Stack trace:" << '\n';
//   DINGO_LOG(ERROR) << "Received signal " << signo;
//   DINGO_LOG(ERROR) << "Stack trace:";

//   struct backtrace_state* state = backtrace_create_state(nullptr, 0, ErrorCallback, nullptr);
//   if (state == nullptr) {
//     std::cerr << "state is null" << '\n';
//   }

//   struct DingoStackTraceInfo all[MAX_STACKTRACE_SIZE];
//   struct DingoBacktraceData data;

//   data.all = &all[0];
//   data.index = 0;
//   data.max = MAX_STACKTRACE_SIZE;
//   data.failed = 0;

//   int i = backtrace_full(state, 0, BacktraceCallback, ErrorCallback, &data);
//   if (i != 0) {
//     std::cerr << "backtrace_full failed" << '\n';
//     DINGO_LOG(ERROR) << "backtrace_full failed";
//   }

//   for (size_t x = 0; x < data.index; x++) {
//     int status;
//     char* nameptr = all[x].function;
//     char* demangled = abi::__cxa_demangle(all[x].function, nullptr, nullptr, &status);
//     if (status == 0 && demangled) {
//       nameptr = demangled;
//     }

//     Dl_info info = {};

//     if (!dladdr((void*)all[x].pc, &info)) {
//       auto error_msg = butil::string_printf("#%zu source[%s:%d] symbol[%s] pc[0x%0lx]", x, all[x].filename,
//                                             all[x].lineno, nameptr, static_cast<uint64_t>(all[x].pc));
//       DINGO_LOG(ERROR) << error_msg;
//       std::cout << error_msg << '\n';
//     } else {
//       auto error_msg = butil::string_printf(
//           "#%zu source[%s:%d] symbol[%s] pc[0x%0lx] fname[%s] fbase[0x%lx] sname[%s] saddr[0x%lx] ", x,
//           all[x].filename, all[x].lineno, nameptr, static_cast<uint64_t>(all[x].pc), info.dli_fname,
//           (uint64_t)info.dli_fbase, info.dli_sname, (uint64_t)info.dli_saddr);
//       DINGO_LOG(ERROR) << error_msg;
//       std::cout << error_msg << '\n';
//     }
//     if (demangled) {
//       free(demangled);
//     }
//   }

//   // call abort() to generate core dump
//   DINGO_LOG(ERROR) << "call abort() to generate core dump for signo=" << signo << " " << strsignal(signo);
//   auto s = signal(SIGABRT, SIG_DFL);
//   if (s == SIG_ERR) {
//     std::cerr << "Failed to set signal handler to SIG_DFL for SIGABRT" << '\n';
//   }
//   abort();
// }

// void SetupSignalHandler() {
//   sighandler_t s;
//   s = signal(SIGTERM, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGTERM\n");
//     exit(-1);
//   }
//   s = signal(SIGSEGV, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGSEGV\n");
//     exit(-1);
//   }
//   s = signal(SIGFPE, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGFPE\n");
//     exit(-1);
//   }
//   s = signal(SIGBUS, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGBUS\n");
//     exit(-1);
//   }
//   s = signal(SIGILL, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGILL\n");
//     exit(-1);
//   }
//   s = signal(SIGABRT, SignalHandler);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGABRT\n");
//     exit(-1);
//   }
//   // ignore SIGPIPE
//   s = signal(SIGPIPE, SIG_IGN);
//   if (s == SIG_ERR) {
//     printf("Failed to setup signal handler for SIGPIPE\n");
//     exit(-1);
//   }
// }

int main(int argc, char* argv[]) {
  if (dingofs::mdsv2::Helper::IsExistPath("conf/gflags.conf")) {
    google::SetCommandLineOption("flagfile", "conf/gflags.conf");
  }

  google::ParseCommandLineFlags(&argc, &argv, true);

  if (dingofs::mdsv2::FLAGS_show_version) {
    dingofs::mdsv2::DingoShowVerion();

    printf("Usage: %s --conf ./conf/mdsv2.conf --coor_url=[file://./conf/coor_list]\n", argv[0]);
    printf("Example: \n");
    printf("         bin/dingofs_mdsv2\n");
    printf(
        "         bin/dingofs_mdsv2 --conf ./conf/mdsv2.yaml "
        "--coor_url=file://./conf/coor_list\n");
    exit(-1);
  }

  dingofs::mdsv2::Server& server = dingofs::mdsv2::Server::GetInstance();

  CHECK(server.InitConfig(FLAGS_conf)) << fmt::format("init config({}) error.", FLAGS_conf);
  CHECK(server.InitLog()) << "init log error.";
  CHECK(server.InitMDSMeta()) << "init mds meta error.";
  CHECK(server.InitCoordinatorClient(FLAGS_coor_url)) << "init coordinator client error.";
  CHECK(server.InitFsIdGenerator()) << "init fs id generator error.";
  CHECK(server.InitStorage(FLAGS_coor_url)) << "init storage error.";
  CHECK(server.InitFileSystem()) << "init file system error.";
  CHECK(server.InitWorkerSet()) << "init worker set error.";
  CHECK(server.InitHeartbeat()) << "init heartbeat error.";
  CHECK(server.InitCrontab()) << "init crontab error.";

  server.Run();

  server.Stop();

  return 0;
}