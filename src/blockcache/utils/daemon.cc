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

#include "blockcache/utils/daemon.h"

#include <glog/logging.h>
#include <poll.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <iostream>

#include "utils/daemonize.h"

namespace dingofs {
namespace blockcache {

static constexpr char kReadyFdEnv[] = "DINGO_CACHE_READY_FD";
static constexpr int kReadyTimeoutMs = 120 * 1000;

int DaemonizeAndWait(const std::vector<std::string>& args) {
  int fds[2];
  if (pipe(fds) != 0) {
    perror("pipe() failed");
    return -1;
  }
  const pid_t pid = fork();
  if (pid < 0) {
    perror("fork() failed");
    return -1;
  }
  if (pid == 0) {
    close(fds[0]);
    setenv(kReadyFdEnv, std::to_string(fds[1]).c_str(), 1);
    _exit(utils::DaemonizeExec(args) ? 0 : 1);  // only returns on failure
  }
  close(fds[1]);
  int wstatus = 0;
  waitpid(pid, &wstatus, 0);  // the intermediate parent exits at once

  pollfd pfd{};
  pfd.fd = fds[0];
  pfd.events = POLLIN;
  const int rc = poll(&pfd, 1, kReadyTimeoutMs);
  char byte = 0;
  const ssize_t n = rc > 0 ? read(fds[0], &byte, 1) : 0;
  close(fds[0]);
  if (n == 1) {
    return 0;
  }
  if (rc == 0) {
    std::cerr << "dingo-cache is still starting after "
              << kReadyTimeoutMs / 1000 << " s, left running\n";
    return 0;
  }
  std::cerr << "dingo-cache failed to start, see " << ::FLAGS_log_dir << "\n";
  return -1;
}

void ReportDaemonReady() {
  const char* env = getenv(kReadyFdEnv);
  if (env == nullptr) {
    return;
  }
  const int fd = static_cast<int>(std::strtol(env, nullptr, 10));
  const char byte = '1';
  (void)!write(fd, &byte, 1);
  close(fd);
  unsetenv(kReadyFdEnv);
}

}  // namespace blockcache
}  // namespace dingofs
