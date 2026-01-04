// Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SRC_UTILS_DAEMONIZE_H_
#define SRC_UTILS_DAEMONIZE_H_

#include <unistd.h>

#include <iostream>

namespace dingofs {
namespace utils {

inline bool Daemonize(const bool chdir_root = false,
                      const bool close_fds = false) {
  pid_t pid = fork();
  if (pid < 0) {
    perror("fork() failed");
    return false;
  }
  if (pid > 0) {
    // parent process exit
    _exit(0);
  }

  // child process continue, create new session, become session leader
  if (setsid() < 0) {
    perror("setsid() failed");
    return false;
  }

  // second fork, prevent from acquiring a controlling terminal
  pid = fork();
  if (pid < 0) {
    perror("second fork() failed");
    return false;
  }
  if (pid > 0) {
    // first child process exit
    _exit(0);
  }

  if (chdir_root) {
    // change working directory to root
    if (chdir("/") < 0) {
      perror("chdir() failed");
      return false;
    }
  }

  std::cout << "daemonize success, pid: " << getpid() << '\n';

  if (close_fds) {
    // close standard file descriptors
    close(STDIN_FILENO);
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }

  return true;
}

inline bool SystemDaemonize(const bool chdir_root = false,
                            const bool close_fds = false) {
  int nochdir = chdir_root ? 0 : 1;
  int noclose = close_fds ? 0 : 1;

  if (daemon(nochdir, noclose) != 0) {
    perror("daemon() failed");
    return false;
  }

  return true;
}

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_UTILS_DAEMONIZE_H_