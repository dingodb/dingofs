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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <vector>

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

// NOTE: args should not contail the program name
inline bool DaemonizeExec(const std::vector<std::string>& args) {
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

  std::vector<std::string> new_args;
  {
    char self_path[4096];
    ssize_t len = readlink("/proc/self/exe", self_path, sizeof(self_path) - 1);
    if (len == -1) {
      perror("read self exe failed");
      return false;
    }

    if (len >= static_cast<ssize_t>(sizeof(self_path) - 1)) {
      perror("self exe path too long");
      return false;
    }

    self_path[len] = '\0';
    new_args.push_back(std::string(self_path));

    for (const auto& a : args) {
      if (a == "--daemonize" || a == "-daemonize") {
        continue;
      }
      if (a.rfind("--daemonize", 0) == 0 || a.rfind("-daemonize", 0) == 0) {
        continue;
      }
      new_args.push_back(a);
    }
  }

  umask(0);

  {
    int null_fd = open("/dev/null", O_RDONLY);
    if (null_fd < 0) {
      perror("open(/dev/null) failed for stdin");
      return false;
    }

    dup2(null_fd, STDIN_FILENO);
    close(null_fd);
  }

  {
    std::string stdout_file = "/tmp/" + std::to_string(getpid()) + ".stdout";

    std::cout << "daemonize stdout/stderr file: " << stdout_file << '\n';

    int log_fd = open(stdout_file.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (log_fd < 0) {
      perror("open stdout log file failed");
      return false;
    }

    dup2(log_fd, STDOUT_FILENO);
    dup2(log_fd, STDERR_FILENO);

    std::cout << "daemonize success, pid: " << getpid() << '\n';

    close(log_fd);
  }

  std::vector<char*> new_argv;
  new_argv.reserve(new_args.size());

  for (const auto& arg : new_args) {
    new_argv.push_back(const_cast<char*>(arg.c_str()));
  }
  new_argv.push_back(nullptr);

  execv(new_argv[0], new_argv.data());

  perror("execv() failed");
  return false;
}

}  // namespace utils
}  // namespace dingofs

#endif  // SRC_UTILS_DAEMONIZE_H_