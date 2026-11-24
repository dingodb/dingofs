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

#include "blockcache/utils/daemonize.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <spawn.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <string_view>
#include <thread>

#include "blockcache/utils/string.h"

extern char** environ;

namespace dingofs {
namespace blockcache {

static constexpr int kReadyFd = 3;

int TakeReadyFd() {
  const char* value = std::getenv(kReadyFdEnv);
  if (value == nullptr) {
    return -1;
  }

  std::string_view text = value;
  int fd = -1;
  struct stat st{};
  const bool inherited =
      ConsumeUint(&text, '\0', &fd) && fd > STDERR_FILENO &&
      fstat(fd, &st) == 0 && (S_ISFIFO(st.st_mode) || S_ISSOCK(st.st_mode)) &&
      fcntl(fd, F_GETFD) == 0 && (fcntl(fd, F_GETFL) & O_ACCMODE) != O_RDONLY;
  if (inherited) {
    fcntl(fd, F_SETFD, FD_CLOEXEC);
  } else {
    std::cerr << "WARNING: ignoring " << kReadyFdEnv << "=" << value
              << ": no pipe or socket past stderr we inherited for writing\n";
  }
  unsetenv(kReadyFdEnv);
  return inherited ? fd : -1;
}

static int Spawn(const std::vector<std::string>& argv, int ready, pid_t* pid) {
  std::vector<char*> args;
  args.reserve(argv.size() + 1);
  for (const auto& arg : argv) {
    args.push_back(const_cast<char*>(arg.c_str()));
  }
  args.push_back(nullptr);

  const std::string prefix = std::string(kReadyFdEnv) + "=";
  std::string ready_env = prefix + std::to_string(kReadyFd);
  std::vector<char*> env = {ready_env.data()};
  for (char** kv = environ; *kv != nullptr; kv++) {
    if (!std::string_view(*kv).starts_with(prefix)) {
      env.push_back(*kv);
    }
  }
  env.push_back(nullptr);

  posix_spawn_file_actions_t actions;
  int rc = posix_spawn_file_actions_init(&actions);
  if (rc != 0) {
    return rc;
  }
  const auto add = [&rc](int result) { rc = rc != 0 ? rc : result; };
  add(posix_spawn_file_actions_adddup2(&actions, ready, kReadyFd));
  add(posix_spawn_file_actions_addopen(&actions, STDIN_FILENO, "/dev/null",
                                       O_RDONLY, 0));
  for (const int fd : {STDOUT_FILENO, STDERR_FILENO}) {
    if (fcntl(fd, F_GETFD) != 0) {
      add(posix_spawn_file_actions_addopen(&actions, fd, "/dev/null", O_WRONLY,
                                           0));
    }
  }
  add(posix_spawn_file_actions_addclosefrom_np(&actions, kReadyFd + 1));
  if (rc == 0) {
    rc = posix_spawn(pid, args[0], &actions, nullptr, args.data(), env.data());
  }
  posix_spawn_file_actions_destroy(&actions);
  return rc;
}

static pid_t Wait(pid_t pid, int* status, int options) {
  pid_t rc;
  while ((rc = waitpid(pid, status, options)) < 0 && errno == EINTR) {
  }
  return rc;
}

static int Reap(pid_t pid, const std::string& name) {
  int status = 0;
  pid_t reaped = 0;
  for (int i = 0; i < 50 && reaped == 0; i++) {
    reaped = Wait(pid, &status, WNOHANG);
    if (reaped == 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  const bool stopped = reaped == 0;
  if (stopped) {
    kill(pid, SIGKILL);
    reaped = Wait(pid, &status, 0);
  }
  if (reaped < 0) {
    std::cerr << "Fail to wait for " << name << " (pid " << pid
              << "): " << strerror(errno) << "\n";
    return 1;
  }

  std::cerr << name << " (pid " << pid << ") ";
  if (stopped) {
    std::cerr << "did not report ready and was stopped\n";
  } else if (WIFSIGNALED(status)) {
    std::cerr << "was killed by signal " << WTERMSIG(status) << " ("
              << strsignal(WTERMSIG(status)) << ") before it was ready\n";
  } else {
    std::cerr << "exited with code " << WEXITSTATUS(status)
              << " before it was ready\n";
  }
  return WIFSIGNALED(status) ? 128 + WTERMSIG(status)
                             : std::max(WEXITSTATUS(status), 1);
}

int StartDaemon(const std::vector<std::string>& argv) {
  signal(SIGCHLD, SIG_DFL);

  int ready[2];
  if (pipe2(ready, O_CLOEXEC) != 0) {
    std::cerr << "Fail to create the ready pipe: " << strerror(errno) << "\n";
    return 1;
  }
  pid_t pid = -1;
  const int rc = Spawn(argv, ready[1], &pid);
  close(ready[1]);
  if (rc != 0) {
    close(ready[0]);
    std::cerr << "Fail to start " << argv[0] << ": " << strerror(rc) << "\n";
    return 1;
  }

  const std::string name = std::filesystem::path(argv[0]).filename().string();
  std::cout << "Waiting for " << name << " (pid " << pid << ") to be ready\n"
            << std::flush;
  char byte = 0;
  ssize_t n;
  while ((n = read(ready[0], &byte, 1)) < 0 && errno == EINTR) {
  }
  close(ready[0]);
  return n == 1 && byte == '1' ? 0 : Reap(pid, name);
}

bool Detach(const std::string& output) {
  fflush(nullptr);
  const int fd =
      open(output.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
  if (fd < 0) {
    PLOG(ERROR) << "Fail to open " << output;
    return false;
  }
  dup2(fd, STDOUT_FILENO);
  dup2(fd, STDERR_FILENO);
  if (fd > STDERR_FILENO) {
    close(fd);
  }
  setsid();
  return true;
}

void NotifyReady(int fd) {
  if (fd < 0) {
    return;
  }
  (void)!write(fd, "1", 1);
  close(fd);
}

}  // namespace blockcache
}  // namespace dingofs
