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

/*
 * Project: DingoFS
 * Created Date: 2026-06-22
 * Author: AI
 */

#include "test/integration/cache/deploy/process_guard.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <glog/logging.h>
#include <netinet/in.h>
#include <signal.h>
#include <spawn.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstring>
#include <thread>

extern char** environ;

namespace dingofs {
namespace cache {
namespace integration {

bool ProcessGuard::Start(const std::vector<std::string>& argv,
                         const std::string& log_path) {
  CHECK(!argv.empty()) << "argv must contain at least the binary path";
  CHECK(reaped_) << "ProcessGuard already holds a running child";

  posix_spawn_file_actions_t actions;
  posix_spawn_file_actions_init(&actions);
  // Redirect both stdout and stderr to the log file so a failed launch leaves a
  // diagnosable trace behind.
  posix_spawn_file_actions_addopen(&actions, STDOUT_FILENO, log_path.c_str(),
                                   O_WRONLY | O_CREAT | O_TRUNC, 0644);
  posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);

  std::vector<char*> c_argv;
  c_argv.reserve(argv.size() + 1);
  for (const auto& arg : argv) {
    c_argv.push_back(const_cast<char*>(arg.c_str()));
  }
  c_argv.push_back(nullptr);

  pid_t pid = -1;
  int rc = posix_spawn(&pid, argv[0].c_str(), &actions, nullptr, c_argv.data(),
                       environ);
  posix_spawn_file_actions_destroy(&actions);

  if (rc != 0) {
    LOG(ERROR) << "posix_spawn(" << argv[0] << ") failed: " << std::strerror(rc);
    return false;
  }

  pid_ = pid;
  reaped_ = false;
  LOG(INFO) << "spawned " << argv[0] << " pid=" << pid_ << " log=" << log_path;
  return true;
}

bool ProcessGuard::Reap(bool block) {
  if (reaped_) return true;

  int status = 0;
  pid_t rc = ::waitpid(pid_, &status, block ? 0 : WNOHANG);
  if (rc == pid_) {
    reaped_ = true;
    return true;
  }
  return false;  // 0 (still running) or -1 (already reaped/error)
}

bool ProcessGuard::Running() const {
  if (reaped_) return false;
  // const peek: WNOHANG reap without mutating reaped_ here -- a 0 return means
  // the child is alive; a positive return means it exited (still "not Stopped").
  int status = 0;
  pid_t rc = ::waitpid(pid_, &status, WNOHANG);
  return rc == 0;
}

void ProcessGuard::Stop(int term_grace_ms) {
  if (reaped_ || pid_ <= 0) return;

  ::kill(pid_, SIGTERM);
  for (int waited = 0; waited < term_grace_ms; waited += 20) {
    if (Reap(/*block=*/false)) return;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }

  LOG(WARNING) << "pid=" << pid_ << " did not exit on SIGTERM; sending SIGKILL";
  ::kill(pid_, SIGKILL);
  Reap(/*block=*/true);
}

int PickFreePort() {
  int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  CHECK_GE(fd, 0) << "socket() failed: " << std::strerror(errno);

  int one = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = ::inet_addr("127.0.0.1");
  addr.sin_port = 0;  // let the kernel choose
  CHECK_EQ(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0)
      << "bind() failed: " << std::strerror(errno);

  socklen_t len = sizeof(addr);
  CHECK_EQ(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len), 0)
      << "getsockname() failed: " << std::strerror(errno);
  int port = ntohs(addr.sin_port);
  ::close(fd);
  return port;
}

std::string ExeDir() {
  char buf[4096];
  ssize_t n = ::readlink("/proc/self/exe", buf, sizeof(buf) - 1);
  if (n <= 0) return ".";
  buf[n] = '\0';
  std::string path(buf);
  auto pos = path.find_last_of('/');
  return pos == std::string::npos ? "." : path.substr(0, pos);
}

bool WaitPort(const std::string& ip, int port, int timeout_ms) {
  for (int waited = 0; waited < timeout_ms; waited += 50) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      sockaddr_in addr{};
      addr.sin_family = AF_INET;
      addr.sin_addr.s_addr = ::inet_addr(ip.c_str());
      addr.sin_port = htons(static_cast<uint16_t>(port));
      int rc = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
      ::close(fd);
      if (rc == 0) return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
  }
  return false;
}

}  // namespace integration
}  // namespace cache
}  // namespace dingofs
