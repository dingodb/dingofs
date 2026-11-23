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
 * Created Date: 2026-10-04
 * Author: AI
 */

#include <fcntl.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <set>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#include "blockcache/utils/daemonize.h"
#include "test/unit/blockcache/base/scoped_env.h"
#include "test/unit/blockcache/base/temp_dir.h"

namespace dingofs {
namespace blockcache {

using ::testing::ExitedWithCode;
using ::testing::HasSubstr;
using ::testing::MatchesRegex;

static std::set<int> GetPipeFds() {
  std::set<int> fds;
  for (const auto& entry :
       std::filesystem::directory_iterator("/proc/self/fd")) {
    std::error_code ec;
    const std::filesystem::path target =
        std::filesystem::read_symlink(entry.path(), ec);
    if (!ec && target.string().starts_with("pipe:")) {
      fds.insert(std::stoi(entry.path().filename().string()));
    }
  }
  return fds;
}

static std::set<int> GetFds(pid_t pid) {
  std::set<int> fds;
  for (const auto& entry : std::filesystem::directory_iterator(
           "/proc/" + std::to_string(pid) + "/fd")) {
    fds.insert(std::stoi(entry.path().filename().string()));
  }
  return fds;
}

static int GetLowestFreeFd() {
  const int fd = ::open("/dev/null", O_RDONLY);
  ::close(fd);
  return fd;
}

static std::string ReadFile(const std::string& path) {
  std::ifstream file(path);
  std::stringstream text;
  text << file.rdbuf();
  return text.str();
}

static pid_t ReadPid(const std::string& path) {
  pid_t pid = -1;
  std::ifstream(path) >> pid;
  return pid;
}

static std::vector<std::string> GetShArgv(const std::string& script) {
  return {"/bin/sh", "-c", script};
}

class ScopedStdin {
 public:
  explicit ScopedStdin(int fd)
      : saved_(::fcntl(STDIN_FILENO, F_DUPFD_CLOEXEC, 3)) {
    ::dup2(fd, STDIN_FILENO);
  }

  ~ScopedStdin() {
    if (saved_ >= 0) {
      ::dup2(saved_, STDIN_FILENO);
      ::close(saved_);
    } else {
      ::close(STDIN_FILENO);
    }
  }

  ScopedStdin(const ScopedStdin&) = delete;
  ScopedStdin& operator=(const ScopedStdin&) = delete;

 private:
  const int saved_;
};

static int RunStartDaemon(const std::vector<std::string>& argv,
                          std::string* out, std::string* err) {
  testing::internal::CaptureStdout();
  testing::internal::CaptureStderr();
  const int rc = StartDaemon(argv);
  *out = testing::internal::GetCapturedStdout();
  *err = testing::internal::GetCapturedStderr();
  return rc;
}

TEST(DaemonizeTest, TakeReadyFd) {
  {
    const ScopedEnv none(kReadyFdEnv, nullptr);
    EXPECT_EQ(TakeReadyFd(), -1);
  }

  int pipe_fds[2];
  int socket_fds[2];
  ASSERT_EQ(::pipe(pipe_fds), 0);
  ASSERT_EQ(::socketpair(AF_UNIX, SOCK_STREAM, 0, socket_fds), 0);
  for (const int fd : {pipe_fds[1], socket_fds[1]}) {
    const ScopedEnv env(kReadyFdEnv, std::to_string(fd).c_str());
    EXPECT_EQ(TakeReadyFd(), fd);
    EXPECT_EQ(std::getenv(kReadyFdEnv), nullptr);
    EXPECT_EQ(::fcntl(fd, F_GETFD), FD_CLOEXEC);
  }

  {
    const ScopedStdin stdin_pipe(pipe_fds[1]);
    const ScopedEnv env(kReadyFdEnv, "0");
    testing::internal::CaptureStderr();
    EXPECT_EQ(TakeReadyFd(), -1);
    EXPECT_THAT(
        testing::internal::GetCapturedStderr(),
        HasSubstr("WARNING: ignoring " + std::string(kReadyFdEnv) + "=0:"));
    EXPECT_EQ(::fcntl(STDIN_FILENO, F_GETFD), 0);
  }

  int own_fds[2];
  ASSERT_EQ(::pipe2(own_fds, O_CLOEXEC), 0);
  const int file_fd = ::open("/dev/null", O_WRONLY);
  ASSERT_GE(file_fd, 0);
  const std::vector<std::string> values = {std::to_string(own_fds[1]),
                                           std::to_string(pipe_fds[0]),
                                           std::to_string(file_fd),
                                           "1023",
                                           std::to_string(socket_fds[0]) + "x",
                                           ""};
  for (const std::string& value : values) {
    const ScopedEnv env(kReadyFdEnv, value.c_str());
    testing::internal::CaptureStderr();
    EXPECT_EQ(TakeReadyFd(), -1) << value;
    EXPECT_THAT(testing::internal::GetCapturedStderr(),
                HasSubstr("WARNING: ignoring " + std::string(kReadyFdEnv) +
                          "=" + value + ":"))
        << value;
    EXPECT_EQ(std::getenv(kReadyFdEnv), nullptr) << value;
  }
  EXPECT_EQ(::fcntl(own_fds[1], F_GETFD), FD_CLOEXEC);
  EXPECT_EQ(::fcntl(pipe_fds[0], F_GETFD), 0);
  EXPECT_EQ(::fcntl(file_fd, F_GETFD), 0);

  for (const int fd : {pipe_fds[0], pipe_fds[1], socket_fds[0], socket_fds[1],
                       own_fds[0], own_fds[1], file_fd}) {
    ::close(fd);
  }
}

TEST(DaemonizeTest, NotifyReady) {
  {
    const std::set<int> pipes = GetPipeFds();
    NotifyReady(-1);
    EXPECT_EQ(GetPipeFds(), pipes);
  }

  {
    int fds[2];
    ASSERT_EQ(::pipe(fds), 0);
    NotifyReady(fds[1]);
    EXPECT_EQ(::fcntl(fds[1], F_GETFD), -1);

    char buffer[2] = {};
    EXPECT_EQ(::read(fds[0], buffer, sizeof(buffer)), 1);
    EXPECT_EQ(buffer[0], '1');
    EXPECT_EQ(::read(fds[0], buffer, sizeof(buffer)), 0);
    ::close(fds[0]);
  }
}

TEST(DaemonizeTest, Detach) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  EXPECT_EXIT(
      {
        const int err = ::dup(STDERR_FILENO);
        std::string why;
        {
          TempDir dir;
          const std::string output = (dir.path() / "out").string();
          std::ofstream(output) << "earlier\n";
          if (!Detach(output)) {
            why = "Detach failed";
          }
          std::cout << "to stdout\n" << std::flush;
          std::cerr << "to stderr\n" << std::flush;
          if (ReadFile(output) != "earlier\nto stdout\nto stderr\n") {
            why += " output: " + ReadFile(output);
          }
          if (::getsid(0) != ::getpid()) {
            why += " not a session leader";
          }
        }
        (void)!::write(err, why.data(), why.size());
        std::exit(why.empty() ? 0 : 1);
      },
      ExitedWithCode(0), "");

  TempDir dir;
  struct stat before{};
  ASSERT_EQ(::fstat(STDOUT_FILENO, &before), 0);
  const pid_t sid = ::getsid(0);
  EXPECT_FALSE(Detach((dir.path() / "missing" / "out").string()));
  struct stat after{};
  ASSERT_EQ(::fstat(STDOUT_FILENO, &after), 0);
  EXPECT_EQ(after.st_ino, before.st_ino);
  EXPECT_EQ(::getsid(0), sid);
}

TEST(DaemonizeTest, StartDaemon) {
  {
    TempDir dir;
    const std::string seen = (dir.path() / "seen").string();
    const ScopedEnv stale(kReadyFdEnv, "7");
    const ScopedEnv var("DAEMON_TEST_VAR", "ours");
    int leaked[2];
    ASSERT_EQ(::pipe(leaked), 0);
    const ScopedStdin stdin_pipe(leaked[0]);
    const std::set<int> pipes = GetPipeFds();

    std::string out;
    std::string err;
    const int rc = RunStartDaemon(
        GetShArgv("echo out; echo err >&2; echo $$ $" +
                  std::string(kReadyFdEnv) + " $DAEMON_TEST_VAR > " + seen +
                  "; (printf 1 >&3); exec sleep 60"),
        &out, &err);
    ASSERT_EQ(rc, 0) << err;
    std::istringstream fields(ReadFile(seen));
    pid_t pid = -1;
    std::string ready_fd;
    std::string ours;
    fields >> pid >> ready_fd >> ours;
    ASSERT_GT(pid, 0);

    EXPECT_THAT(out, HasSubstr("Waiting for sh (pid " + std::to_string(pid) +
                               ") to be ready\n"));
    EXPECT_THAT(out, HasSubstr("out\n"));
    EXPECT_EQ(err, "err\n");
    EXPECT_EQ(ready_fd, "3");
    EXPECT_EQ(ours, "ours");
    EXPECT_EQ(GetFds(pid), (std::set<int>{0, 1, 2, 3}));
    const std::string fd_dir = "/proc/" + std::to_string(pid) + "/fd/";
    EXPECT_EQ(std::filesystem::read_symlink(fd_dir + "0"), "/dev/null");
    EXPECT_TRUE(std::filesystem::is_fifo(fd_dir + "3"));
    EXPECT_EQ(::getpgid(pid), ::getpgrp());
    EXPECT_EQ(::getsid(pid), ::getsid(0));
    EXPECT_EQ(GetPipeFds(), pipes);

    ::kill(pid, SIGKILL);
    ::waitpid(pid, nullptr, 0);
    ::close(leaked[0]);
    ::close(leaked[1]);
  }

  {
    TempDir dir;
    const std::string pid_file = (dir.path() / "pid").string();
    const std::string seen = (dir.path() / "seen").string();
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    for (const int fd : {STDOUT_FILENO, STDERR_FILENO}) {
      ASSERT_EQ(::fcntl(fd, F_SETFD, FD_CLOEXEC), 0);
    }
    const int rc = StartDaemon(
        GetShArgv("echo $$ > " + pid_file + "; readlink /proc/$$/fd/1 " +
                  "/proc/$$/fd/2 > " + seen + "; (printf 1 >&3)"));
    for (const int fd : {STDOUT_FILENO, STDERR_FILENO}) {
      ASSERT_EQ(::fcntl(fd, F_SETFD, 0), 0);
    }
    testing::internal::GetCapturedStdout();
    const std::string err = testing::internal::GetCapturedStderr();
    ASSERT_EQ(rc, 0) << err;
    EXPECT_EQ(ReadFile(seen), "/dev/null\n/dev/null\n");
    const pid_t pid = ReadPid(pid_file);
    ::waitpid(pid, nullptr, 0);
  }

  {
    const std::set<int> pipes = GetPipeFds();
    std::string out;
    std::string err;
    EXPECT_EQ(RunStartDaemon(GetShArgv("echo boom >&2; exit 3"), &out, &err),
              3);
    EXPECT_THAT(err, MatchesRegex("boom\nsh \\(pid [0-9]+\\) exited with code "
                                  "3 before it was ready\n"));
    EXPECT_EQ(GetPipeFds(), pipes);
  }

  {
    std::string out;
    std::string err;
    EXPECT_EQ(RunStartDaemon(GetShArgv("exit 0"), &out, &err), 1);
    EXPECT_THAT(err, HasSubstr("exited with code 0 before it was ready\n"));
  }

  {
    std::string out;
    std::string err;
    EXPECT_EQ(RunStartDaemon(GetShArgv("kill -KILL $$"), &out, &err),
              128 + SIGKILL);
    EXPECT_THAT(err, HasSubstr("was killed by signal 9 (Killed) before it "
                               "was ready\n"));
  }

  for (const char* breach : {"exec 3>&-", "printf 0 >&3"}) {
    TempDir dir;
    const std::string pid_file = (dir.path() / "pid").string();
    std::string out;
    std::string err;
    const int rc = RunStartDaemon(
        GetShArgv("echo $$ > " + pid_file + "; " + breach + "; exec sleep 60"),
        &out, &err);
    EXPECT_EQ(rc, 128 + SIGKILL) << breach;
    EXPECT_THAT(err, HasSubstr("did not report ready and was stopped\n"))
        << breach;
    const pid_t pid = ReadPid(pid_file);
    EXPECT_TRUE(::kill(pid, 0) == -1 && errno == ESRCH) << breach;
  }

  {
    struct sigaction saved{};
    ASSERT_EQ(::sigaction(SIGCHLD, nullptr, &saved), 0);
    ASSERT_NE(::signal(SIGCHLD, SIG_IGN), SIG_ERR);
    std::string out;
    std::string err;
    EXPECT_EQ(RunStartDaemon(GetShArgv("exit 3"), &out, &err), 3);
    EXPECT_THAT(err, HasSubstr("exited with code 3 before it was ready\n"));
    ASSERT_EQ(::sigaction(SIGCHLD, &saved, nullptr), 0);
  }

  {
    const std::set<int> pipes = GetPipeFds();
    std::string out;
    std::string err;
    EXPECT_EQ(RunStartDaemon({"/nonexistent/sh"}, &out, &err), 1);
    EXPECT_EQ(out, "");
    EXPECT_EQ(err,
              "Fail to start /nonexistent/sh: No such file or directory\n");
    EXPECT_EQ(GetPipeFds(), pipes);
  }

  {
    testing::internal::CaptureStderr();
    rlimit saved{};
    ASSERT_EQ(::getrlimit(RLIMIT_NOFILE, &saved), 0);
    rlimit tight = saved;
    tight.rlim_cur = GetLowestFreeFd();
    ASSERT_EQ(::setrlimit(RLIMIT_NOFILE, &tight), 0);
    const int rc = StartDaemon(GetShArgv("printf 1 >&3"));
    ASSERT_EQ(::setrlimit(RLIMIT_NOFILE, &saved), 0);
    EXPECT_EQ(rc, 1);
    EXPECT_EQ(testing::internal::GetCapturedStderr(),
              "Fail to create the ready pipe: Too many open files\n");
  }
}

}  // namespace blockcache
}  // namespace dingofs
