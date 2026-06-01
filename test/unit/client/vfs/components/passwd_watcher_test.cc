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

#include "client/vfs/components/passwd_watcher.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

namespace dingofs {
namespace client {
namespace vfs {
namespace test {

namespace {

// Counts callback invocations and lets a test block until at least `n` fire.
class Latch {
 public:
  void Fire() {
    std::lock_guard<std::mutex> lk(mu_);
    ++count_;
    cv_.notify_all();
  }

  // Returns true once count_ >= n within the timeout.
  bool WaitFor(int n, std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(mu_);
    return cv_.wait_for(lk, timeout, [&] { return count_ >= n; });
  }

  int Count() {
    std::lock_guard<std::mutex> lk(mu_);
    return count_;
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  int count_ = 0;
};

// Writes `name` inside `dir` by creating a temp file and rename()-ing it over
// the target — mirrors how useradd/vipw replace /etc/passwd (IN_MOVED_TO).
void RenameInto(const std::string& dir, const std::string& name) {
  std::string tmp = dir + "/." + name + ".tmp";
  std::string dst = dir + "/" + name;
  int fd = ::open(tmp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  ASSERT_GE(fd, 0) << std::strerror(errno);
  ASSERT_EQ(::write(fd, "x\n", 2), 2);
  ASSERT_EQ(::close(fd), 0);
  ASSERT_EQ(::rename(tmp.c_str(), dst.c_str()), 0) << std::strerror(errno);
}

}  // namespace

class PasswdWatcherTest : public ::testing::Test {
 protected:
  void SetUp() override {
    char tmpl[] = "/tmp/dingofs_pwwatch_XXXXXX";
    char* p = ::mkdtemp(tmpl);
    ASSERT_NE(p, nullptr) << std::strerror(errno);
    dir_ = p;
  }

  void TearDown() override {
    if (dir_.empty()) return;
    // Best-effort cleanup of the entries a test might have created.
    for (const char* name : {"passwd", "group", "unrelated"}) {
      ::unlink((dir_ + "/" + name).c_str());
    }
    ::rmdir(dir_.c_str());
  }

  std::string dir_;
};

TEST_F(PasswdWatcherTest, FiresOnWatchedFileChange) {
  Latch latch;
  PasswdWatcher watcher(dir_, {"passwd", "group"}, [&] { latch.Fire(); });
  ASSERT_TRUE(watcher.Start());

  RenameInto(dir_, "passwd");

  EXPECT_TRUE(latch.WaitFor(1, std::chrono::seconds(3)))
      << "callback did not fire after watched file changed";
  watcher.Stop();
}

TEST_F(PasswdWatcherTest, IgnoresUnwatchedFile) {
  Latch latch;
  PasswdWatcher watcher(dir_, {"passwd", "group"}, [&] { latch.Fire(); });
  ASSERT_TRUE(watcher.Start());

  RenameInto(dir_, "unrelated");

  // Give the watcher ample time; an unwatched name must never trigger.
  EXPECT_FALSE(latch.WaitFor(1, std::chrono::milliseconds(800)));
  EXPECT_EQ(latch.Count(), 0);
  watcher.Stop();
}

TEST_F(PasswdWatcherTest, CoalescesBurstIntoOneCallback) {
  Latch latch;
  PasswdWatcher watcher(dir_, {"passwd", "group"}, [&] { latch.Fire(); });
  ASSERT_TRUE(watcher.Start());

  // One useradd touches passwd + group back to back; expect a single rebuild.
  RenameInto(dir_, "passwd");
  RenameInto(dir_, "group");

  ASSERT_TRUE(latch.WaitFor(1, std::chrono::seconds(3)));
  // Let any stragglers arrive, then assert the burst collapsed.
  EXPECT_FALSE(latch.WaitFor(2, std::chrono::milliseconds(800)));
  EXPECT_EQ(latch.Count(), 1);
  watcher.Stop();
}

TEST_F(PasswdWatcherTest, StopIsIdempotent) {
  PasswdWatcher watcher(dir_, {"passwd"}, [] {});
  ASSERT_TRUE(watcher.Start());
  watcher.Stop();
  watcher.Stop();  // must not hang or crash
}

}  // namespace test
}  // namespace vfs
}  // namespace client
}  // namespace dingofs
