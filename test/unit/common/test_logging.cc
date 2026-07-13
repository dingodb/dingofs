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

#include "common/directory.h"
#include "common/logging.h"

#include <cstdlib>
#include <gtest/gtest.h>

namespace dingofs {

namespace {

// RAII helper restoring Logger's global mutable state so these tests don't
// bleed configuration into tests that run later in the same binary.
class LoggerStateGuard {
 public:
  LoggerStateGuard()
      : min_log_level_(Logger::GetMinLogLevel()),
        min_verbose_level_(Logger::GetMinVerboseLevel()),
        log_buf_secs_(Logger::GetLogBuffSecs()),
        max_log_size_(Logger::GetMaxLogSize()),
        stop_on_full_disk_(Logger::GetStoppingWhenDiskFull()) {}

  ~LoggerStateGuard() {
    Logger::SetMinLogLevel(min_log_level_);
    Logger::SetMinVerboseLevel(min_verbose_level_);
    Logger::SetLogBuffSecs(log_buf_secs_);
    Logger::SetMaxLogSize(max_log_size_);
    Logger::SetStoppingWhenDiskFull(stop_on_full_disk_);
  }

 private:
  int min_log_level_;
  int min_verbose_level_;
  int log_buf_secs_;
  int32_t max_log_size_;
  bool stop_on_full_disk_;
};

}  // namespace

// ------------------------- directory.h -------------------------

TEST(DirectoryTest, GetBaseDirHonorsEnvOverride) {
  ASSERT_EQ(::setenv("DINGOFS_BASE_DIR", "/tmp/dingofs-env-test", 1), 0);
  EXPECT_EQ(GetBaseDir(), "/tmp/dingofs-env-test");
  ::unsetenv("DINGOFS_BASE_DIR");
}

TEST(DirectoryTest, GetBaseDirFallsBackToHomeWhenEnvUnset) {
  ::unsetenv("DINGOFS_BASE_DIR");
  if (::getuid() == 0) {
    GTEST_SKIP() << "root falls back to /var/dingofs, not $HOME";
  }
  EXPECT_EQ(GetBaseDir(), Helper::GetHomeDir() + "/.dingofs");
}

TEST(DirectoryTest, GetDefaultDirAppendsSubDir) {
  ASSERT_EQ(::setenv("DINGOFS_BASE_DIR", "/tmp/dingofs-env-test", 1), 0);
  EXPECT_EQ(GetDefaultDir(kLogDir), "/tmp/dingofs-env-test/log");
  ::unsetenv("DINGOFS_BASE_DIR");
}

// ------------------------- logging.h -------------------------

TEST(LoggerTest, MinLogLevelGetterReflectsSetter) {
  LoggerStateGuard guard;
  Logger::SetMinLogLevel(2);
  EXPECT_EQ(Logger::GetMinLogLevel(), 2);
}

TEST(LoggerTest, MinVerboseLevelGetterReflectsSetter) {
  LoggerStateGuard guard;
  Logger::SetMinVerboseLevel(5);
  EXPECT_EQ(Logger::GetMinVerboseLevel(), 5);
}

TEST(LoggerTest, LogBuffSecsGetterReflectsSetter) {
  LoggerStateGuard guard;
  Logger::SetLogBuffSecs(42);
  EXPECT_EQ(Logger::GetLogBuffSecs(), 42);
}

TEST(LoggerTest, MaxLogSizeGetterReflectsSetter) {
  LoggerStateGuard guard;
  Logger::SetMaxLogSize(123);
  EXPECT_EQ(Logger::GetMaxLogSize(), 123);
}

TEST(LoggerTest, StoppingWhenDiskFullGetterReflectsSetter) {
  LoggerStateGuard guard;
  Logger::SetStoppingWhenDiskFull(false);
  EXPECT_FALSE(Logger::GetStoppingWhenDiskFull());
  Logger::SetStoppingWhenDiskFull(true);
  EXPECT_TRUE(Logger::GetStoppingWhenDiskFull());
}

TEST(LoggerTest, ChangeGlogLevelByNameMapsDebugToVerboseZero) {
  LoggerStateGuard guard;
  Logger::ChangeGlogLevel("DEBUG");
  EXPECT_EQ(Logger::GetMinLogLevel(), 0);
}

TEST(LoggerTest, ChangeGlogLevelByNameIsCaseInsensitive) {
  LoggerStateGuard guard;
  Logger::ChangeGlogLevel("error");
  EXPECT_EQ(Logger::GetMinLogLevel(),
           static_cast<int>(LogLevel::kERROR) - 1);
}

TEST(LoggerTest, ChangeGlogLevelByNameDefaultsUnknownToInfo) {
  LoggerStateGuard guard;
  Logger::ChangeGlogLevel("not-a-level");
  EXPECT_EQ(Logger::GetMinLogLevel(), static_cast<int>(LogLevel::kINFO) - 1);
}

TEST(LoggerTest, ChangeGlogLevelByEnumSetsVerboseLevel) {
  LoggerStateGuard guard;
  Logger::ChangeGlogLevel(LogLevel::kWARNING, 7);
  EXPECT_EQ(Logger::GetMinLogLevel(), static_cast<int>(LogLevel::kWARNING) - 1);
  EXPECT_EQ(Logger::GetMinVerboseLevel(), 7);
}

TEST(LoggerTest, ChangeGlogLevelByEnumDebugMapsToVerboseZero) {
  LoggerStateGuard guard;
  Logger::ChangeGlogLevel(LogLevel::kDEBUG, 3);
  EXPECT_EQ(Logger::GetMinLogLevel(), 0);
}

}  // namespace dingofs
