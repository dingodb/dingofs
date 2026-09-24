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

#include <bvar/variable.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "common/version.h"

namespace dingofs {
namespace {

class VersionLogSink final : public google::LogSink {
 public:
  VersionLogSink() { google::AddLogSink(this); }
  ~VersionLogSink() override { google::RemoveLogSink(this); }

  void send(google::LogSeverity, const char*, const char*, int,
            const google::LogMessageTime&, const char* message,
            size_t message_len) override {
    std::string line(message, message_len);
    if (line.rfind("DINGOFS VERSION:[", 0) == 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      version_ = std::move(line);
    } else if (line.rfind("DINGOFS BUILD_SOURCE:[", 0) == 0) {
      std::lock_guard<std::mutex> lock(mutex_);
      build_source_ = std::move(line);
    }
  }

  std::string Version() {
    std::lock_guard<std::mutex> lock(mutex_);
    return version_;
  }

  std::string BuildSource() {
    std::lock_guard<std::mutex> lock(mutex_);
    return build_source_;
  }

 private:
  std::mutex mutex_;
  std::string version_;
  std::string build_source_;
};

}  // namespace

TEST(VersionTest, AllSurfacesIdentifyTheSameBuild) {
  const auto fields = DingoVersion();
  const std::map<std::string, std::string> metadata(fields.begin(),
                                                    fields.end());
  const std::string identity =
      metadata.at("BRANCH") + "-" + metadata.at("COMMIT_HASH");
  const std::string expected = GetGitVersion();
  EXPECT_TRUE(expected == identity || expected == identity + "-dirty" ||
              expected == identity + "-unknown-state");

  const std::string& source = metadata.at("BUILD_SOURCE");
  ASSERT_TRUE(source == "ci/cd" || source == "local");
  EXPECT_EQ(DingoShortVersionString(), expected + " [" + source + "]");
  const std::string version_line = "DINGOFS VERSION:[" + expected + "]";
  const std::string full_version = DingoVersionString();
  EXPECT_EQ(full_version.substr(0, full_version.find('\n')), version_line);
  const std::string source_line = "DINGOFS BUILD_SOURCE:[" + source + "]";
  EXPECT_NE(full_version.find(source_line + "\n"), std::string::npos);

  gflags::FlagSaver flag_saver;
  FLAGS_minloglevel = google::GLOG_INFO;
  VersionLogSink sink;
  DingoLogVersion();
  EXPECT_EQ(sink.Version(), version_line);
  EXPECT_EQ(sink.BuildSource(), source_line);

  ExposeDingoVersion();
  EXPECT_EQ(bvar::Variable::describe_exposed("dingo_version"), expected);
}

}  // namespace dingofs
