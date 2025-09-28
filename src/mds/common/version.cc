// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#include "mds/common/version.h"

#include <iostream>
#include <sstream>

#include "fmt/format.h"
#include "mds/common/logging.h"

namespace dingofs {
namespace mds {

static const std::string kGitCommitHash = GIT_VERSION;
static const std::string kGitTagName = GIT_TAG_NAME;
static const std::string kGitCommitUser = GIT_COMMIT_USER;
static const std::string kGitCommitMail = GIT_COMMIT_MAIL;
static const std::string kGitCommitTime = GIT_COMMIT_TIME;
static const std::string kMajorVersion = MAJOR_VERSION;
static const std::string kMinorVersion = MINOR_VERSION;
static const std::string kDingoFsBuildType = DINGOFS_BUILD_TYPE;
static bool kUseTcmalloc = false;
static bool kUseProfiler = false;
static bool kUseSanitizer = false;

static std::string GetBuildFlag() {
#ifdef LINK_TCMALLOC
  kUseTcmalloc = true;
#else
  kUseTcmalloc = false;
#endif

#ifdef BRPC_ENABLE_CPU_PROFILER
  kUseProfiler = true;
#else
  kUseProfiler = false;
#endif

#ifdef USE_SANITIZE
  kUseSanitizer = true;
#else
  kUseSanitizer = false;
#endif

  return fmt::format("DINGOFS LINK_TCMALLOC:[{}] BRPC_ENABLE_CPU_PROFILER:[{}] USE_SANITIZE:[{}]",
                     kUseTcmalloc ? "ON" : "OFF", kUseProfiler ? "ON" : "OFF", kUseSanitizer ? "ON" : "OFF");
}

std::string DingoVersionString() {
  std::ostringstream oss;
  oss << fmt::format("DINGOFS VERSION:[{}-{}]\n", kMajorVersion.c_str(), kMinorVersion.c_str());
  oss << fmt::format("DINGOFS GIT_TAG_VERSION:[{}]\n", kGitTagName.c_str());
  oss << fmt::format("DINGOFS GIT_COMMIT_HASH:[{}]\n", kGitCommitHash.c_str());
  oss << fmt::format("DINGOFS BUILD_TYPE:[{}]\n", kDingoFsBuildType.c_str());
  oss << GetBuildFlag() << "\n";

  return oss.str();
}

void DingoShowVersion() { std::cout << DingoVersionString(); }

void DingoLogVersion() {
  DINGO_LOG(INFO) << "DINGOFS VERSION:[" << kMajorVersion << "-" << kMinorVersion << "]";
  DINGO_LOG(INFO) << "DINGOFS GIT_TAG_VERSION:[" << kGitTagName << "]";
  DINGO_LOG(INFO) << "DINGOFS GIT_COMMIT_HASH:[" << kGitCommitHash << "]";
  DINGO_LOG(INFO) << "DINGOFS BUILD_TYPE:[" << kDingoFsBuildType << "]";
  DINGO_LOG(INFO) << GetBuildFlag();
  DINGO_LOG(INFO) << "PID: " << getpid();
}

std::vector<std::pair<std::string, std::string>> DingoVersion() {
  std::vector<std::pair<std::string, std::string>> result;
  result.emplace_back("VERSION", fmt::format("{}.{}", kMajorVersion, kMinorVersion));
  result.emplace_back("TAG_VERSION", kGitTagName);
  result.emplace_back("COMMIT_HASH", kGitCommitHash);
  result.emplace_back("COMMIT_USER", kGitCommitUser);
  result.emplace_back("COMMIT_MAIL", kGitCommitMail);
  result.emplace_back("COMMIT_TIME", kGitCommitTime);
  result.emplace_back("BUILD_TYPE", kDingoFsBuildType);

  return result;
}

std::string GetGitVersion() { return fmt::format("{}.{}", kMajorVersion, kMinorVersion); }

std::string GetGitCommitHash() { return kGitCommitHash; }

std::string GetGitCommitTime() { return kGitCommitTime; }

}  // namespace mds
}  // namespace dingofs
