// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "common/version.h"

#include <iostream>
#include <sstream>

#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {

static const std::string kGitVersion = GIT_VERSION;
static const std::string kGitTagName = GIT_TAG_NAME;
static const std::string kGitBranchName = GIT_BRANCH_NAME;
static const std::string kGitLastCommit = GIT_LAST_COMMIT_ID;
static const std::string kGitCommitUser = GIT_COMMIT_USER;
static const std::string kGitCommitMail = GIT_COMMIT_MAIL;
static const std::string kGitCommitTime = GIT_COMMIT_TIME;
static const std::string kDingoFsBuildType = DINGOFS_BUILD_TYPE;
static bool kUseTcmalloc = false;
static bool kUseProfiler = false;
static bool kUseSanitizer = false;
static bool kUseCICDBuild = false;

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

  return fmt::format(
      "DINGOFS LINK_TCMALLOC:[{}] BRPC_ENABLE_CPU_PROFILER:[{}] "
      "USE_SANITIZE:[{}]",
      kUseTcmalloc ? "ON" : "OFF", kUseProfiler ? "ON" : "OFF",
      kUseSanitizer ? "ON" : "OFF");
}

std::string DingoVersionString() {
  std::ostringstream oss;
  oss << fmt::format("DINGOFS VERSION:[{}]\n", kGitVersion.c_str());
  oss << fmt::format("DINGOFS GIT_LAST_TAG:[{}]\n", kGitTagName.c_str());
  oss << fmt::format("DINGOFS GIT_BRANCH_NAME:[{}]\n", kGitBranchName.c_str());
  oss << fmt::format("DINGOFS GIT_COMMIT_HASH:[{}]\n", kGitLastCommit.c_str());
  oss << fmt::format("DINGOFS BUILD_TYPE:[{}]\n", kDingoFsBuildType.c_str());
  oss << GetBuildFlag() << "\n";

  return oss.str();
}

std::string DingoShortVersionString() {
#ifdef USE_CICD_BUILD
  kUseCICDBuild = true;
#else
  kUseCICDBuild = false;
#endif

  return fmt::format("{}, {} build-{}:{} + {}", kGitTagName,
                     kUseCICDBuild ? "CI/CD" : "Local", kGitBranchName,
                     kGitLastCommit, kDingoFsBuildType);
}

void DingoLogVersion() {
  LOG(INFO) << "DINGOFS VERSION:[" << kGitVersion << "]";
  LOG(INFO) << "DINGOFS GIT_LAST_TAG:[" << kGitTagName << "]";
  LOG(INFO) << "DINGOFS GIT_BRANCH_NAME:[" << kGitBranchName << "]";
  LOG(INFO) << "DINGOFS GIT_COMMIT_HASH:[" << kGitLastCommit << "]";
  LOG(INFO) << "DINGOFS BUILD_TYPE:[" << kDingoFsBuildType << "]";
  LOG(INFO) << GetBuildFlag();
  LOG(INFO) << "PID: " << getpid();
}

std::vector<std::pair<std::string, std::string>> DingoVersion() {
  std::vector<std::pair<std::string, std::string>> result;
  result.emplace_back("LAST_TAG", kGitTagName);
  result.emplace_back("COMMIT_HASH", kGitLastCommit);
  result.emplace_back("COMMIT_USER", kGitCommitUser);
  result.emplace_back("COMMIT_MAIL", kGitCommitMail);
  result.emplace_back("COMMIT_TIME", kGitCommitTime);
  result.emplace_back("BUILD_TYPE", kDingoFsBuildType);

  return result;
}

std::string GetGitVersion() { return kGitVersion; }

std::string GetGitCommitHash() { return kGitLastCommit; }

std::string GetGitCommitTime() { return kGitCommitTime; }
}  // namespace dingofs
