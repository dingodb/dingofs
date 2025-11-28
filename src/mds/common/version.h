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

#ifndef DINGOFS_MDS_COMMON_VERSION_H_
#define DINGOFS_MDS_COMMON_VERSION_H_

#include <string>
#include <utility>
#include <vector>

namespace dingofs {
namespace mds {

#ifndef GIT_VERSION
#define GIT_VERSION "unknown"
#endif

#ifndef GIT_TAG_NAME
#define GIT_TAG_NAME "unknown"
#endif

#ifndef GIT_COMMIT_USER
#define GIT_COMMIT_USER "unknown"
#endif

#ifndef GIT_COMMIT_MAIL
#define GIT_COMMIT_MAIL "unknown"
#endif

#ifndef GIT_COMMIT_TIME
#define GIT_COMMIT_TIME "unknown"
#endif

#ifndef DINGOFS_BUILD_TYPE
#define DINGOFS_BUILD_TYPE "unknown"
#endif

#ifndef GIT_LAST_COMMIT_ID
#define GIT_LAST_COMMIT_ID "unknown"
#endif

std::string DingoVersionString();
void DingoShowVersion();
void DingoLogVersion();
std::vector<std::pair<std::string, std::string>> DingoVersion();

std::string GetGitVersion();
std::string GetGitCommitHash();
std::string GetGitCommitTime();

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_VERSION_H_
