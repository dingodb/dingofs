/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-06-03
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_COMMON_CONST_H_
#define DINGOFS_SRC_COMMON_CONST_H_

#include <butil/file_util.h>
#include <fmt/format.h>
#include <unistd.h>

#include <cstdint>
#include <string>
#include <unordered_map>

namespace dingofs {

const uint64_t kRootIno = 1;
const uint64_t kRootParentIno = 0;

const uint64_t kRecycleIno = 2;
const char kRecycleName[] = ".recycle";

const uint64_t kStatsIno = 0x7FFFFFFF00000001;
const char kStatsName[] = ".stats";

inline bool IsInternalNode(uint64_t ino) {
  return ino == kStatsIno || ino == kRecycleIno || ino == kRootIno;
}

inline bool IsInternalName(const std::string& name) {
  return name == kStatsName || name == kRecycleName;
}

// set inode attribute flags
constexpr uint32_t kSetAttrMode = 1 << 0;
constexpr uint32_t kSetAttrUid = 1 << 1;
constexpr uint32_t kSetAttrGid = 1 << 2;
constexpr uint32_t kSetAttrSize = 1 << 3;
constexpr uint32_t kSetAttrAtime = 1 << 4;
constexpr uint32_t kSetAttrMtime = 1 << 5;
constexpr uint32_t kSetAttrAtimeNow = 1 << 7;
constexpr uint32_t kSetAttrMtimeNow = 1 << 8;
constexpr uint32_t kSetAttrCtime = 1 << 10;
constexpr uint32_t kSetAttrFlags = 1 << 11;
constexpr uint32_t kSetAttrNlink = 1 << 12;

const int kEmptyDirMinLinkNum = 2;

// meta table names
const std::string kMetaTableName = "dingofs-meta";
const std::string kFsStatsTableName = "dingofs-fsstats";

inline std::string GenFsMetaTableName(const std::string& fs_name) {
  return "dingofs-fsmeta[" + fs_name + "]";
}

inline std::string GenFsMetaTableName(uint32_t cluster_id,
                                      const std::string& fs_name) {
  return "dingofs-fsmeta[" + std::to_string(cluster_id) + "][" + fs_name + "]";
}

static constexpr uint64_t kKiB = 1024ULL;
static constexpr uint64_t kMiB = 1024ULL * kKiB;
static constexpr uint64_t kGiB = 1024ULL * kMiB;
static constexpr uint64_t kTiB = 1024ULL * kGiB;

}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_CONST_H_
