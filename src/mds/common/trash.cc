// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mds/common/trash.h"

#include <sys/stat.h>

#include <ctime>

#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/type.h"
#include "utils/time.h"

namespace dingofs {
namespace mds {

static constexpr size_t kMaxNameLen = 255;
static constexpr const char* kBucketFormat = "%Y-%m-%d-%H";

std::string FormatTrashBucketName(uint64_t timestamp_s) {
  std::time_t t = static_cast<std::time_t>(timestamp_s);
  std::tm tm{};
  gmtime_r(&t, &tm);
  char buf[32];
  std::strftime(buf, sizeof(buf), kBucketFormat, &tm);
  return std::string(buf);
}

uint64_t ParseTrashBucketName(const std::string& name) {
  std::tm tm{};
  if (strptime(name.c_str(), kBucketFormat, &tm) == nullptr) {
    return 0;
  }
  return static_cast<uint64_t>(timegm(&tm));
}

std::string BuildTrashEntryName(Ino parent_ino, Ino file_ino, const std::string& original_name) {
  std::string result = fmt::format("{}-{}-{}", parent_ino, file_ino, original_name);
  if (result.size() > kMaxNameLen) {
    LOG(WARNING) << fmt::format("trash entry name truncated from {} to {} bytes: {}", result.size(), kMaxNameLen,
                                result.substr(0, 64));
    result.resize(kMaxNameLen);
  }
  return result;
}

bool ParseTrashEntryName(const std::string& trash_name, Ino& parent_ino, Ino& file_ino, std::string& original_name) {
  auto pos1 = trash_name.find('-');
  if (pos1 == std::string::npos) return false;

  auto pos2 = trash_name.find('-', pos1 + 1);
  if (pos2 == std::string::npos) return false;

  try {
    parent_ino = std::stoull(trash_name.substr(0, pos1));
    file_ino = std::stoull(trash_name.substr(pos1 + 1, pos2 - pos1 - 1));
  } catch (...) {
    return false;
  }

  original_name = trash_name.substr(pos2 + 1);
  return true;
}

Ino ParseTrashEntryName(const std::string& trash_name) {
  auto pos1 = trash_name.find('-');
  if (pos1 == std::string::npos) return false;

  try {
    return std::stoull(trash_name.substr(0, pos1));

  } catch (...) {
    return 0;
  }

  return 0;
}

AttrEntry BuildTrashInodeAttr(uint32_t fs_id, uint64_t fs_create_time_ns) {
  AttrEntry attr;
  attr.set_fs_id(fs_id);
  attr.set_ino(kTrashInodeId);
  attr.set_type(pb::mds::FileType::DIRECTORY);
  attr.set_mode(S_IFDIR | 0555);
  attr.set_nlink(2);
  attr.set_length(4096);
  attr.set_uid(0);
  attr.set_gid(0);
  // Pin to fs creation time (ns) so the synthesized attr is stable across
  // calls and across MDSes: kernel/client attr caches hit, replies are
  // bytewise identical regardless of which MDS handled the request.
  attr.set_ctime(fs_create_time_ns);
  attr.set_mtime(fs_create_time_ns);
  attr.set_atime(fs_create_time_ns);
  attr.set_version(1);
  return attr;
}

// Hour-bucket attr is intentionally write-once after the initial Put:
// bumping it on every trash-move-in would serialize cluster-wide unlinks on
// one hot key, and 0555 + non-root EPERM means no POSIX observer cares
// about nlink/mtime/version drift.
AttrEntry BuildSubTrashBucketAttr(uint32_t fs_id, Ino sub_trash_ino) {
  AttrEntry attr;
  attr.set_fs_id(fs_id);
  attr.set_ino(sub_trash_ino);
  attr.set_type(pb::mds::FileType::DIRECTORY);
  attr.set_mode(S_IFDIR | 0555);
  attr.set_nlink(2);
  attr.set_length(4096);
  attr.set_uid(0);
  attr.set_gid(0);
  const uint64_t now_ns = utils::TimestampNs();
  attr.set_ctime(now_ns);
  attr.set_mtime(now_ns);
  attr.set_atime(now_ns);
  attr.set_version(1);
  attr.add_parents(kTrashInodeId);
  return attr;
}

}  // namespace mds
}  // namespace dingofs
