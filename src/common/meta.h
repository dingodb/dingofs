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

#ifndef DINGOFS_COMMON_META_H_
#define DINGOFS_COMMON_META_H_

#include <cstdint>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace dingofs {

using Ino = uint64_t;

// Root inode number. Pass as `parent` to operate on the filesystem root.
static constexpr Ino kRootIno = 1;

// Bitmask flags for the `set` parameter of SetAttr().
// Combine with bitwise-OR to indicate which Attr fields to update.
static constexpr uint32_t kSetAttrMode     = 1 << 0;
static constexpr uint32_t kSetAttrUid      = 1 << 1;
static constexpr uint32_t kSetAttrGid      = 1 << 2;
static constexpr uint32_t kSetAttrSize     = 1 << 3;
static constexpr uint32_t kSetAttrAtime    = 1 << 4;
static constexpr uint32_t kSetAttrMtime    = 1 << 5;
static constexpr uint32_t kSetAttrAtimeNow = 1 << 7;
static constexpr uint32_t kSetAttrMtimeNow = 1 << 8;
static constexpr uint32_t kSetAttrCtime    = 1 << 10;
static constexpr uint32_t kSetAttrFlags    = 1 << 11;
static constexpr uint32_t kSetAttrNlink    = 1 << 12;

enum FileType : uint8_t {
  kDirectory = 1,
  kSymlink = 2,
  kFile = 3,
};

struct Attr {
  Ino ino{0};
  uint32_t mode{0};
  uint32_t nlink{0};
  uint32_t uid{0};
  uint32_t gid{0};
  uint64_t length{0};
  uint64_t rdev{0};
  uint64_t atime{0};
  uint64_t mtime{0};
  uint64_t ctime{0};
  FileType type;
  uint32_t flags{0};
  std::vector<Ino> parents;
  std::vector<std::pair<std::string, std::string>> xattrs;
  uint64_t version{0};
};

struct DirEntry {
  Ino ino;
  std::string name;
  Attr attr;
};

struct FsStat {
  int64_t max_bytes;
  int64_t used_bytes;
  int64_t max_inodes;
  int64_t used_inodes;
};

using ReadDirHandler =
    std::function<bool(const DirEntry& entry, uint64_t offset)>;

}  // namespace dingofs

#endif  // DINGOFS_COMMON_META_H_
