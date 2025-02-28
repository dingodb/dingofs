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

#ifndef DINGOFS_CLIENT_VFS_META_H_
#define DINGOFS_CLIENT_VFS_META_H_

#include <cstdint>
#include <sstream>
#include <string>
#include <vector>

namespace dingofs {
namespace client {
namespace vfs {

using Ino = uint64_t;

// 'to_set' flags in setattr
constexpr int kSetAttrMode = (1 << 0);
constexpr int kSetAttrUid = (1 << 1);
constexpr int kSetAttrGid = (1 << 2);
constexpr int kSetAttrSize = (1 << 3);
constexpr int kSetAttrAtime = (1 << 4);
constexpr int kSetAttrMtime = (1 << 5);
constexpr int kSetAttrAtimeNow = (1 << 7);
constexpr int kSetAttrMtimeNow = (1 << 8);
constexpr int kSetAttrCtime = (1 << 10);

enum FileType : uint8_t {
  kDirectory = 1,
  kSymlink = 2,
  kFile = 3,  // NOTE: match to pb TYPE_S3
};

inline std::string FileType2Str(const FileType& file_type) {
  switch (file_type) {
    case kDirectory:
      return "Directory";
    case kSymlink:
      return "Symlink";
    case kFile:
      return "File";
    default:
      return "Unknown";
  }
}

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
  // TODO: refact, maybe use separate key for hardlink
  std::vector<Ino> parents;
};

static std::string Attr2Str(const Attr& attr, bool with_parent = false) {
  std::ostringstream oss;
  oss << "Attr {"
      << " ino: " << attr.ino << ", mode: " << attr.mode
      << ", nlink: " << attr.nlink << ", uid: " << attr.uid
      << ", gid: " << attr.gid << ", length: " << attr.length
      << ", rdev: " << attr.rdev << ", atime: " << attr.atime
      << ", mtime: " << attr.mtime << ", ctime: " << attr.ctime
      << ", type: " << FileType2Str(attr.type);

  if (with_parent) {
    oss << ", parents: [";
    for (size_t i = 0; i < attr.parents.size(); ++i) {
      oss << attr.parents[i];
      if (i < attr.parents.size() - 1) {
        oss << ", ";
      }
    }
    oss << "] ";
  }

  oss << " }";

  return oss.str();
}

struct DirEntry {
  Ino ino;
  std::string name;
  Attr attr;
};

struct FsStat {
  uint64_t max_bytes;
  uint64_t used_bytes;
  uint64_t max_inodes;
  uint64_t used_inodes;
};

static std::string FsStat2Str(const FsStat& fs_stat) {
  std::ostringstream oss;
  oss << "FsStat {"
      << " max_bytes: " << fs_stat.max_bytes
      << ", used_bytes: " << fs_stat.used_bytes
      << ", max_inodes: " << fs_stat.max_inodes
      << ", used_inodes: " << fs_stat.used_inodes << " }";

  return oss.str();
}

// map pb chunkinfo
struct Slice {
  uint64_t id;          // slice id map to old pb chunkid
  uint64_t offset;      // offset in the file
  uint64_t length;      // length of the slice
  uint64_t compaction;  // compaction version
  bool is_zero;         // is zero slice
  uint64_t size;        // now same as length, maybe use for future or remove

  uint64_t End() const { return offset + length; }
};

static std::string Slice2Str(const Slice& slice) {
  std::ostringstream oss;
  oss << "{id: " << slice.id << ", range: [" << slice.offset << "-"
      << slice.End() << "]" << ", compaction: " << slice.compaction
      << ", is_zero: " << (slice.is_zero ? "true" : "false")
      << ", size: " << slice.size << " }";

  return oss.str();
}

enum StoreType : uint8_t {
  kS3 = 1,
  kRados = 2,
};

inline std::string StoreType2Str(const StoreType& store_type) {
  switch (store_type) {
    case kS3:
      return "S3";
    case kRados:
      return "Rados";
    default:
      return "Unknown";
  }
}

struct FsInfo {
  std::string name;
  uint32_t id;
  uint64_t chunk_size;
  uint64_t block_size;
  StoreType store_type;
  std::string uuid;
};

struct S3Info {
  std::string ak;
  std::string sk;
  std::string endpoint;
  std::string bucket;
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs
#endif  // DINGOFS_CLIENT_VFS_META_H_