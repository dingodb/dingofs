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
#include <functional>
#include <string>
#include <vector>

#include "common/status.h"

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
constexpr int kSetAttrFlags = (1 << 11);

enum FsStatus : uint8_t {
  kInit = 1,
  kNormal = 2,
  kDeleted = 3,
  kRecycling = 4,
};

inline std::string FsStatus2Str(const FsStatus& fs_status) {
  switch (fs_status) {
    case kInit:
      return "Init";
    case kNormal:
      return "Normal";
    case kDeleted:
      return "Deleted";
    case kRecycling:
      return "Recycling";
    default:
      return "Unknown";
  }
}

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
  uint32_t flags{0};
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

struct S3Info {
  std::string ak;
  std::string sk;
  std::string endpoint;
  std::string bucket_name;
};

struct RadosInfo {
  std::string user_name;
  std::string key;
  std::string mon_host;
  std::string pool_name;
  std::string cluster_name;
};

struct StorageInfo {
  StoreType store_type;
  S3Info s3_info;
  RadosInfo rados_info;
};

struct FsInfo {
  std::string name;
  uint32_t id;
  uint64_t chunk_size;
  uint64_t block_size;
  std::string uuid;
  StorageInfo storage_info;
  FsStatus status;
};

//  *off* should be any non-zero value that the vfs can use to
//  identify the current point in the directory stream. It does not
//  need to be the actual physical position. A value of zero is
//  reserved to mean "from the beginning", and should therefore never
//  be used (the first call to ReadDirHandler should be passed the
//  offset of the second directory entry).
//  When the handler returns false, it indicates that the
//  ReadDir operation should stop, and the *off* parameter should be
//  updated to the offset of the next entry that should be read on the
//  next call to ReadDirHandler.
using ReadDirHandler =
    std::function<bool(const DirEntry& dir_entry, uint64_t offset)>;

using DoneClosure = std::function<void(const Status& status)>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_META_H_