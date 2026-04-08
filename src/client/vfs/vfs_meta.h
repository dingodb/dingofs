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

#include "common/meta.h"
#include "common/status.h"

namespace dingofs {
namespace client {
namespace vfs {

using Ino = dingofs::Ino;
using FileType = dingofs::FileType;
using Attr = dingofs::Attr;
using DirEntry = dingofs::DirEntry;
using FsStat = dingofs::FsStat;
using ReadDirHandler = dingofs::ReadDirHandler;

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

inline std::string FileType2Str(const FileType& file_type) {
  switch (file_type) {
    case dingofs::kDirectory:
      return "Directory";
    case dingofs::kSymlink:
      return "Symlink";
    case dingofs::kFile:
      return "File";
    default:
      return "Unknown";
  }
}

// Slice describes a contiguous range of physical data within a chunk.
// Fields align with proto/dingofs/mds.proto::Slice (uint32 in proto).
// Use signed int32_t internally so that subtraction bugs surface as
// negative values instead of silent wrap-around.
// Actual values are bounded by chunk_size (typically 64 MB), well within
// int32_t range (2 GB).
struct Slice {
  uint64_t id{0};   // slice ID (globally unique, no arithmetic)
  int32_t pos{0};   // start byte position within the owning chunk
  int32_t size{0};  // total physical data size of the slice
  int32_t off{0};   // read offset within the slice (for CopyFileRange/Clone)
  int32_t len{0};   // logical length of this mapping
};

enum StoreType : uint8_t {
  kS3 = 1,
  kRados = 2,
  kLocalFile = 3,
};

inline std::string StoreType2Str(const StoreType& store_type) {
  switch (store_type) {
    case kS3:
      return "S3";
    case kRados:
      return "Rados";
    case kLocalFile:
      return "LocalFile";
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

struct LocalFileInfo {
  std::string path;
};

struct StorageInfo {
  StoreType store_type;
  S3Info s3_info;
  RadosInfo rados_info;
  LocalFileInfo file_info;
};

struct FsInfo {
  std::string name;
  uint32_t id;
  int32_t chunk_size;
  int32_t block_size;
  std::string uuid;
  StorageInfo storage_info;
  FsStatus status;
};

using DoneClosure = std::function<void(const Status& status)>;

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_META_H_