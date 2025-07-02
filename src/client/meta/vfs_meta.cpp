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

#include "client/meta/vfs_meta.h"

#include "fmt/format.h"

namespace dingofs {
namespace client {
namespace vfs {

std::string Attr2Str(const Attr& attr, bool with_parent) {
  if (!with_parent) {
    return fmt::format(
        "(ino: {}, mode: {}, nlink: {}, uid: {}, gid: {}, length: {}, "
        "rdev: {}, atime: {}, mtime: {}, ctime: {}, type: {})",
        attr.ino, attr.mode, attr.nlink, attr.uid, attr.gid, attr.length,
        attr.rdev, attr.atime, attr.mtime, attr.ctime, FileType2Str(attr.type));
  } else {
    std::string parents_str;
    for (size_t i = 0; i < attr.parents.size(); ++i) {
      parents_str += fmt::format("{}", attr.parents[i]);
      if (i < attr.parents.size() - 1) {
        parents_str += ", ";
      }
    }

    return fmt::format(
        "(ino: {}, mode: {}, nlink: {}, uid: {}, gid: {}, length: {}, "
        "rdev: {}, atime: {}, mtime: {}, ctime: {}, type: {}, parents: [{}])",
        attr.ino, attr.mode, attr.nlink, attr.uid, attr.gid, attr.length,
        attr.rdev, attr.atime, attr.mtime, attr.ctime, FileType2Str(attr.type),
        parents_str);
  }
}

std::string Slice2Str(const Slice& slice) {
  return fmt::format(
      "(id: {}, range: [{}-{}], compaction: {}, is_zero: {}, size: {})",
      slice.id, slice.offset, slice.End(), slice.compaction,
      slice.is_zero ? "true" : "false", slice.size);
}

std::string FsInfo2Str(const FsInfo& fs_info) {
  return fmt::format(
      "(name: {}, id: {}, chunk_size: {}, block_size: {}, uuid: {}, "
      "store_type: {})",
      fs_info.name, fs_info.id, fs_info.chunk_size, fs_info.block_size,
      fs_info.uuid, StoreType2Str(fs_info.storage_info.store_type));
}

std::string FsStat2Str(const FsStat& fs_stat) {
  return fmt::format(
      "(max_bytes: {}, used_bytes: {}, max_inodes: {}, used_inodes: {})",
      fs_stat.max_bytes, fs_stat.used_bytes, fs_stat.max_inodes,
      fs_stat.used_inodes);
}

}  // namespace vfs
}  // namespace client
}  // namespace dingofs