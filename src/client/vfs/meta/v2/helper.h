// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_

#include <cstdint>
#include <vector>

#include "client/meta/vfs_meta.h"
#include "glog/logging.h"
#include "mdsv2/common/type.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

const uint32_t kMaxHostNameLength = 255;

class Helper {
 public:
  static StoreType ToStoreType(pb::mdsv2::FsType fs_type) {
    switch (fs_type) {
      case pb::mdsv2::FsType::S3:
        return StoreType::kS3;

      case pb::mdsv2::FsType::RADOS:
        return StoreType::kRados;

      default:
        CHECK(false) << "unknown fs type: " << pb::mdsv2::FsType_Name(fs_type);
    }
  }

  static FileType ToFileType(pb::mdsv2::FileType type) {
    switch (type) {
      case pb::mdsv2::FileType::FILE:
        return FileType::kFile;

      case pb::mdsv2::FileType::DIRECTORY:
        return FileType::kDirectory;

      case pb::mdsv2::FileType::SYM_LINK:
        return FileType::kSymlink;

      default:
        CHECK(false) << "unknown file type: " << type;
    }
  }

  static pb::mdsv2::FileType ToFileType(FileType type) {
    switch (type) {
      case FileType::kFile:
        return pb::mdsv2::FileType::FILE;

      case FileType::kDirectory:
        return pb::mdsv2::FileType::DIRECTORY;

      case FileType::kSymlink:
        return pb::mdsv2::FileType::SYM_LINK;

      default:
        CHECK(false) << "unknown file type: " << type;
    }
  }

  static Attr ToAttr(const mdsv2::AttrEntry& inode) {
    Attr out_attr;

    out_attr.ino = inode.ino();
    out_attr.mode = inode.mode();
    out_attr.nlink = inode.nlink();
    out_attr.uid = inode.uid();
    out_attr.gid = inode.gid();
    out_attr.length = inode.length();
    out_attr.rdev = inode.rdev();
    out_attr.atime = inode.atime();
    out_attr.mtime = inode.mtime();
    out_attr.ctime = inode.ctime();
    out_attr.type = ToFileType(inode.type());

    for (const auto& parent : inode.parents()) {
      out_attr.parents.push_back(parent);
    }

    return out_attr;
  }

  static mdsv2::AttrEntry ToAttr(const Attr& attr) {
    mdsv2::AttrEntry out_attr;

    out_attr.set_ino(attr.ino);
    out_attr.set_mode(attr.mode);
    out_attr.set_nlink(attr.nlink);
    out_attr.set_uid(attr.uid);
    out_attr.set_gid(attr.gid);
    out_attr.set_length(attr.length);
    out_attr.set_rdev(attr.rdev);
    out_attr.set_atime(attr.atime);
    out_attr.set_mtime(attr.mtime);
    out_attr.set_ctime(attr.ctime);
    out_attr.set_type(ToFileType(attr.type));

    for (const auto& parent : attr.parents) {
      out_attr.add_parents(parent);
    }

    return std::move(out_attr);
  }

  static DirEntry ToDirEntry(const pb::mdsv2::ReadDirResponse::Entry& entry) {
    DirEntry out_entry;
    out_entry.name = entry.name();
    out_entry.ino = entry.ino();
    out_entry.attr = ToAttr(entry.inode());

    return std::move(out_entry);
  }

  static Slice ToSlice(const mdsv2::SliceEntry& slice) {
    Slice out_slice;

    out_slice.id = slice.id();
    out_slice.offset = slice.offset();
    out_slice.length = slice.len();
    out_slice.compaction = slice.compaction_version();
    out_slice.is_zero = slice.zero();
    out_slice.size = slice.size();

    return out_slice;
  }

  static mdsv2::SliceEntry ToSlice(const Slice& slice) {
    pb::mdsv2::Slice out_slice;

    out_slice.set_id(slice.id);
    out_slice.set_offset(slice.offset);
    out_slice.set_len(slice.length);
    out_slice.set_compaction_version(slice.compaction);
    out_slice.set_zero(slice.is_zero);
    out_slice.set_size(slice.size);

    return out_slice;
  }

  static std::string GetHostName() {
    char hostname[kMaxHostNameLength];
    int ret = gethostname(hostname, kMaxHostNameLength);
    if (ret < 0) {
      LOG(ERROR) << "[meta.filesystem] get hostname fail, ret=" << ret;
      return "";
    }

    return std::string(hostname);
  }

  static S3Info ToS3Info(const pb::mdsv2::S3Info& s3_info) {
    S3Info result;
    result.ak = s3_info.ak();
    result.sk = s3_info.sk();
    result.endpoint = s3_info.endpoint();
    result.bucket_name = s3_info.bucketname();
    return result;
  }

  static RadosInfo ToRadosInfo(const pb::mdsv2::RadosInfo& rados_info) {
    RadosInfo result;
    result.user_name = rados_info.user_name();
    result.key = rados_info.key();
    result.mon_host = rados_info.mon_host();
    result.pool_name = rados_info.pool_name();
    result.cluster_name = rados_info.cluster_name();
    return result;
  }

  static mdsv2::DeltaSliceEntry ToDeltaSliceEntry(
      uint64_t chunk_index, const std::vector<Slice>& slices) {
    mdsv2::DeltaSliceEntry delta_slice_entry;

    delta_slice_entry.set_chunk_index(chunk_index);
    for (const auto& slice : slices) {
      *delta_slice_entry.add_slices() = Helper::ToSlice(slice);
    }

    return std::move(delta_slice_entry);
  }

  static uint64_t CalLength(const std::vector<Slice>& slices) {
    uint64_t length = 0;
    for (const auto& slice : slices) {
      length = std::max(length, slice.offset + slice.length);
    }

    return length;
  };

  static std::vector<uint64_t> GetSliceIds(const std::vector<Slice>& slices) {
    std::vector<uint64_t> slice_ids;
    slice_ids.reserve(slices.size());
    for (const auto& slice : slices) {
      slice_ids.push_back(slice.id);
    }
    return std::move(slice_ids);
  }

  static std::vector<uint64_t> GetSliceIds(
      const std::vector<mdsv2::SliceEntry>& slices) {
    std::vector<uint64_t> slice_ids;
    slice_ids.reserve(slices.size());
    for (const auto& slice : slices) {
      slice_ids.push_back(slice.id());
    }
    return slice_ids;
  }
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_HELPER_H_