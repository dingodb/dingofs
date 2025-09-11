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

#ifndef DINGOFS_MDSV2_COMMON_TYPE_H_
#define DINGOFS_MDSV2_COMMON_TYPE_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"

namespace dingofs {
namespace mdsv2 {

using Ino = uint64_t;
using AttrEntry = pb::mdsv2::Inode;
using DentryEntry = pb::mdsv2::Dentry;
using SliceEntry = pb::mdsv2::Slice;
using ChunkEntry = pb::mdsv2::Chunk;
using FsInfoEntry = pb::mdsv2::FsInfo;
using TrashSliceEntry = pb::mdsv2::TrashSlice;
using TrashSliceList = pb::mdsv2::TrashSliceList;
using QuotaEntry = pb::mdsv2::Quota;
using UsageEntry = pb::mdsv2::Usage;
using MdsEntry = pb::mdsv2::MDS;
using ClientEntry = pb::mdsv2::Client;
using FileSessionEntry = pb::mdsv2::FileSession;
using FsStatsDataEntry = pb::mdsv2::FsStatsData;
using PartitionPolicy = pb::mdsv2::PartitionPolicy;
using FsOpLog = pb::mdsv2::FsOpLog;
using FileType = pb::mdsv2::FileType;
using CacheMemberEntry = pb::mdsv2::CacheGroupMember;
using HashPartitionEntry = pb::mdsv2::HashPartition;
using BucketSetEntry = pb::mdsv2::HashPartition::BucketSet;
using DeltaSliceEntry = pb::mdsv2::WriteSliceRequest::DeltaSlice;
using RecycleProgress = pb::mdsv2::RecycleProgress;

struct Range {
  std::string start;
  std::string end;

  std::string ToString() const { return fmt::format("[{}, {})", start, end); }
};

struct IntRange {
  uint64_t start;
  uint64_t end;

  std::string ToString() const { return fmt::format("[{}, {})", start, end); }
};

inline bool IsDir(Ino ino) { return (ino & 1) == 1; }
inline bool IsFile(Ino ino) { return (ino & 1) == 0; }

inline std::string DescribeAttr(const AttrEntry& attr) {
  auto parents_func = [](const auto& parents) {
    std::string result;
    for (const auto& parent : parents) {
      if (!result.empty()) {
        result += ",";
      }
      result += std::to_string(parent);
    }
    return result;
  };

  return fmt::format("{}:{}:{}:{}:{}:{}:{}:{} v{} p{} t{}:{}:{}", attr.fs_id(), attr.ino(),
                     pb::mdsv2::FileType_Name(attr.type()), attr.nlink(), attr.mode(), attr.uid(), attr.gid(),
                     attr.length(), attr.version(), parents_func(attr.parents()), attr.ctime(), attr.mtime(),
                     attr.atime());
}

struct S3Info {
  // s3 info
  std::string ak;
  std::string sk;           // S3 access key and secret key
  std::string endpoint;     // S3 endpoint
  std::string bucket_name;  // S3 bucket name
  std::string object_name;  // S3 object name

  bool Validate() const {
    return !ak.empty() && !sk.empty() && !endpoint.empty() && !bucket_name.empty() && !object_name.empty();
  }

  std::string ToString() const {
    return fmt::format("ak: {}, sk: {}, endpoint: {}, bucket_name: {}, object_name: {}", ak, sk, endpoint, bucket_name,
                       object_name);
  }
};

struct RadosInfo {
  std::string mon_host;
  std::string user_name;
  std::string key;
  std::string pool_name;
  std::string cluster_name{"ceph"};
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_TYPE_H_