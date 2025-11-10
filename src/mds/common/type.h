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

#ifndef DINGOFS_MDS_COMMON_TYPE_H_
#define DINGOFS_MDS_COMMON_TYPE_H_

#include <sys/types.h>

#include <cstdint>
#include <string>

#include "dingofs/mds.pb.h"
#include "fmt/format.h"

namespace dingofs {
namespace mds {

using Ino = uint64_t;
using AttrEntry = pb::mds::Inode;
using DentryEntry = pb::mds::Dentry;
using SliceEntry = pb::mds::Slice;
using ChunkEntry = pb::mds::Chunk;
using ChunkDescriptor = pb::mds::ChunkDescriptor;
using FsInfoEntry = pb::mds::FsInfo;
using TrashSliceEntry = pb::mds::TrashSlice;
using TrashSliceList = pb::mds::TrashSliceList;
using QuotaEntry = pb::mds::Quota;
using UsageEntry = pb::mds::Usage;
using MdsEntry = pb::mds::MDS;
using ClientEntry = pb::mds::Client;
using FileSessionEntry = pb::mds::FileSession;
using FsStatsDataEntry = pb::mds::FsStatsData;
using PartitionPolicy = pb::mds::PartitionPolicy;
using FsOpLog = pb::mds::FsOpLog;
using FileType = pb::mds::FileType;
using CacheMemberEntry = pb::mds::CacheGroupMember;
using HashPartitionEntry = pb::mds::HashPartition;
using BucketSetEntry = pb::mds::HashPartition::BucketSet;
using DeltaSliceEntry = pb::mds::WriteSliceRequest::DeltaSlice;
using RecycleProgress = pb::mds::RecycleProgress;
using ContextEntry = pb::mds::Context;

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
                     pb::mds::FileType_Name(attr.type()), attr.nlink(), attr.mode(), attr.uid(), attr.gid(),
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

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_TYPE_H_