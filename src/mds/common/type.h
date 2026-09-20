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

#include <glog/logging.h>
#include <sys/types.h>

#include <algorithm>
#include <cstdint>
#include <string>

#include "absl/container/inlined_vector.h"
#include "common/const.h"
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
using BatchReadSliceReqEntry = pb::mds::BatchReadSliceRequest::Entry;
using BatchReadSliceResEntry = pb::mds::BatchReadSliceResponse::Entry;
using FsInfoEntry = pb::mds::FsInfo;
using TrashSliceEntry = pb::mds::TrashSlice;
using TrashSliceList = pb::mds::TrashSliceList;
using QuotaEntry = pb::mds::Quota;
using UsageEntry = pb::mds::Usage;
using DirStatEntry = pb::mds::DirStat;

// in-memory single-level dir-stat delta (length/inodes/dirs)
struct DirStatDelta {
  int64_t length{0};
  int64_t inodes{0};
  int64_t dirs{0};
};

// a single buffered dir-stat delta tagged with a strictly-monotonic logical
// timestamp. The timestamp lets the flush/recompute paths compact a time-bounded
// prefix of the per-directory delta log (see DirStatManager) instead of dropping
// the whole buffer -- mirrors quota's Usage{bytes,inodes,time_ns}.
struct DirStatDeltaEntry {
  int64_t length{0};
  int64_t inodes{0};
  int64_t dirs{0};
  uint64_t time_ns{0};
};
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
using AttrMutationEntry = pb::mds::AttrMutation;
using SliceRefEntry = pb::mds::SliceRef;

struct KeyValue {
  enum class OpType : uint8_t {
    kPut = 0,
    kDelete = 1,
  };

  static std::string OpTypeName(OpType op_type) {
    switch (op_type) {
      case OpType::kPut:
        return "Put";
      case OpType::kDelete:
        return "Delete";
      default:
        return "Unknown";
    }
  }

  OpType opt_type{OpType::kPut};
  std::string key;
  std::string value;
};

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

inline std::string DescribeAttrMutation(const AttrMutationEntry& attr_mutation) {
  return fmt::format("{}:{}:{}:{}:{}:{}", attr_mutation.ino(), attr_mutation.index(), attr_mutation.atime(),
                     attr_mutation.mtime(), attr_mutation.ctime(), attr_mutation.delta_version());
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

struct LocalFileInfo {
  std::string path;
};

struct AttrVersionVec;

struct AttrWithMutation {
  AttrEntry attr;
  absl::InlinedVector<AttrMutationEntry, kDirAttrMutationNum> mutations;

  uint64_t BaseVersion() const { return attr.version(); }
  uint64_t CompleteVersion() const { return attr.version() + TotalDeltaVersion(); }
  uint64_t DeltaVersion(uint32_t index) const {
    CHECK(index < mutations.size()) << "invalid mutation index(" << index << "), should be less than "
                                    << mutations.size();
    return mutations[index].delta_version();
  }

  // Mutations are per-slot absolute counters (see AttrVersionVec::PutIf). Callers
  // may append them across retries, so dedup per slot instead of summing blindly:
  // a duplicated slot must not inflate the version.
  uint64_t TotalDeltaVersion() const {
    CHECK(mutations.size() <= kDirAttrMutationNum) << "too many mutations: " << mutations.size();

    absl::InlinedVector<uint64_t, kDirAttrMutationNum> deltas(kDirAttrMutationNum, 0);
    for (const auto& mutation : mutations) {
      deltas[mutation.index()] = std::max(mutation.delta_version(), deltas[mutation.index()]);
    }

    uint64_t total_delta_version = 0;
    for (uint64_t delta : deltas) total_delta_version += delta;

    return total_delta_version;
  }

  AttrEntry ToCompleteAttr() const {
    AttrEntry latest_attr = attr;
    for (const auto& mutation : mutations) {
      latest_attr.set_atime(std::max(latest_attr.atime(), mutation.atime()));
      latest_attr.set_mtime(std::max(latest_attr.mtime(), mutation.mtime()));
      latest_attr.set_ctime(std::max(latest_attr.ctime(), mutation.ctime()));
    }
    latest_attr.set_version(attr.version() + TotalDeltaVersion());

    return latest_attr;
  }
};

// Represents a single attribute version, which can be either a base version or a delta version.
struct AttrVersion {
  bool is_delta{false};
  uint32_t index{0};
  uint64_t version{0};
  AttrVersion(uint64_t version) : version(version) {}
  AttrVersion(uint32_t index, uint64_t version) : is_delta(true), index(index), version(version) {}

  std::string ToString() const {
    if (!is_delta) {
      return fmt::format("{}", version);
    } else {
      return fmt::format("{}-{}", index, version);
    }
  }
};

// Represents a collection of attribute versions, including a base version and multiple delta versions.
struct AttrVersionVec {
  // base version
  uint64_t base_version{0};
  // delta versions
  absl::InlinedVector<uint64_t, kDirAttrMutationNum> delta_versions;
  // sum of delta versions, used for quick check if there is mutation
  uint64_t total_delta_version{0};

  AttrVersionVec(uint64_t base_version) : base_version(base_version) { delta_versions.resize(kDirAttrMutationNum, 0); }
  AttrVersionVec(const AttrWithMutation& attr_with_mutation) : base_version(attr_with_mutation.attr.version()) {
    delta_versions.resize(kDirAttrMutationNum, 0);
    total_delta_version = 0;
    for (const auto& mutation : attr_with_mutation.mutations) {
      PutIf(mutation);
    }
  }

  uint64_t BaseVersion() const { return base_version; }
  uint64_t CompleteVersion() const { return base_version + total_delta_version; }
  uint64_t DeltaVersion(uint32_t index) const {
    CHECK(index < kDirAttrMutationNum) << fmt::format("out of range, {}/{}.", index, kDirAttrMutationNum);

    return delta_versions[index];
  }

  bool PutIf(const AttrMutationEntry& attr_mutation) {
    uint32_t index = attr_mutation.index();
    CHECK(index < kDirAttrMutationNum) << fmt::format("out of range, {}/{}.", index, kDirAttrMutationNum);

    uint64_t& delta_version = delta_versions[index];
    if (attr_mutation.delta_version() <= delta_version) return false;

    total_delta_version += (attr_mutation.delta_version() - delta_version);
    delta_version = attr_mutation.delta_version();

    return true;
  }

  bool PutIf(const AttrVersion& attr_version) {
    if (!attr_version.is_delta) {
      if (attr_version.version <= base_version) return false;
      base_version = attr_version.version;

    } else {
      CHECK(attr_version.index < kDirAttrMutationNum)
          << fmt::format("out of range, {}/{}.", attr_version.index, kDirAttrMutationNum);

      uint64_t& delta_version = delta_versions[attr_version.index];
      if (attr_version.version <= delta_version) return false;

      total_delta_version += (attr_version.version - delta_version);
      delta_version = attr_version.version;
    }

    return true;
  }

  bool PutIf(const AttrEntry& attr) {
    if (attr.version() <= base_version) return false;

    base_version = attr.version();

    return true;
  }

  bool PutIf(const AttrWithMutation& attr_with_mutation) {
    bool updated = false;
    updated |= PutIf(attr_with_mutation.attr);
    for (const auto& mutation : attr_with_mutation.mutations) {
      updated |= PutIf(mutation);
    }

    return updated;
  }

  bool PutIf(const AttrVersionVec& version_vec) {
    bool updated = false;
    if (version_vec.base_version > base_version) {
      updated = true;
      base_version = version_vec.base_version;
    }

    uint32_t size = std::min(delta_versions.size(), version_vec.delta_versions.size());
    for (uint32_t i = 0; i < size; ++i) {
      if (version_vec.delta_versions[i] > delta_versions[i]) {
        updated = true;
        total_delta_version += (version_vec.delta_versions[i] - delta_versions[i]);
        delta_versions[i] = version_vec.delta_versions[i];
      }
    }

    return updated;
  }

  // less than or equal
  bool LessThanOrEqual(const AttrVersion& other) const {
    if (!other.is_delta) {
      return base_version <= other.version;
    } else {
      return DeltaVersion(other.index) <= other.version;
    }
  }

  // greater than or equal
  bool GreaterThanOrEqual(const AttrVersion& other) const {
    if (!other.is_delta) {
      return base_version >= other.version;
    } else {
      return DeltaVersion(other.index) >= other.version;
    }
  }

  std::string ToString() const { return fmt::format("{} {}", base_version, total_delta_version); }
};

// AttrOrMutation carries either a full parent inode attr (need_parent_key path)
// or only a delta mutation against it (mutation path). The two are disambiguated
// by attr.ino(): when attr.ino() == 0 the full attr was not loaded and only the
// mutation entry is valid; otherwise attr is authoritative and mutation may be
// empty. Use IsMutation() instead of inspecting ino() directly at call sites.
struct AttrOrMutation {
  AttrEntry attr;
  AttrMutationEntry mutation;

  bool IsMutation() const { return attr.ino() == 0; }

  AttrVersion ToAttrVersion() const {
    return !IsMutation() ? AttrVersion(attr.version()) : AttrVersion(mutation.index(), mutation.delta_version());
  }
};

enum class ReqType : uint8_t {
  kNormal = 0,
  kRetryToPrimary = 1,
  kRetryToSecondary = 2,
};

inline bool IsDeleted(const AttrEntry& attr_entry) { return attr_entry.nlink() == 0; }

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_TYPE_H_