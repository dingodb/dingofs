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

#ifndef DINGOFS_SRC_MDSV2_FS_UTILS_H_
#define DINGOFS_SRC_MDSV2_FS_UTILS_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/type.h"
#include "mdsv2/storage/storage.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace mdsv2 {

struct FsTreeNode {
  bool is_orphan{true};
  DentryType dentry;
  AttrType attr;

  std::vector<FsTreeNode*> children;
};

void FreeFsTree(FsTreeNode* root);

class HashRouter {
 public:
  HashRouter(const pb::mdsv2::HashPartition& hash_partition) : hash_partition_(hash_partition) {
    buckets_.resize(hash_partition.bucket_num());

    for (const auto& [mds_id, bucket_set] : hash_partition.distributions()) {
      for (const auto& bucket_id : bucket_set.bucket_ids()) {
        buckets_[bucket_id] = mds_id;
      }
    }
  }
  ~HashRouter() = default;

  uint64_t GetMDS(Ino parent) {
    int64_t bucket_id = parent % hash_partition_.bucket_num();
    return buckets_.at(bucket_id);
  }

 private:
  const pb::mdsv2::HashPartition hash_partition_;
  std::vector<uint64_t> buckets_;
};

using HashRouterUPtr = std::unique_ptr<HashRouter>;

class FsUtils {
 public:
  FsUtils(KVStorageSPtr kv_storage) : kv_storage_(kv_storage) {}
  FsUtils(KVStorageSPtr kv_storage, const FsInfoType& fs_info) : kv_storage_(kv_storage), fs_info_(fs_info) {
    if (fs_info_.partition_policy().type() == pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION) {
      hash_router_ = std::make_unique<HashRouter>(fs_info_.partition_policy().parent_hash());
    }
  }

  FsTreeNode* GenFsTree(uint32_t fs_id);
  std::string GenFsTreeJsonString();

 private:
  void GenFsTreeJson(FsTreeNode* node, nlohmann::json& doc);

  FsInfoType fs_info_;
  KVStorageSPtr kv_storage_;

  HashRouterUPtr hash_router_{nullptr};
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDSV2_FS_UTILS_H_
