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

#ifndef DINGOFS_SRC_MDS_FS_UTILS_H_
#define DINGOFS_SRC_MDS_FS_UTILS_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "mds/common/status.h"
#include "mds/common/type.h"
#include "mds/filesystem/store_operation.h"
#include "mds/storage/storage.h"
#include "nlohmann/json.hpp"

namespace dingofs {
namespace mds {

struct FsTreeNode {
  bool is_orphan{true};
  DentryEntry dentry;
  AttrEntry attr;

  std::vector<FsTreeNode*> children;
};

void FreeFsTree(FsTreeNode* root);

class HashRouter {
 public:
  HashRouter(const pb::mds::HashPartition& hash_partition) : hash_partition_(hash_partition) {
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
  const pb::mds::HashPartition hash_partition_;
  std::vector<uint64_t> buckets_;
};

using HashRouterUPtr = std::unique_ptr<HashRouter>;

class FsUtils {
 public:
  FsUtils(OperationProcessorSPtr operation_processor) : operation_processor_(operation_processor) {}
  FsUtils(OperationProcessorSPtr operation_processor, const FsInfoEntry& fs_info)
      : operation_processor_(operation_processor), fs_info_(fs_info) {
    if (fs_info_.partition_policy().type() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
      hash_router_ = std::make_unique<HashRouter>(fs_info_.partition_policy().parent_hash());
    }
  }

  FsTreeNode* GenFsTree(uint32_t fs_id);
  std::string GenFsTreeJsonString();
  Status GenDirJsonString(Ino parent, std::string& output);

  Status GetChunks(uint32_t fs_id, Ino ino, std::vector<ChunkEntry>& chunks);

 private:
  void GenFsTreeJson(FsTreeNode* node, nlohmann::json& doc);
  Status GenRootDirJsonString(std::string& output);

  FsInfoEntry fs_info_;

  OperationProcessorSPtr operation_processor_;

  HashRouterUPtr hash_router_{nullptr};
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_FS_UTILS_H_
