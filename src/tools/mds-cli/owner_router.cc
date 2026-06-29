// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#include "tools/mds-cli/owner_router.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <iostream>
#include <string>
#include <unordered_set>

#include "dingofs/error.pb.h"

namespace dingofs {
namespace mds {
namespace client {

bool OwnerRouter::Init(uint32_t fs_id, MDSClient& bootstrap) {
  fs_id_ = fs_id;

  auto fs_resp = bootstrap.GetFs(fs_id);
  if (fs_resp.error().errcode() != pb::error::OK) {
    std::cerr << fmt::format("get fs info fail: {} ({})\n", fs_resp.error().errmsg(),
                             static_cast<int>(fs_resp.error().errcode()));
    return false;
  }
  partition_policy_ = fs_resp.fs_info().partition_policy();

  auto mds_resp = bootstrap.GetMdsList();
  if (mds_resp.error().errcode() != pb::error::OK) {
    std::cerr << fmt::format("get mds list fail: {} ({})\n", mds_resp.error().errmsg(),
                             static_cast<int>(mds_resp.error().errcode()));
    return false;
  }
  std::unordered_map<uint64_t, std::string> mds_addrs;
  for (const auto& mds : mds_resp.mdses()) {
    mds_addrs[mds.id()] = fmt::format("{}:{}", mds.location().host(), mds.location().port());
  }

  std::unordered_set<uint64_t> owner_mds_ids;
  if (partition_policy_.type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    owner_mds_ids.insert(partition_policy_.mono().mds_id());

  } else if (partition_policy_.type() == pb::mds::PartitionType::PARENT_ID_HASH_PARTITION) {
    if (partition_policy_.parent_hash().bucket_num() == 0) {
      std::cerr << "parent hash bucket_num is 0\n";
      return false;
    }
    for (const auto& [mds_id, bucket_set] : partition_policy_.parent_hash().distributions()) {
      owner_mds_ids.insert(mds_id);
      for (const auto& bucket_id : bucket_set.bucket_ids()) bucket_to_mds_[bucket_id] = mds_id;
    }

  } else {
    std::cerr << fmt::format("unknown partition type({})\n", pb::mds::PartitionType_Name(partition_policy_.type()));
    return false;
  }

  for (uint64_t mds_id : owner_mds_ids) {
    auto it = mds_addrs.find(mds_id);
    if (it == mds_addrs.end()) {
      LOG(ERROR) << fmt::format("mds({}) not in mds list, inodes owned by it cannot be routed", mds_id);
      continue;
    }
    auto mds_client = std::make_unique<MDSClient>(fs_id_);
    if (!mds_client->Init(it->second)) {
      LOG(ERROR) << fmt::format("init client for mds({}) at {} fail, inodes owned by it cannot be routed", mds_id,
                                it->second);
      continue;
    }
    // Seed the partition epoch so epoch-validated RPCs (ReadDir / ListDentry /
    // SyncDirStat) issued through this client are accepted after a rebalance.
    mds_client->SetEpoch(partition_policy_.epoch());
    mds_clients_[mds_id] = std::move(mds_client);
  }

  return true;
}

MDSClient* OwnerRouter::ClientForIno(Ino ino) {
  uint64_t mds_id = 0;
  if (partition_policy_.type() == pb::mds::PartitionType::MONOLITHIC_PARTITION) {
    mds_id = partition_policy_.mono().mds_id();

  } else {
    auto it = bucket_to_mds_.find(static_cast<uint32_t>(ino % partition_policy_.parent_hash().bucket_num()));
    if (it != bucket_to_mds_.end()) mds_id = it->second;
  }

  auto it = mds_clients_.find(mds_id);
  if (it != mds_clients_.end()) return it->second.get();

  return nullptr;
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs
