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

#include "mdsv2/common/partition_helper.h"

#include <cstdint>
#include <vector>

#include "glog/logging.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/mds/mds_helper.h"

namespace dingofs {
namespace mdsv2 {

static std::map<uint64_t, BucketSetEntry> GetDistributions(const pb::mdsv2::HashPartition& partition) {
  std::map<uint64_t, BucketSetEntry> distributions;
  for (const auto& [mds_id, bucket_set] : partition.distributions()) {
    distributions[mds_id] = bucket_set;
  }

  return distributions;
}

// no put back randomee select mds id
static uint64_t RandomSelectMdsId(std::set<uint64_t>& mds_ids) {
  CHECK(!mds_ids.empty()) << "mds_ids is empty";

  auto it = mds_ids.begin();
  std::advance(it, Helper::GenerateRandomInteger(0, 100000) % mds_ids.size());
  uint64_t mds_id = *it;
  mds_ids.erase(it);
  return mds_id;
}

// uniform distribute bucket id
static void UniformDistribute(uint32_t bucket_num, std::map<uint64_t, BucketSetEntry>& distributions,
                              std::vector<uint64_t>& pending_bucket_ids) {
  if (pending_bucket_ids.empty()) return;

  int mean_num = bucket_num / distributions.size();
  for (auto& [mds_id, bucket_set] : distributions) {
    while (bucket_set.bucket_ids_size() <= mean_num && !pending_bucket_ids.empty()) {
      uint32_t bucket_id = pending_bucket_ids.back();
      pending_bucket_ids.pop_back();
      bucket_set.add_bucket_ids(bucket_id);
    }
  }

  CHECK(pending_bucket_ids.empty()) << "pending_bucket_ids is not empty.";
}

std::map<uint64_t, BucketSetEntry> HashPartitionHelper::AdjustDistribution(PartitionPolicy partition_policy,
                                                                           const std::set<uint64_t>& online_mds_ids,
                                                                           const std::set<uint64_t>& offline_mds_ids) {
  const uint32_t expect_mds_num = partition_policy.parent_hash().expect_mds_num();
  const uint32_t bucket_num = partition_policy.parent_hash().bucket_num();

  auto distributions = GetDistributions(partition_policy.parent_hash());

  // get join online/join offline/other online mds id
  std::set<uint64_t> join_onlines, join_offlines, other_onlines;
  for (auto it = distributions.begin(); it != distributions.end();) {
    uint64_t mds_id = it->first;

    if (MdsHelper::IsContain(offline_mds_ids, mds_id)) {
      join_offlines.insert(mds_id);
    } else {
      join_onlines.insert(mds_id);
    }
  }
  for (const auto& mds_id : online_mds_ids) {
    if (join_onlines.find(mds_id) == join_onlines.end()) {
      other_onlines.insert(mds_id);
    }
  }

  // get pending bucket id and erase offline mds from distributions
  std::vector<uint64_t> pending_bucket_ids;
  for (auto it = distributions.begin(); it != distributions.end();) {
    uint64_t mds_id = it->first;
    const auto& bucket_set = it->second;

    if (MdsHelper::IsContain(offline_mds_ids, mds_id)) {
      pending_bucket_ids.insert(pending_bucket_ids.end(), bucket_set.bucket_ids().begin(),
                                bucket_set.bucket_ids().end());
      it = distributions.erase(it);
    } else {
      ++it;
    }
  }

  if (join_onlines.size() >= expect_mds_num) {
    // join online mds is enough, so don't need other mds.
    // uniform distribute pending bucket ids to join online mds

    UniformDistribute(bucket_num, distributions, pending_bucket_ids);

  } else {
    // join online mds is not enough,  so need take mds from other online mds
    const uint32_t new_mds_num = std::min(expect_mds_num - join_onlines.size(), other_onlines.size());
    const uint32_t mean_num = bucket_num / (distributions.size() + new_mds_num);
    for (uint32_t i = 0; i < new_mds_num; ++i) {
      uint64_t mds_id = RandomSelectMdsId(other_onlines);

      BucketSetEntry bucket_set;
      for (uint32_t j = 0; j < mean_num && !pending_bucket_ids.empty(); ++j) {
        bucket_set.add_bucket_ids(pending_bucket_ids.back());
        pending_bucket_ids.pop_back();
      }

      distributions.insert({mds_id, bucket_set});
    }

    // uniform distribute rest pending bucket ids
    UniformDistribute(bucket_num, distributions, pending_bucket_ids);
  }

  // sort bucketset
  for (auto& [mds_id, bucket_set] : distributions) {
    std::sort(bucket_set.mutable_bucket_ids()->begin(), bucket_set.mutable_bucket_ids()->end());
  }

  return distributions;
}

}  // namespace mdsv2
}  // namespace dingofs