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

#ifndef DINGOFS_MDS_COMMON_PARTITION_HELPER_H_
#define DINGOFS_MDS_COMMON_PARTITION_HELPER_H_

#include <map>
#include <set>

#include "mds/common/type.h"

namespace dingofs {
namespace mds {

class HashPartitionHelper {
 public:
  static std::map<uint64_t, BucketSetEntry> AdjustDistribution(PartitionPolicy partition_policy,
                                                               const std::set<uint64_t>& online_mds_ids,
                                                               const std::set<uint64_t>& offline_mds_ids);

  static bool CheckHashPartition(const HashPartitionEntry& hash);
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_PARTITION_HELPER_H_
