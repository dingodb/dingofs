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

#ifndef DINGOFS_MDS_COMMON_HELPER_H_
#define DINGOFS_MDS_COMMON_HELPER_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "dingofs/mds.pb.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {

class Helper {
 public:
  static std::string ParseStorageAddr(const std::string& url);

  static std::vector<uint64_t> GetMdsIds(const pb::mds::HashPartition& partition);
  static std::vector<uint64_t> GetMdsIds(const std::map<uint64_t, BucketSetEntry>& distributions);

  static std::string ToString(const std::vector<mds::SliceEntry>& slices) {
    std::string result;
    for (uint32_t i = 0; i < slices.size(); ++i) {
      result += std::to_string(slices[i].id());
      if (i != slices.size() - 1) result += ",";
    }

    return result;
  }
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_HELPER_H_
