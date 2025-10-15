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

#ifndef DINGOFS_MDS_MDS_MDS_HELPER_H_
#define DINGOFS_MDS_MDS_MDS_HELPER_H_

#include <cstdint>
#include <vector>

#include "mds/common/helper.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace mds {

class MdsHelper {
 public:
  static std::vector<uint64_t> GetMdsIds(const std::vector<MDSMeta>& mds_metas) {
    std::vector<uint64_t> mds_ids;
    mds_ids.reserve(mds_metas.size());

    for (const auto& mds_meta : mds_metas) {
      mds_ids.push_back(mds_meta.ID());
    }

    return mds_ids;
  }

  static std::set<uint64_t> GetMdsIdSet(const std::vector<MDSMeta>& mds_metas) {
    std::set<uint64_t> mds_ids;

    for (const auto& mds_meta : mds_metas) {
      mds_ids.insert(mds_meta.ID());
    }

    return mds_ids;
  }

  static std::vector<MDSMeta> FilterMdsMetas(const std::vector<MDSMeta>& mds_metas,
                                             const std::vector<uint64_t>& mds_ids) {
    std::vector<MDSMeta> dst_mds_metas;
    dst_mds_metas.reserve(mds_ids.size());

    for (const auto& mds_id : mds_ids) {
      for (const auto& mds_meta : mds_metas) {
        if (mds_meta.ID() == mds_id) {
          dst_mds_metas.push_back(mds_meta);
          break;
        }
      }
    }

    return dst_mds_metas;
  }

  static bool IsContain(const std::set<uint64_t>& mds_ids, uint64_t target_mds_id) {
    for (const auto& mds_id : mds_ids) {
      if (target_mds_id == mds_id) {
        return true;
      }
    }

    return false;
  }

  static std::vector<MDSMeta> RandomSelectMds(const std::vector<MDSMeta>& mds_metas, uint32_t num) {
    if (mds_metas.size() <= num) return mds_metas;

    std::list<MDSMeta> temp_mds_metas(mds_metas.begin(), mds_metas.end());
    std::vector<MDSMeta> selected_mds_metas;
    for (uint32_t i = 0; i < num; ++i) {
      if (temp_mds_metas.empty()) break;

      auto it = temp_mds_metas.begin();
      std::advance(it, Helper::GenerateRealRandomInteger(0, 100000) % temp_mds_metas.size());

      selected_mds_metas.push_back(*it);
      temp_mds_metas.erase(it);
    }

    return selected_mds_metas;
  }
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_MDS_MDS_HELPER_H_