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

#ifndef DINGOFS_MDS_COMMON_SUFFIX_SET_H_
#define DINGOFS_MDS_COMMON_SUFFIX_SET_H_

#include <sys/types.h>

#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "mds/common/helper.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace mds {

class SuffixSet {
 public:
  SuffixSet() = default;
  ~SuffixSet() = default;

  void Update(const std::string& suffix_str) {
    std::vector<std::string> suffixs;
    Helper::SplitString(suffix_str, ',', suffixs);

    {
      utils::WriteLockGuard lk(lock_);

      for (const auto& suffix : suffixs) {
        suffix_set_.insert(suffix);
      }
    }
  }

  bool HasSuffix(const std::string& name) const {
    utils::ReadLockGuard lk(lock_);

    // name is xxx.jpg, we need to get jpg
    auto pos = name.rfind('.');
    if (pos == std::string::npos) return false;

    return suffix_set_.find(name.substr(pos + 1)) != suffix_set_.end();
  }

 private:
  mutable utils::RWLock lock_;

  absl::flat_hash_set<std::string> suffix_set_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_SUFFIX_SET_H_