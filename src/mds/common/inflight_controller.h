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

#ifndef DINGOFS_MDS_COMMON_INFLIGHT_CONTROLLER_H_
#define DINGOFS_MDS_COMMON_INFLIGHT_CONTROLLER_H_

#include "absl/container/flat_hash_map.h"
#include "bthread/countdown_event.h"
#include "bthread/mutex.h"
#include "mds/common/status.h"

namespace dingofs {
namespace mds {

template <typename T, typename U>
class InflightController {
 public:
  struct InflightEntry {
    bthread::CountdownEvent done{1};
    Status status;
    U value;
  };
  using InflightEntryPtr = std::shared_ptr<InflightEntry>;

  InflightEntryPtr GetOrCreate(const T& key, bool& is_leader) {
    std::lock_guard<bthread::Mutex> guard(mutex_);

    auto it = inflight_entries_.find(key);
    if (it != inflight_entries_.end()) {
      is_leader = false;
      return it->second;

    } else {
      auto inflight = std::make_shared<InflightEntry>();
      inflight_entries_[key] = inflight;
      is_leader = true;
      return inflight;
    }
  }

  void Delete(const T& key) {
    std::lock_guard<bthread::Mutex> guard(mutex_);
    inflight_entries_.erase(key);
  }

 private:
  bthread::Mutex mutex_;
  absl::flat_hash_map<T, std::shared_ptr<InflightEntry>> inflight_entries_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_INFLIGHT_CONTROLLER_H_