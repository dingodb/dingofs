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

#ifndef DINGOFS_MDS_COMMON_TRACING_H_
#define DINGOFS_MDS_COMMON_TRACING_H_

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "mds/common/helper.h"

namespace dingofs {
namespace mds {

class Trace {
 public:
  Trace() { last_time_us_ = Helper::TimestampUs(); }

  using ElapsedTime = std::pair<std::string, uint32_t>;

  struct Time {
    std::vector<ElapsedTime> elapsed_times;
  };

  struct Cache {
    bool is_hit_partition{false};
    bool is_hit_dentry{false};
    bool is_hit_inode{false};
  };

  struct Txn {
    uint64_t txn_id{0};
    bool is_one_pc{false};
    bool is_conflict{false};
    uint64_t read_time_us{0};
    uint64_t write_time_us{0};
  };

  Time& GetTime() { return time_; }
  const Time& GetTime() const { return time_; }
  Cache& GetCache() { return cache_; }
  const Cache& GetCache() const { return cache_; }
  void AddTxn(const Txn& txn) { txns_.push_back(txn); }
  const std::vector<Txn>& GetTxns() const { return txns_; }

  void SetHitPartition() { cache_.is_hit_partition = true; }
  void SetHitDentry() { cache_.is_hit_dentry = true; }
  void SetHitInode() { cache_.is_hit_inode = true; }

  void RecordElapsedTime(const std::string& name) {
    uint64_t time_us = Helper::TimestampUs();
    time_.elapsed_times.emplace_back(name, time_us - last_time_us_);
    last_time_us_ = time_us;
  }

 private:
  uint64_t last_time_us_{0};

  Time time_;
  Cache cache_;
  std::vector<Txn> txns_;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_TRACING_H_
