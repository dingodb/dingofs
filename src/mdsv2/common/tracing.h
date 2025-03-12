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

#ifndef DINGOFS_MDSV2_COMMON_TRACING_H_
#define DINGOFS_MDSV2_COMMON_TRACING_H_

#include <cstdint>

#include "mdsv2/common/helper.h"

namespace dingofs {
namespace mdsv2 {

class Trace {
 public:
  Trace() = default;

  struct Time {
    uint64_t txn_pending_time_us{0};
    uint64_t txn_exec_time_us{0};
  };

  struct Cache {
    bool is_hit_partition{false};
    bool is_hit_inode{false};
  };

  struct Txn {
    uint64_t txn_id{0};
    bool is_one_pc{false};
    bool is_conflict{false};
    uint64_t read_time_us{0};
    uint64_t write_time_us{0};
    uint32_t retry{0};
  };

  Time& GetTime() { return time_; }
  const Time& GetTime() const { return time_; }
  Cache& GetCache() { return cache_; }
  const Cache& GetCache() const { return cache_; }
  Txn& GetTxn() { return txn_; }
  const Txn& GetTxn() const { return txn_; }

  Txn& GetFileTxn() { return txn_; }
  const Txn& GetFileTxn() const { return txn_; }

  void SetHitPartition() { cache_.is_hit_partition = true; }
  void SetHitInode() { cache_.is_hit_inode = true; }

  void UpdateLastTime() { last_time_us_ = Helper::TimestampUs(); }

  void SetTxnPendingTime() {
    uint64_t now_us = Helper::TimestampUs();
    time_.txn_pending_time_us = now_us - last_time_us_;
    last_time_us_ = now_us;
  }

  void SetTxnExecTime() {
    uint64_t now_us = Helper::TimestampUs();
    time_.txn_exec_time_us = now_us - last_time_us_;
    last_time_us_ = now_us;
  }

 private:
  uint64_t last_time_us_{0};

  Time time_;
  Cache cache_;
  Txn txn_;
  Txn file_txn_;
};

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COMMON_TRACING_H_
