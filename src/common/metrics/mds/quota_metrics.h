/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef DINGOFS_SRC_COMMON_METRICS_MDS_QUOTA_METRICS_H_
#define DINGOFS_SRC_COMMON_METRICS_MDS_QUOTA_METRICS_H_

#include <bvar/bvar.h>

#include <cstdint>
#include <string>

namespace dingofs {
namespace metrics {
namespace mds {

class FsQuotaMetric {
 public:
  bvar::PassiveStatus<int64_t> used_bytes;
  bvar::PassiveStatus<int64_t> used_inodes;
  bvar::PassiveStatus<int64_t> max_bytes;
  bvar::PassiveStatus<int64_t> max_inodes;

  static double GetInodeUsageRatio(void* arg) {
    auto* metric = reinterpret_cast<FsQuotaMetric*>(arg);
    int64_t max = metric->max_inodes.get_value();
    if (max == 0) {
      return 0.0;
    }
    return static_cast<double>(metric->used_inodes.get_value()) / max;
  }

  static double GetBytesUsageRatio(void* arg) {
    auto* metric = reinterpret_cast<FsQuotaMetric*>(arg);
    int64_t max = metric->max_bytes.get_value();
    if (max == 0) {
      return 0.0;
    }
    return static_cast<double>(metric->used_bytes.get_value()) / max;
  }

  template <typename T>
  FsQuotaMetric(const std::string& prefix, T* obj,
                int64_t (*get_used_bytes)(void*),
                int64_t (*get_used_inodes)(void*),
                int64_t (*get_max_bytes)(void*),
                int64_t (*get_max_inodes)(void*))
      : used_bytes(prefix + "_used_bytes", get_used_bytes, obj),
        used_inodes(prefix + "_used_inodes", get_used_inodes, obj),
        max_bytes(prefix + "_max_bytes", get_max_bytes, obj),
        max_inodes(prefix + "_max_inodes", get_max_inodes, obj),
        inode_usage_ratio_(prefix + "_inode_usage_ratio", GetInodeUsageRatio,
                           this),
        bytes_usage_ratio_(prefix + "_bytes_usage_ratio", GetBytesUsageRatio,
                           this) {}

  double get_inode_usage_ratio() const {
    return inode_usage_ratio_.get_value();
  }
  double get_bytes_usage_ratio() const {
    return bytes_usage_ratio_.get_value();
  }

 private:
  bvar::PassiveStatus<double> inode_usage_ratio_;
  bvar::PassiveStatus<double> bytes_usage_ratio_;
};

}  // namespace mds
}  // namespace metrics
}  // namespace dingofs

#endif  // DINGOFS_SRC_COMMON_METRICS_MDS_QUOTA_METRICS_H_
