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

#ifndef DINGOFS_MDS_COMMON_TIME_H_
#define DINGOFS_MDS_COMMON_TIME_H_

#include <cstdint>

#include "mds/common/helper.h"

namespace dingofs {
namespace mds {

class Duration {
 public:
  Duration() { start_time_ns_ = Helper::TimestampNs(); }
  ~Duration() = default;

  int64_t StartNs() const { return start_time_ns_; }
  int64_t StartUs() const { return start_time_ns_ / 1000; }
  int64_t StartMs() const { return start_time_ns_ / 1000000; }

  // Get elapsed time in nanoseconds
  int64_t ElapsedNs() const { return Helper::TimestampNs() - start_time_ns_; }
  int64_t ElapsedUs() const { return (Helper::TimestampNs() - start_time_ns_) / 1000; }
  int64_t ElapsedMs() const { return (Helper::TimestampNs() - start_time_ns_) / 1000000; }
  int64_t ElapsedS() const { return (Helper::TimestampNs() - start_time_ns_) / 1000000000; }

 private:
  int64_t start_time_ns_ = 0;
};

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_COMMON_TIME_H_