/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Project: DingoFS
 * Created Date: 2026-01-14
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_IUTIL_MATH_UTIL_H_
#define DINGOFS_SRC_CACHE_IUTIL_MATH_UTIL_H_

#include <glog/logging.h>
#include <sys/stat.h>

#include <cstdint>
#include <numeric>
#include <vector>

namespace dingofs {
namespace cache {
namespace iutil {

inline std::vector<uint64_t> NormalizeByGcd(const std::vector<uint64_t>& nums) {
  uint64_t gcd = 0;
  std::vector<uint64_t> out;
  for (const auto& num : nums) {
    out.push_back(num);
    gcd = std::gcd(gcd, num);
  }
  CHECK_NE(gcd, 0);

  for (auto& num : out) {
    num = num / gcd;
  }
  return out;
}

}  // namespace iutil
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_IUTIL_MATH_UTIL_H_
