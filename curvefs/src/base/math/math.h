/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2024-08-24
 * Author: Jingli Chen (Wine93)
 */

#ifndef CURVEFS_SRC_BASE_MATH_MATH_H_
#define CURVEFS_SRC_BASE_MATH_MATH_H_

#include <cassert>
#include <cstdint>

namespace curvefs {
namespace base {
namespace math {

constexpr uint64_t kKiB = 1024ULL;
constexpr uint64_t kMiB = 1024ULL * kKiB;
constexpr uint64_t kGiB = 1024ULL * kMiB;
constexpr uint64_t kTiB = 1024ULL * kGiB;

inline double Divide(uint64_t a, uint64_t b) {
  assert(b != 0);
  return static_cast<double>(a) / static_cast<double>(b);
}

}  // namespace math
}  // namespace base
}  // namespace curvefs

#endif  // CURVEFS_SRC_BASE_MATH_MATH_H_
