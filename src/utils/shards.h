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

#ifndef DINGOFS_SRC_UTILS_SHARDS_H_
#define DINGOFS_SRC_UTILS_SHARDS_H_

#include <array>
#include <mutex>

#include "absl/hash/hash.h"

// #include "RobinHoodUtils.h"

namespace dingofs {
namespace utils {

template <class T, std::size_t N>
class Shards {
 public:
  auto position(auto&&... args) {
    return absl::HashOf(args...) % N;

    // absl::HashState state;
    // absl::HashState::combine(state, args...) % N;
  }

  auto withLockAt(auto&& f, std::size_t pos) {
    auto lock = std::unique_lock(locks_[pos]);
    return f(array_[pos]);
  }

  auto withLock(auto&& f, auto&&... args) {
    return withLockAt(f, position(args...));
  }

  void iterate(auto&& f) {
    for (std::size_t idx = 0; idx < N; ++idx) {
      withLockAt(f, idx);
    }
  }

 private:
  std::array<std::mutex, N> locks_;
  std::array<T, N> array_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_SHARDS_H_
