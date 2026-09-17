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
#include <utility>

#include "absl/hash/hash.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace utils {

template <typename T, std::size_t N>
class Shards {
 public:
  // Passing no key makes HashOf() a constant, which would silently pin every
  // access to a single shard, so require at least one.
  template <typename... Args>
  auto position(Args&&... args) {
    static_assert(sizeof...(Args) > 0,
                  "Shards needs at least one key to choose a shard");
    return absl::HashOf(std::forward<Args>(args)...) % N;
  }

  template <typename F>
  auto withRLockAt(F&& f, std::size_t pos) {  // NOLINT
    utils::ReadLockGuard lk(locks_[pos]);

    return f(array_[pos]);
  }

  template <typename F>
  auto withWLockAt(F&& f, std::size_t pos) {  // NOLINT
    utils::WriteLockGuard lk(locks_[pos]);

    return f(array_[pos]);
  }

  template <typename F, typename... Args>
  auto withRLock(F&& f, Args&&... args) {  // NOLINT
    return withRLockAt(std::forward<F>(f),
                       position(std::forward<Args>(args)...));
  }

  template <typename F, typename... Args>
  auto withWLock(F&& f, Args&&... args) {  // NOLINT
    return withWLockAt(std::forward<F>(f),
                       position(std::forward<Args>(args)...));
  }

  template <typename F>
  void iterate(F&& f) {  // NOLINT
    for (std::size_t idx = 0; idx < N; ++idx) {
      withRLockAt(f, idx);
    }
  }

  template <typename F>
  void iterateWLock(F&& f) {  // NOLINT
    for (std::size_t idx = 0; idx < N; ++idx) {
      withWLockAt(f, idx);
    }
  }

 private:
  std::array<utils::RWLock, N> locks_;
  std::array<T, N> array_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_SHARDS_H_
