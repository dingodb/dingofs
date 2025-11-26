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
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace utils {

template <class T, std::size_t N>
class Shards {
 public:
  using Func = std::function<void(T&)>;

  auto position(auto&&... args) { return absl::HashOf(args...) % N; }

  auto withRLockAt(Func&& f, std::size_t pos) {
    utils::ReadLockGuard lk(locks_[pos]);

    return f(array_[pos]);
  }

  auto withWLockAt(Func&& f, std::size_t pos) {
    utils::WriteLockGuard lk(locks_[pos]);

    return f(array_[pos]);
  }

  auto withRLock(Func&& f, auto&&... args) {
    return withRLockAt(std::move(f), position(args...));
  }

  auto withWLock(Func&& f, auto&&... args) {
    return withWLockAt(std::move(f), position(args...));
  }

  void iterate(Func&& f) {
    for (std::size_t idx = 0; idx < N; ++idx) {
      Func temp_f = f;
      withRLockAt(std::move(temp_f), idx);
    }
  }

  void iterateWLock(Func&& f) {
    for (std::size_t idx = 0; idx < N; ++idx) {
      Func temp_f = f;
      withWLockAt(std::move(temp_f), idx);
    }
  }

 private:
  std::array<utils::RWLock, N> locks_;
  std::array<T, N> array_;
};

}  // namespace utils
}  // namespace dingofs

#endif  // DINGOFS_SRC_UTILS_SHARDS_H_
