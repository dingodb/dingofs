/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#ifndef DINGOFS_CACHE_V2_UTILS_CONTAINERS_CIRCULAR_BUFFER_H_
#define DINGOFS_CACHE_V2_UTILS_CONTAINERS_CIRCULAR_BUFFER_H_

#include <bit>
#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>

namespace dingofs {
namespace cache {
namespace v2 {

template <typename T>
class CircularBuffer {
  static_assert(std::is_trivially_copyable_v<T>);

 public:
  explicit CircularBuffer(uint32_t initial_capacity = 512)
      : capacity_(std::bit_ceil(static_cast<uint64_t>(initial_capacity))),
        data_(new T[capacity_]) {}

  void PushBack(T v) {
    if (end_ - begin_ == capacity_) {
      Grow();
    }
    data_[end_ & (capacity_ - 1)] = v;
    ++end_;
  }

  T PopFront() {
    T v = data_[begin_ & (capacity_ - 1)];
    ++begin_;
    return v;
  }

  bool empty() const { return begin_ == end_; }

 private:
  [[gnu::cold, gnu::noinline]] void Grow() {
    uint64_t n = capacity_;
    uint64_t at = begin_ & (n - 1);
    std::unique_ptr<T[]> new_data(new T[n * 2]);
    std::memcpy(new_data.get(), data_.get() + at, (n - at) * sizeof(T));
    std::memcpy(new_data.get() + (n - at), data_.get(), at * sizeof(T));
    data_ = std::move(new_data);
    capacity_ = n * 2;
    begin_ = 0;
    end_ = n;
  }

  uint64_t capacity_;
  std::unique_ptr<T[]> data_;
  uint64_t begin_ = 0;
  uint64_t end_ = 0;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_UTILS_CONTAINERS_CIRCULAR_BUFFER_H_
