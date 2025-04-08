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
 * Created Date: 2025-04-12
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CLIENT_BLOCKCACHE_HELPER_H_
#define DINGOFS_SRC_CLIENT_BLOCKCACHE_HELPER_H_

#include <absl/strings/str_format.h>
#include <brpc/closure_guard.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>

#include <cstring>
#include <sstream>
#include <string>

namespace dingofs {
namespace client {
namespace blockcache {

class Helper {
 public:
  template <typename... Args>
  static std::string Errorf(int code, const char* format, const Args&... args) {
    std::ostringstream message;
    message << absl::StrFormat(format, args...) << ": " << ::strerror(code);
    return message.str();
  }

  template <typename... Args>
  static std::string Errorf(const char* format, const Args&... args) {
    return Errorf(errno, format, args...);
  }
};

class ThrottleQueue {
 public:
  ThrottleQueue(uint32_t capacity) : capacity_(capacity) {}

  void PushOne() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    while (size_ == capacity_) {
      cv_.wait(lk);
    }
    size_++;
  }

  void PopOne() {
    std::unique_lock<bthread::Mutex> lk(mutex_);
    CHECK(size_ > 0);
    size_--;
    cv_.notify_one();
  }

 private:
  uint32_t size_{0};
  uint32_t capacity_;
  bthread::Mutex mutex_;
  bthread::ConditionVariable cv_;
};

}  // namespace blockcache
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_BLOCKCACHE_HELPER_H_
