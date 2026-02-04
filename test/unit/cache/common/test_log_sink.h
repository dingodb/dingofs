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

/*
 * Project: DingoFS
 * Created Date: 2026-02-04
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_CACHE_COMMON_TEST_LOG_SINK_H_
#define DINGOFS_TEST_UNIT_CACHE_COMMON_TEST_LOG_SINK_H_

#include <glog/logging.h>

#include <mutex>
#include <string>
#include <unordered_map>

namespace dingofs {
namespace cache {
namespace test {

class TestLogSink : public google::LogSink {
 public:
  void send(google::LogSeverity /*severity*/, const char* /*full_filename*/,
            const char* /*base_filename*/, int /*line*/,
            const google::LogMessageTime& /*logmsgtime*/, const char* message,
            size_t message_len) override {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string msg(message, message_len);
    for (auto& [pattern, count] : patterns_) {
      if (msg.find(pattern) != std::string::npos) {
        count++;
      }
    }
  }

  void Register(const std::string& pattern) {
    std::lock_guard<std::mutex> lock(mutex_);
    patterns_[pattern] = 0;
  }

  int Count(const std::string& pattern) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = patterns_.find(pattern);
    if (it != patterns_.end()) {
      return it->second;
    }
    return 0;
  }

  void Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [pattern, count] : patterns_) {
      count = 0;
    }
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, int> patterns_;
};

}  // namespace test
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CACHE_COMMON_TEST_LOG_SINK_H_
