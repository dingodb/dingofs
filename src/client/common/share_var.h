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

#ifndef DINGOFS_SRC_CLIENT_COMMON_SHARE_VAR_H_
#define DINGOFS_SRC_CLIENT_COMMON_SHARE_VAR_H_

#include <mutex>
#include <string>
#include <unordered_map>

namespace dingofs {
namespace client {
namespace common {

enum SHAREDKEY : uint8_t {
  kOldPid = 0,
  kSmoothUpgradeNew = 1,
  kSmoothUpgradeOld = 2,
  kNoUmount = 3,
};

class ShareVar {
 public:
  ShareVar(const ShareVar&) = delete;
  ShareVar& operator=(const ShareVar&) = delete;

  static ShareVar& GetInstance() {
    static ShareVar instance;
    return instance;
  }

  void SetValue(const SHAREDKEY& key, const std::string& value) {
    std::lock_guard<std::mutex> lck(mutex_);
    values_.emplace(key, value);
  }

  std::string GetValue(const SHAREDKEY& key) {
    std::lock_guard<std::mutex> lck(mutex_);
    return values_[key];
  }

  bool HasValue(const SHAREDKEY& key) {
    std::lock_guard<std::mutex> lck(mutex_);
    return values_.find(key) != values_.end();
  }

 private:
  ShareVar() = default;

  std::unordered_map<SHAREDKEY, std::string> values_;
  std::mutex mutex_;
};

}  // namespace common
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_COMMON_SHARE_VAR_H_
