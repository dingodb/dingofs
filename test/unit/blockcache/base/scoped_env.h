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
 * Created Date: 2026-10-09
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_SCOPED_ENV_H_
#define DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_SCOPED_ENV_H_

#include <cstdlib>
#include <optional>
#include <string>

namespace dingofs {
namespace blockcache {

class ScopedEnv {
 public:
  ScopedEnv(const char* name, const char* value) : name_(name) {
    if (const char* old = std::getenv(name); old != nullptr) {
      saved_ = old;
    }
    Set(value);
  }

  ~ScopedEnv() { Set(saved_ ? saved_->c_str() : nullptr); }

  ScopedEnv(const ScopedEnv&) = delete;
  ScopedEnv& operator=(const ScopedEnv&) = delete;

 private:
  void Set(const char* value) const {
    if (value != nullptr) {
      ::setenv(name_, value, 1);
    } else {
      ::unsetenv(name_);
    }
  }

  const char* name_;
  std::optional<std::string> saved_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_BLOCKCACHE_BASE_SCOPED_ENV_H_
