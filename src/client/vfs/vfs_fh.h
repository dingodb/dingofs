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

#ifndef DINGOFS_CLIENT_VFS_FH_H_
#define DINGOFS_CLIENT_VFS_FH_H_

#include <atomic>

namespace dingofs {
namespace client {
namespace vfs {

class FhGenerator {
 public:
  FhGenerator() = default;
  ~FhGenerator() = default;

  FhGenerator(const FhGenerator&) = delete;
  FhGenerator& operator=(const FhGenerator&) = delete;

  // Generate a new file handler
  static uint64_t GenFh() {
    return next_fh_.fetch_add(1, std::memory_order_relaxed);
  }

  // Update next fh to a new value
  static void UpdateNextFh(uint64_t new_fh) {
    next_fh_.store(new_fh, std::memory_order_release);
  }

  // Get the next file handler
  static uint64_t GetNextFh() {
    return next_fh_.load(std::memory_order_acquire);
  }

 private:
  inline static std::atomic<uint64_t> next_fh_{1};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_CLIENT_VFS_FH_H_