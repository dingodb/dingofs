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

#ifndef DINGOFS_SRC_CLIENT_MEMRORY_READ_BUFFER_MANAGER_H_
#define DINGOFS_SRC_CLIENT_MEMRORY_READ_BUFFER_MANAGER_H_

#include <bvar/reducer.h>
#include <bvar/status.h>
#include <sys/types.h>

#include <atomic>
#include <cstdint>

namespace dingofs {
namespace client {

class ReadBufferManager {
 public:
  explicit ReadBufferManager(int64_t total_bytes);

  ~ReadBufferManager() = default;

  void Take(int64_t bytes);

  void Release(int64_t bytes);

  int64_t GetTotalBytes() const;

  int64_t GetUsedBytes() const;

  double GetUsageRatio() const;

  bool IsHighPressure(double threshold = 0.8) const;

 private:
  const int64_t total_bytes_{0};
  std::atomic<int64_t> used_bytes_{0};

  bvar::Status<int64_t> read_buffer_total_bytes_;
  bvar::Adder<int64_t> read_buffer_used_bytes_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_MEMRORY_READ_BUFFER_MANAGER_H_
