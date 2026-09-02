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
 * Created Date: 2026-09-02
 * Author: AI
 */

#ifndef DINGOFS_TEST_UNIT_CACHE_HELPER_INFINIBAND_H_
#define DINGOFS_TEST_UNIT_CACHE_HELPER_INFINIBAND_H_

#include <cstdint>
#include <string>
#include <vector>

#include "cache/infiniband/common.h"
#include "common/status.h"

namespace dingofs {
namespace cache {
namespace test {

// Owns the backing storage that an RDMABuffer points at.
class ScopedBuffer {
 public:
  explicit ScopedBuffer(uint32_t capacity) : storage_(capacity, 0) {
    buf_.data = storage_.data();
    buf_.capacity = capacity;
    buf_.length = 0;
  }
  infiniband::RDMABuffer* get() { return &buf_; }

 private:
  std::vector<char> storage_;
  infiniband::RDMABuffer buf_;
};

inline infiniband::Region MakeRegion(uint64_t addr, uint32_t length,
                                     uint32_t rkey) {
  infiniband::Region region;
  region.addr = addr;
  region.length = length;
  region.rkey = rkey;
  return region;
}

// True when |status| is InvalidParam and its text mentions |reason|.
inline bool Rejects(const Status& status, const std::string& reason) {
  return status.IsInvalidParam() &&
         status.ToString().find(reason) != std::string::npos;
}

}  // namespace test
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_TEST_UNIT_CACHE_HELPER_INFINIBAND_H_
