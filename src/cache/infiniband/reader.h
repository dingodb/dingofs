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
 * Created Date: 2026-06-13
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_READER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_READER_H_

#include <memory>

#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class BodyReader {
 public:
  explicit BodyReader(Connection* conn);

  Status Read(const char* dst, uint32_t lkey, const std::vector<Region>& src,
              size_t size);

 private:
  Status CheckSource(const std::vector<Region>& regions, size_t size);
  void PrepWorkRequests(const char* dst, uint32_t lkey,
                        const std::vector<Region>& regions,
                        InflightContext* ctx,
                        std::vector<SendWorkRequest>* work_requests);

  Connection* conn_;
};

using BodyReaderUPtr = std::unique_ptr<BodyReader>;

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_READER_H_
