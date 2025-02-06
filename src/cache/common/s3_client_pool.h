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
 * Created Date: 2025-03-17
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_
#define DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_

#include <memory>
#include <unordered_map>

#include "cache/common/s3_client.h"
#include "stub/rpcclient/mds_client.h"

namespace dingofs {
namespace cache {
namespace common {

using stub::rpcclient::MdsClient;

class S3ClientPool {
 public:
  S3ClientPool(uint32_t pool_size);

  virtual Errno Get(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client) = 0;
};

class S3ClientPoolImpl : public S3ClientPool {
 public:
  explicit S3ClientPool(std::shared_ptr<MdsClient> mds_client);

  Errno Get(uint32_t fs_id, std::shared_ptr<S3Client>& s3_client) override;

 private:
  RWLock rwlock_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unordered_map<uint32_t, std::shared_ptr<S3Client>> clients_;
};

}  // namespace common
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_S3CLIENT_POOL_H_
