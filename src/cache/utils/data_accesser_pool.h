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

#ifndef DINGOFS_SRC_CACHE_UTILS_DATA_ACCESSER_POOL_H_
#define DINGOFS_SRC_CACHE_UTILS_DATA_ACCESSER_POOL_H_

#include <memory>
#include <unordered_map>

#include "cache/common/common.h"
#include "dataaccess/accesser.h"
#include "stub/rpcclient/mds_client.h"
#include "utils/concurrent/concurrent.h"

namespace dingofs {
namespace cache {
namespace utils {

using dingofs::dataaccess::DataAccesserPtr;
using dingofs::stub::rpcclient::MdsClient;
using dingofs::utils::RWLock;

class DataAccesserPool {
 public:
  virtual ~DataAccesserPool() = default;

  virtual Status Get(uint32_t fs_id, DataAccesserPtr& data_accesser) = 0;
};

class DataAccesserPoolImpl : public DataAccesserPool {
 public:
  explicit DataAccesserPoolImpl(std::shared_ptr<MdsClient> mds_client);

  Status Get(uint32_t fs_id, DataAccesserPtr& data_accesser) override;

 private:
  Status DoGet(uint32_t fs_id, DataAccesserPtr& data_accesser);

  void DoInsert(uint32_t fs_id, DataAccesserPtr data_accesser);

  bool NewDataAccesser(uint32_t fs_id, DataAccesserPtr& data_accesser);

 private:
  RWLock rwlock_;
  std::shared_ptr<MdsClient> mds_client_;
  std::unordered_map<uint32_t, DataAccesserPtr> data_accessers_;
};

}  // namespace utils
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_UTILS_DATA_ACCESSER_POOL_H_
