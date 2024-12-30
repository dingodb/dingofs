// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDV2_ID_GENERATOR_H_
#define DINGOFS_MDV2_ID_GENERATOR_H_

#include <cstdint>
#include <memory>

#include "bthread/types.h"
#include "dingofs/src/mdsv2/common/status.h"
#include "dingofs/src/mdsv2/coordinator/coordinator_client.h"

namespace dingofs {

namespace mdsv2 {

class IdGenerator {
 public:
  IdGenerator() = default;
  virtual ~IdGenerator() = default;

  virtual bool Init() = 0;

  virtual bool GenID(int64_t& id) = 0;
};

using IdGeneratorPtr = std::shared_ptr<IdGenerator>;

class AutoIncrementIdGenerator : public IdGenerator {
 public:
  AutoIncrementIdGenerator(CoordinatorClientPtr client, int64_t table_id, int64_t start_id, int batch_size);
  ~AutoIncrementIdGenerator() override;

  static IdGeneratorPtr New(CoordinatorClientPtr client, int64_t table_id, int64_t start_id, int batch_size) {
    return std::make_shared<AutoIncrementIdGenerator>(client, table_id, start_id, batch_size);
  }

  bool Init() override;
  bool GenID(int64_t& id) override;

 private:
  Status IsExistAutoIncrement();
  Status CreateAutoIncrement();

  Status TakeBundleIdFromCoordinator();

  int64_t table_id_{0};
  int64_t start_id_{0};
  const int batch_size_{0};

  CoordinatorClientPtr client_;

  bthread_mutex_t mutex_;

  // [bundle, bundle_end)
  int64_t bundle_{0};
  int64_t next_id_{0};
  int64_t bundle_end_{0};
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDV2_ID_GENERATOR_H_