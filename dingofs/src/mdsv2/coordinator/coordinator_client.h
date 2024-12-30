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

#ifndef DINGOFS_MDSV2_COORDINATOR_CLIENT_H_
#define DINGOFS_MDSV2_COORDINATOR_CLIENT_H_

#include <memory>

#include "dingofs/src/mdsv2/common/status.h"
#include "dingofs/src/mdsv2/mds/mds_meta.h"
#include "dingosdk/coordinator.h"

namespace dingofs {
namespace mdsv2 {

class CoordinatorClient {
 public:
  struct AutoIncrement {
    int64_t table_id{0};
    int64_t start_id{0};
    int64_t alloc_id{0};
  };

  CoordinatorClient() = default;
  virtual ~CoordinatorClient() = default;

  virtual bool Init(const std::string& addr) = 0;
  virtual bool Destroy() = 0;

  virtual Status MDSHeartbeat(const MDSMeta& mds) = 0;
  virtual Status GetMDSList(std::vector<MDSMeta>& mdses) = 0;

  virtual Status CreateAutoIncrement(int64_t table_id, int64_t start_id) = 0;
  virtual Status DeleteAutoIncrement(int64_t table_id) = 0;
  virtual Status UpdateAutoIncrement(int64_t table_id, int64_t start_id) = 0;
  virtual Status GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) = 0;
  virtual Status GetAutoIncrement(int64_t table_id, int64_t& start_id) = 0;
  virtual Status GetAutoIncrements(std::vector<AutoIncrement>& auto_increments) = 0;
};

using CoordinatorClientPtr = std::shared_ptr<CoordinatorClient>;

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_COORDINATOR_CLIENT_H_
