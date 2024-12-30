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

#ifndef DINGOFS_MDV2_STORAGE_H_
#define DINGOFS_MDV2_STORAGE_H_

#include <memory>
#include <string>
#include <vector>

#include "dingofs/src/mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {

struct KeyValue {
  enum class OpType {
    kPut = 0,
    kDelete = 1,
  };

  OpType opt_type{OpType::kPut};
  std::string key;
  std::string value;
};

class KVStorage {
 public:
  struct TableOption {
    std::string start_key;
    std::string end_key;
  };

  struct WriteOption {
    bool is_if_absent{false};
  };

  virtual ~KVStorage() = default;

  virtual bool Init(const std::string& addr) = 0;
  virtual bool Destroy() = 0;

  virtual Status CreateTable(const std::string& name, const TableOption& option, int64_t& table_id) = 0;
  virtual Status DropTable(int64_t table_id) = 0;
  virtual Status IsExistTable(const std::string& start_key, const std::string& end_key) = 0;

  virtual Status Put(WriteOption option, const std::string& key, const std::string& value) = 0;
  virtual Status Put(WriteOption option, KeyValue& kv) = 0;
  virtual Status Put(WriteOption option, const std::vector<KeyValue>& kvs) = 0;

  virtual Status Get(const std::string& key, std::string& value) = 0;

  virtual Status Delete(const std::string& key) = 0;
};
using KVStoragePtr = std::shared_ptr<KVStorage>;

}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDV2_STORAGE_H_