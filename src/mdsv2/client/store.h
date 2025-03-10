// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <map>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/storage/storage.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

class StoreClient {
 public:
  StoreClient() = default;
  ~StoreClient() = default;

  bool Init(const std::string& coor_addr);

  bool CreateFsTable(const std::string& name);
  // print fs dentry tree
  void PrintDentryTree(uint32_t fs_id, bool is_details);

 private:
  KVStoragePtr kv_storage_;
};

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs