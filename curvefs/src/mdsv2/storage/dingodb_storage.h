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

#ifndef DINGOFS_MDV2_DINGODB_STORAGE_H_
#define DINGOFS_MDV2_DINGODB_STORAGE_H_

#include "curvefs/src/mdsv2/storage/storage.h"
// #include "sdk/client.h"
// #include "sdk/client_stub.h"

namespace dingofs {

namespace mdsv2 {

class DingodbStorage : public KVStorage {
 public:
  DingodbStorage() = default;

  ~DingodbStorage() override = default;

  Status Put(const std::string& key, const std::string& value) override;

  Status Get(const std::string& key, std::string& value) override;

  Status Delete(const std::string& key) override;

  //  private:
  //   std::shared_ptr<dingodb::sdk::ClientStub> client_stub_;
  //   std::shared_ptr<dingodb::sdk::Client> client_;
};

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDV2_DINGODB_STORAGE_H_