// Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

#ifndef CURVEFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_
#define CURVEFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_

#include <memory>

#include "curvefs/proto/client_h2.pb.h"
#include "curvefs/src/client/inode_cache_manager.h"

namespace curvefs {
namespace client {
class InodeObjectsService : public inode_objects {
 public:
  InodeObjectsService() = default;

  ~InodeObjectsService() override = default;

  void Init(std::shared_ptr<InodeCacheManager> inode_cache_manager) {
    CHECK(inode_cache_manager != nullptr) << "inode_cache_manager is nullptr";
    inode_cache_manager_ = std::move(inode_cache_manager);
  }

  void default_method(google::protobuf::RpcController* controller,
                      const InodeObjectsRequest* request,
                      InodeObjectsResponse* response,
                      google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<InodeCacheManager> inode_cache_manager_;
};

}  // namespace client
}  // namespace curvefs

#endif  // CURVEFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_