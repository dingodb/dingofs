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

#ifndef DINGOFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_
#define DINGOFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_

#include <memory>

#include "client/vfs_old/inode_cache_manager.h"
#include "client/vfs_old/s3/client_s3_adaptor.h"
#include "dingofs/client_h2.pb.h"

namespace dingofs {
namespace client {
class InodeObjectsService : public pb::client::inode_objects {
 public:
  InodeObjectsService() = default;

  ~InodeObjectsService() override = default;

  void Init(std::shared_ptr<S3ClientAdaptor> s3_adapter,
            std::shared_ptr<InodeCacheManager> inode_cache_manager) {
    CHECK(s3_adapter != nullptr) << "s3_adapter is nullptr";
    CHECK(inode_cache_manager != nullptr) << "inode_cache_manager is nullptr";
    s3_adapter_ = std::move(s3_adapter);
    inode_cache_manager_ = std::move(inode_cache_manager);
  }

  void default_method(google::protobuf::RpcController* controller,
                      const pb::client::InodeObjectsRequest* request,
                      pb::client::InodeObjectsResponse* response,
                      google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<S3ClientAdaptor> s3_adapter_;
  std::shared_ptr<InodeCacheManager> inode_cache_manager_;
};

}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_SERVICE_INODE_OBJECTS_SERVICE_H_