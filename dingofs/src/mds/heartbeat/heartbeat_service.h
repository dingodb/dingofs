/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: dingo
 * Created Date: 2021-09-16
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_
#define DINGOFS_SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_

#include <brpc/server.h>

#include <memory>

#include "dingofs/proto/heartbeat.pb.h"
#include "dingofs/src/mds/heartbeat/heartbeat_manager.h"

namespace dingofs {
namespace mds {
namespace heartbeat {

class HeartbeatServiceImpl : public pb::mds::heartbeat::HeartbeatService {
 public:
  HeartbeatServiceImpl() = default;
  explicit HeartbeatServiceImpl(
      std::shared_ptr<HeartbeatManager> heartbeat_manager);
  ~HeartbeatServiceImpl() override = default;

  void MetaServerHeartbeat(
      google::protobuf::RpcController* controller,
      const pb::mds::heartbeat::MetaServerHeartbeatRequest* request,
      pb::mds::heartbeat::MetaServerHeartbeatResponse* response,
      google::protobuf::Closure* done) override;

 private:
  std::shared_ptr<mds::heartbeat::HeartbeatManager> heartbeatManager_;
};
}  // namespace heartbeat
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_SRC_MDS_HEARTBEAT_HEARTBEAT_SERVICE_H_