/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2026-04-27
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_

#include <brpc/server.h>
#include <bthread/mutex.h>

#include <memory>
#include <mutex>
#include <vector>

#include "cache/infiniband/connection.h"
#include "cache/infiniband/infiniband.h"
#include "cache/infiniband/server_session.h"
#include "cache/infiniband/service.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Listener {
 public:
  Listener() = default;
  Status Listen(const EndPoint& ep);
  ConnectionUPtr Accept(const ConnManagementMeta& remote_cm_meta);

 private:
  Device* device_{nullptr};
  Port* port_{nullptr};
  ProtectDomain* protect_domain_{nullptr};
};

using ListenerUPtr = std::unique_ptr<Listener>;

struct ServerOptions {
  brpc::Server* brpc_server{nullptr};
};

class InfinibandServiceImpl : public pb::infiniband::InfinibandService {
 public:
  InfinibandServiceImpl(Listener* listener, ServiceHub* service_hub);
  ~InfinibandServiceImpl() override;

  void Sync(google::protobuf::RpcController* controller,
            const pb::infiniband::SyncRequest* request,
            pb::infiniband::SyncResponse* response,
            google::protobuf::Closure* done) override;

 private:
  void AddSession(ServerSessionSPtr session);
  void CloseAllSessions();

  Listener* listener_;
  ServiceHub* service_hub_;
  bthread::Mutex mutex_;
  std::vector<ServerSessionSPtr> sessions_;
};

class Server {
 public:
  Server();
  Status Start(const EndPoint& ep, ServerOptions* options);
  void Shutdown();

  Status AddService(google::protobuf::Service* service) {
    return service_hub_->AddService(service);
  }

 private:
  ListenerUPtr listener_;
  ServiceHubUPtr service_hub_;
  std::unique_ptr<pb::infiniband::InfinibandService> service_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_SERVER_H_
