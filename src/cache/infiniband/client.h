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
 * Created Date: 2026-04-29
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_

#include <google/protobuf/message.h>

#include <memory>
#include <type_traits>

#include "cache/infiniband/client_session.h"
#include "cache/infiniband/common.h"
#include "cache/infiniband/connection.h"
#include "cache/infiniband/controller.h"
#include "cache/infiniband/infiniband.h"
#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Dialer;
class Client;

using DialerUPtr = std::unique_ptr<Dialer>;
using ClientUPtr = std::unique_ptr<Client>;

class Dialer {
 public:
  Dialer(Device* device, Port* port, ProtectDomain* protect_domain);
  static DialerUPtr Create(const EndPoint& ep);

  ConnectionUPtr Dial(const std::string& address);

 private:
  Status SyncConnManagementMeta(const std::string& address,
                                const ConnManagementMeta& local_cm_meta,
                                ConnManagementMeta* remote_cm_meta);

  Device* device_;
  Port* port_;
  ProtectDomain* protect_domain_;
};

class Client {
 public:
  explicit Client(DialerUPtr dialer);
  ~Client();
  static ClientUPtr Create(const EndPoint& ep);

  Status Connect(const std::string& address);

  template <typename Request, typename Response>
  void Call(Controller* cntl, std::string_view service_name,
            std::string_view method_name, const Request& request,
            Response* response) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Request>,
                  "Request must be a protobuf Message type");
    static_assert(std::is_base_of_v<google::protobuf::Message, Response>,
                  "Response must be a protobuf Message type");
    if (session_ == nullptr) {
      SetFailed(cntl, pb::infiniband::ErrorCode::InternalError,
                "rdma client is not connected");
      return;
    }

    session_->DoCall(cntl, service_name, method_name, request, response);
  }

 private:
  DialerUPtr dialer_;
  ClientSessionSPtr session_;
};

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_CLIENT_H_
