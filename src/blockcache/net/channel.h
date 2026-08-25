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

#ifndef DINGOFS_BLOCKCACHE_NET_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_NET_CHANNEL_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

struct Call {
  const google::protobuf::MethodDescriptor* method = nullptr;
  Opcode opcode = kOpUnspecified;
  const google::protobuf::Message* request = nullptr;
  google::protobuf::Message* response = nullptr;
  BufferViews send;
  BufferView recv;
};

template <typename Req, typename Resp>
struct MethodRef {
  Opcode opcode = kOpUnspecified;
  const google::protobuf::MethodDescriptor* method = nullptr;
};

struct ChannelOption {
  std::string server;
  std::string tag;
  uint64_t route_key = 0;
  uint32_t expected_shard = UINT32_MAX;
};

class Channel {
 public:
  virtual ~Channel() = default;
  Channel() = default;

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  virtual Future<Status> Init(ChannelOption option) = 0;
  virtual Future<> Shutdown() = 0;

  virtual Future<Status> CallMethod(blockcache::Call call) = 0;

  virtual bool Alive() const { return true; }

  template <typename Req, typename Resp>
  Future<Status> Call(MethodRef<Req, Resp> ref, const Req* request,
                      Resp* response, Controller* cntl) {
    const BufferViews to_server = cntl->request_attachment_views();
    const BufferView to_client = cntl->response_attachment_view();
    if (!to_server.empty() && !to_client.empty()) {
      cntl->SetFailed(EINVAL, "both attachments are set on one call");
      co_return Status::Internal(
          "a call carries a attachment one way, not both");
    }

    const Status status = co_await CallMethod(blockcache::Call{
        .method = ref.method,
        .opcode = ref.opcode,
        .request = request,
        .response = response,
        .send = to_server,
        .recv = to_client,
    });
    if (!status.ok()) {
      cntl->SetFailed(EIO, status.ToString());
    }
    co_return status;
  }
};

using ChannelUPtr = std::unique_ptr<Channel>;

}  // namespace blockcache
}  // namespace dingofs

#endif
