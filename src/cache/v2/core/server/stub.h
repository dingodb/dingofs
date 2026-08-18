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

#ifndef DINGOFS_CACHE_V2_CORE_SERVER_STUB_H_
#define DINGOFS_CACHE_V2_CORE_SERVER_STUB_H_

#include <glog/logging.h>
#include <google/protobuf/message.h>

#include <string_view>
#include <type_traits>

#include "cache/v2/core/server/channel.h"
#include "cache/v2/core/server/proto_binding.h"

namespace dingofs {
namespace cache {
namespace v2 {

// One resolved verb, typed to its request/response messages.
template <typename Req, typename Resp>
struct MethodRef {
  Opcode opcode = 0;
};

// Calling side of a .proto contract; mismatches die in the constructor.
class Stub {
 protected:
  Stub(Channel* channel, const google::protobuf::ServiceDescriptor* descriptor)
      : channel_(channel), descriptor_(descriptor) {
    CHECK(channel != nullptr) << "a stub calls through a channel";
    CheckContract(descriptor);
  }

  template <typename Req, typename Resp>
  MethodRef<Req, Resp> Resolve(std::string_view name) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Req> &&
                      std::is_base_of_v<google::protobuf::Message, Resp>,
                  "a contract method speaks the contract's messages");
    const google::protobuf::MethodDescriptor* m =
        MethodNamed(descriptor_, name);
    CheckMethodTypes(m, Req::descriptor(), Resp::descriptor());
    resolved_.Mark(m);
    return MethodRef<Req, Resp>{OpcodeOf(m)};
  }

  // Last line of a derived constructor: every contract method was Resolved.
  void CheckComplete() const { resolved_.CheckAll(descriptor_); }

  template <typename Req, typename Resp>
  Future<Status> Call(const MethodRef<Req, Resp>& ref, const Req* request,
                      Resp* response, Controller* cntl) {
    return channel_->Call(ref.opcode, request, response, cntl);
  }

 private:
  Channel* channel_;  // not owned
  const google::protobuf::ServiceDescriptor* descriptor_;
  MethodSet resolved_;
};

}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_SERVER_STUB_H_
