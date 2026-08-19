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

#ifndef DINGOFS_BLOCKCACHE_NET_PROTOCOL_PROTO_SERVICE_H_
#define DINGOFS_BLOCKCACHE_NET_PROTOCOL_PROTO_SERVICE_H_

#include <google/protobuf/message.h>

#include <string_view>
#include <type_traits>

#include "blockcache/net/protocol/proto_binding.h"
#include "blockcache/net/server/service.h"

namespace dingofs {
namespace blockcache {

// A Service whose verbs come from a .proto contract; mismatches die at startup.
class ProtoService : public Service {
 public:
  void VerifyBound() const override { bound_.CheckAll(descriptor_); }

 protected:
  explicit ProtoService(const google::protobuf::ServiceDescriptor* descriptor)
      : descriptor_(descriptor) {
    CheckContract(descriptor);
  }

  template <typename S, typename Req, typename Resp>
  void AddMethod(std::string_view name,
                 Future<> (S::*method)(Controller*, const Req*, Resp*),
                 MethodFlags flags = MethodFlags::kNone) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Req> &&
                      std::is_base_of_v<google::protobuf::Message, Resp>,
                  "a contract method speaks the contract's messages");
    const google::protobuf::MethodDescriptor* m =
        MethodNamed(descriptor_, name);
    CheckMethodTypes(m, Req::descriptor(), Resp::descriptor());
    Service::AddMethod(OpcodeOf(m), method, flags);
    bound_.Mark(m);
  }

  // Escape hatch: a hot verb may bind a POD to a raw opcode off-contract.
  using Service::AddMethod;

 private:
  const google::protobuf::ServiceDescriptor* descriptor_;
  MethodSet bound_;
};

}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_NET_PROTOCOL_PROTO_SERVICE_H_
