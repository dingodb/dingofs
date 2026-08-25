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

#ifndef DINGOFS_BLOCKCACHE_NET_PROTO_H_
#define DINGOFS_BLOCKCACHE_NET_PROTO_H_

#include <google/protobuf/descriptor.h>

#include <cstdint>
#include <string_view>

#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

Opcode OpcodeOf(const google::protobuf::MethodDescriptor* method);

void CheckContract(const google::protobuf::ServiceDescriptor* service);

const google::protobuf::MethodDescriptor* MethodNamed(
    const google::protobuf::ServiceDescriptor* service, std::string_view name);

void CheckMethodTypes(const google::protobuf::MethodDescriptor* method,
                      const google::protobuf::Descriptor* request,
                      const google::protobuf::Descriptor* response);

class MethodSet {
 public:
  void Mark(const google::protobuf::MethodDescriptor* method);
  void CheckAll(const google::protobuf::ServiceDescriptor* service) const;

 private:
  uint64_t bits_ = 0;
};

}  // namespace blockcache
}  // namespace dingofs

#endif
