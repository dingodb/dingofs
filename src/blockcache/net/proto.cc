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

#include "blockcache/net/proto.h"

#include <glog/logging.h>
#include <google/protobuf/descriptor.pb.h>

#include <string>

#include "dingofs/cache.pb.h"

namespace dingofs {
namespace blockcache {

static_assert(kReplyOk == pb::blockcache::REPLY_OK);
static_assert(kReplyBadOpcode == pb::blockcache::REPLY_BAD_OPCODE);
static_assert(kReplyHandlerError == pb::blockcache::REPLY_HANDLER_ERROR);
static_assert(kReplyTooLarge == pb::blockcache::REPLY_TOO_LARGE);
static_assert(kReplyBadRequest == pb::blockcache::REPLY_BAD_REQUEST);
static_assert(kOpUnspecified == pb::blockcache::OPCODE_UNSPECIFIED);

Opcode OpcodeOf(const google::protobuf::MethodDescriptor* method) {
  return static_cast<Opcode>(
      method->options().GetExtension(pb::blockcache::opcode));
}

void CheckContract(const google::protobuf::ServiceDescriptor* service) {
  CHECK(service != nullptr) << "a contract needs a service descriptor";
  const int count = service->method_count();
  CHECK_LE(count, 64) << service->full_name()
                      << ": the bound-method bookkeeping holds 64 methods";
  for (int i = 0; i < count; ++i) {
    const google::protobuf::MethodDescriptor* method = service->method(i);
    CHECK(method->options().HasExtension(pb::blockcache::opcode))
        << method->full_name() << " declares no opcode";
    CHECK_NE(OpcodeOf(method), kOpUnspecified)
        << method->full_name() << " declares the unspecified opcode";
    for (int j = i + 1; j < count; ++j) {
      CHECK(OpcodeOf(method) != OpcodeOf(service->method(j)))
          << method->full_name() << " and " << service->method(j)->full_name()
          << " declare the same opcode";
    }
  }
}

const google::protobuf::MethodDescriptor* MethodNamed(
    const google::protobuf::ServiceDescriptor* service, std::string_view name) {
  const google::protobuf::MethodDescriptor* method =
      service->FindMethodByName(std::string(name));
  CHECK(method != nullptr) << service->full_name() << " has no method named "
                           << name;
  return method;
}

void CheckMethodTypes(const google::protobuf::MethodDescriptor* method,
                      const google::protobuf::Descriptor* request,
                      const google::protobuf::Descriptor* response) {
  CHECK(method->input_type() == request)
      << method->service()->full_name() << "." << method->name() << " takes "
      << method->input_type()->full_name() << ", not " << request->full_name();
  CHECK(method->output_type() == response)
      << method->service()->full_name() << "." << method->name() << " returns "
      << method->output_type()->full_name() << ", not "
      << response->full_name();
}

void MethodSet::Mark(const google::protobuf::MethodDescriptor* method) {
  const uint64_t bit = uint64_t{1} << method->index();
  CHECK((bits_ & bit) == 0) << method->service()->full_name() << "."
                            << method->name() << " is bound twice";
  bits_ |= bit;
}

void MethodSet::CheckAll(
    const google::protobuf::ServiceDescriptor* service) const {
  for (int i = 0; i < service->method_count(); ++i) {
    CHECK((bits_ & (uint64_t{1} << i)) != 0)
        << service->full_name() << "." << service->method(i)->name()
        << " is in the contract but was never bound";
  }
}

}  // namespace blockcache
}  // namespace dingofs
