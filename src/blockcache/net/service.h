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

#ifndef DINGOFS_BLOCKCACHE_NET_SERVICE_H_
#define DINGOFS_BLOCKCACHE_NET_SERVICE_H_

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/net/controller.h"
#include "blockcache/net/proto.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {

class ResponseWriter {
 public:
  ResponseWriter() = default;
  virtual ~ResponseWriter() = default;

  ResponseWriter(const ResponseWriter&) = delete;
  ResponseWriter& operator=(const ResponseWriter&) = delete;

  virtual Future<bool> Write(const google::protobuf::Message& response) = 0;
};

class Service {
 public:
  using Method = std::function<Future<ReplyCode>(
      Controller* cntl, std::string_view request, ResponseWriter* out)>;

  Service() = default;
  virtual ~Service() = default;

  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  virtual void VerifyBound() const {}

  bool Has(Opcode opcode) const {
    return opcode < methods_.size() && methods_[opcode] != nullptr;
  }
  const Method& MethodOf(Opcode opcode) const { return methods_[opcode]; }
  size_t method_limit() const { return methods_.size(); }

 protected:
  template <typename S, typename Req, typename Resp>
  void AddMethod(Opcode opcode,
                 Future<> (S::*method)(Controller*, const Req*, Resp*)) {
    auto* self = static_cast<S*>(this);
    Bind(opcode, [self, method](Controller* cntl, std::string_view request,
                                ResponseWriter* out) {
      return Call<S, Req, Resp>(self, method, cntl, request, out);
    });
  }

 private:
  template <typename S, typename Req, typename Resp, typename M>
  static Future<ReplyCode> Call(S* self, M method, Controller* cntl,
                                std::string_view request, ResponseWriter* out) {
    Req req;
    if (!req.ParseFromArray(request.data(), static_cast<int>(request.size()))) {
      co_return kReplyBadRequest;
    }
    Resp resp;
    co_await (self->*method)(cntl, &req, &resp);
    if (cntl->Failed()) {
      co_return kReplyHandlerError;
    }
    if (!co_await out->Write(resp)) {
      co_return kReplyHandlerError;
    }
    co_return kReplyOk;
  }

  void Bind(Opcode opcode, Method method) {
    if (methods_.size() <= opcode) {
      methods_.resize(size_t{opcode} + 1);
    }
    methods_[opcode] = std::move(method);
  }

  std::vector<Method> methods_;
};

using ServiceUPtr = std::unique_ptr<Service>;

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
                 Future<> (S::*method)(Controller*, const Req*, Resp*)) {
    static_assert(std::is_base_of_v<google::protobuf::Message, Req> &&
                      std::is_base_of_v<google::protobuf::Message, Resp>,
                  "a contract method speaks the contract's messages");
    const google::protobuf::MethodDescriptor* m =
        MethodNamed(descriptor_, name);
    CheckMethodTypes(m, Req::descriptor(), Resp::descriptor());
    Service::AddMethod(OpcodeOf(m), method);
    bound_.Mark(m);
  }

 private:
  const google::protobuf::ServiceDescriptor* descriptor_;
  MethodSet bound_;
};

class ServiceRegistry final {
 public:
  ServiceRegistry() = default;

  ServiceRegistry(const ServiceRegistry&) = delete;
  ServiceRegistry& operator=(const ServiceRegistry&) = delete;

  void Add(Service* service);

  const Service::Method* Find(Opcode opcode) const {
    if (opcode >= methods_.size() || methods_[opcode] == nullptr) {
      return nullptr;
    }
    return &methods_[opcode];
  }

 private:
  std::vector<Service::Method> methods_;
};

using ServiceRegistryUPtr = std::unique_ptr<ServiceRegistry>;

}  // namespace blockcache
}  // namespace dingofs

#endif
