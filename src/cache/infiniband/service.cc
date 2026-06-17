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
 * Created Date: 2026-05-07
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/service.h"

#include <glog/logging.h>
#include <google/protobuf/descriptor.h>

namespace dingofs {
namespace cache {
namespace infiniband {

Status ServiceHub::AddService(google::protobuf::Service* service) {
  CHECK_NOTNULL(service);

  const std::string name = service->GetDescriptor()->full_name();
  auto [_, inserted] = services_.emplace(name, service);
  if (!inserted) {
    LOG(ERROR) << "Service=" << name << " already registered";
    return Status::Exist("service already registered: " + name);
  }

  LOG(INFO) << "Successfully register service=" << name;
  return Status::OK();
}

Status ServiceHub::GetService(const std::string& service_name,
                              google::protobuf::Service** service) {
  CHECK_NOTNULL(service);

  auto iter = services_.find(service_name);
  if (iter == services_.end()) {
    LOG(ERROR) << "Service=" << service_name << " not found";
    return Status::NotFound("service not found: " + service_name);
  }

  *service = iter->second;
  return Status::OK();
}

Status ServiceHub::GetMethod(google::protobuf::Service* service,
                             const std::string& method_name,
                             google::protobuf::MethodDescriptor** method) {
  CHECK_NOTNULL(service);
  CHECK_NOTNULL(method);

  const auto* descriptor =
      service->GetDescriptor()->FindMethodByName(method_name);
  if (descriptor == nullptr) {
    LOG(ERROR) << "Method=" << method_name << " not found in service="
               << service->GetDescriptor()->full_name();
    return Status::NotFound("method not found: " + method_name);
  }

  *method = const_cast<::google::protobuf::MethodDescriptor*>(descriptor);
  return Status::OK();
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
