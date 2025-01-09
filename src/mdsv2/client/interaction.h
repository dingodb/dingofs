// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gflags/gflags.h>

#include "brpc/channel.h"
#include "brpc/controller.h"
#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/core.h"
#include "mdsv2/common/logging.h"
#include "mdsv2/common/status.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

DECLARE_int32(timeout_ms);
DECLARE_bool(log_each_request);

const int kMaxRetry = 3;

class Interaction {
 public:
  Interaction() = default;
  ~Interaction() = default;

  bool Init(const std::string& addr);

  template <typename Request, typename Response>
  butil::Status SendRequest(const std::string& service_name, const std::string& api_name, const Request& request,
                            Response& response);

 private:
  brpc::Channel channel_;
};

template <typename Request, typename Response>
butil::Status Interaction::SendRequest(const std::string& service_name, const std::string& api_name,
                                       const Request& request, Response& response) {
  const google::protobuf::MethodDescriptor* method = nullptr;

  if (service_name == "MDSService") {
    method = dingofs::pb::mdsv2::MDSService::descriptor()->FindMethodByName(api_name);
  } else {
    DINGO_LOG(FATAL) << "Unknown service name: " << service_name;
  }

  if (method == nullptr) {
    DINGO_LOG(FATAL) << "Unknown api name: " << api_name;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(FLAGS_timeout_ms);
  cntl.set_log_id(butil::fast_rand());

  channel_.CallMethod(method, &cntl, &request, &response, nullptr);
  if (FLAGS_log_each_request) {
    DINGO_LOG(INFO) << fmt::format("send request api {} response: {} request: {}", api_name,
                                   response.ShortDebugString().substr(0, 256),
                                   request.ShortDebugString().substr(0, 256));
  }
  if (cntl.Failed()) {
    DINGO_LOG(ERROR) << fmt::format("{} response failed, {} {} {}", api_name, cntl.log_id(), cntl.ErrorCode(),
                                    cntl.ErrorText());
    return Status(cntl.ErrorCode(), cntl.ErrorText());
  }

  if (response.error().errcode() != dingofs::pb::error::OK) {
    DINGO_LOG(ERROR) << fmt::format("{} response failed, error: {} {}", api_name,
                                    dingofs::pb::error::Errno_Name(response.error().errcode()),
                                    response.error().errmsg());

    return Status(response.error().errcode(), response.error().errmsg());
  }

  return Status();
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs