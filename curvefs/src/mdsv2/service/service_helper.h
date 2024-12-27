// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_SERVICE_HELPER_H_
#define DINGOFS_MDSV2_SERVICE_HELPER_H_

#include <cstdint>

#include "brpc/closure_guard.h"
#include "curvefs/proto/error.pb.h"
#include "curvefs/src/mdsv2/common/helper.h"
#include "curvefs/src/mdsv2/common/logging.h"
#include "curvefs/src/mdsv2/common/runnable.h"
#include "fmt/core.h"

namespace dingofs {

namespace mdsv2 {

DECLARE_int64(service_log_threshold_time_ns);
DECLARE_int32(log_print_max_length);

class ServiceHelper {
 public:
  static void SetError(pb::error::Error* error, int errcode, const std::string& errmsg);
};

// Handle service request in execute queue.
class ServiceTask : public TaskRunnable {
 public:
  using Handler = std::function<void(void)>;
  ServiceTask(Handler handle) : handle_(handle) {}
  ~ServiceTask() override = default;

  std::string Type() override { return "SERVICE_TASK"; }

  void Run() override { handle_(); }

 private:
  Handler handle_;
};

// Wrapper brpc service closure for log.
template <typename T, typename U>
class ServiceClosure : public google::protobuf::Closure {
 public:
  ServiceClosure(const std::string& method_name, google::protobuf::Closure* done, const T* request, U* response)
      : method_name_(method_name), done_(done), request_(request), response_(response) {
    start_time_ = Helper::TimestampNs();
  }

  ~ServiceClosure() override = default;

  void Run() override;

 private:
  std::string method_name_;

  uint64_t start_time_;

  google::protobuf::Closure* done_;
  const T* request_;
  U* response_;
};

template <typename T, typename U>
void ServiceClosure<T, U>::Run() {
  std::unique_ptr<ServiceClosure<T, U>> self_guard(this);
  brpc::ClosureGuard done_guard(done_);

  uint64_t elapsed_time = Helper::TimestampNs() - start_time_;

  if (response_->error().errcode() != 0) {
    DINGO_LOG(ERROR) << fmt::format(
        "[service.{}][request_id({})][elapsed(ns)({})] Request failed, response: {} request: {}", method_name_,
        request_->request_info().request_id(), elapsed_time,
        response_->ShortDebugString().substr(0, FLAGS_log_print_max_length),
        request_->ShortDebugString().substr(0, FLAGS_log_print_max_length));
  } else {
    if (BAIDU_UNLIKELY(elapsed_time >= FLAGS_service_log_threshold_time_ns)) {
      DINGO_LOG(INFO) << fmt::format(
          "[service.{}][request_id({})][elapsed(ns)({})] Request finish, response: {} request: {}", method_name_,
          request_->request_info().request_id(), elapsed_time,
          response_->ShortDebugString().substr(0, FLAGS_log_print_max_length),
          request_->ShortDebugString().substr(0, FLAGS_log_print_max_length));
    } else {
      DINGO_LOG(DEBUG) << fmt::format(
          "[service.{}][request_id({})][elapsed(ns)({})] Request finish, response: {} request: {}", method_name_,
          request_->request_info().request_id(), elapsed_time,
          response_->ShortDebugString().substr(0, FLAGS_log_print_max_length),
          request_->ShortDebugString().substr(0, FLAGS_log_print_max_length));
    }
  }
}

}  // namespace mdsv2

}  // namespace dingofs

#endif  // DINGOFS_MDSV2_SERVICE_HELPER_H_