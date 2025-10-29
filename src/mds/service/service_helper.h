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

#ifndef DINGOFS_MDS_SERVICE_HELPER_H_
#define DINGOFS_MDS_SERVICE_HELPER_H_

#include <cstdint>

#include "brpc/closure_guard.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/core.h"
#include "mds/common/context.h"
#include "mds/common/helper.h"
#include "mds/common/logging.h"
#include "mds/common/runnable.h"
#include "mds/common/status.h"

namespace dingofs {
namespace mds {

DECLARE_uint64(mds_service_log_threshold_time_us);
DECLARE_int32(mds_service_log_print_max_length);

class ServiceHelper {
 public:
  static void SetError(pb::error::Error* error, const Status& status);
  static void SetError(pb::error::Error* error, int errcode, const std::string& errmsg);

  static void SetResponseInfo(const Trace& trace, pb::mds::ResponseInfo* info);

  // protobuf transform
  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedPtrField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedPtrField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.emplace_back(std::move(item));
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(const google::protobuf::RepeatedField<T>& data) {
    std::vector<T> vec;
    vec.reserve(data.size());
    for (auto& item : data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static std::vector<T> PbRepeatedToVector(google::protobuf::RepeatedField<T>* data) {
    std::vector<T> vec;
    vec.reserve(data->size());
    for (auto& item : *data) {
      vec.push_back(item);
    }

    return vec;
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedPtrField<T>* out) {
    for (auto& item : vec) {
      *(out->Add()) = item;
    }
  }

  template <typename T>
  static void VectorToPbRepeated(const std::vector<T>& vec, google::protobuf::RepeatedField<T>* out) {
    for (auto& item : vec) {
      out->Add(item);
    }
  }

  static void PbMapToMap(const google::protobuf::Map<std::string, std::string>& pb_map,
                         std::map<std::string, std::string>& out) {
    for (const auto& item : pb_map) {
      out[item.first] = item.second;
    }
  }

  static void MapToPbMap(const std::map<std::string, std::string>& map,
                         google::protobuf::Map<std::string, std::string>* out) {
    for (const auto& item : map) {
      (*out)[item.first] = item.second;
    }
  }
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

class TraceClosure : public google::protobuf::Closure {
 public:
  TraceClosure() { start_time_us = Helper::TimestampUs(); }
  ~TraceClosure() override = default;

  void SetQueueWaitTime() { queue_wait_time_us = Helper::TimestampUs() - start_time_us; }

 protected:
  uint64_t start_time_us{0};
  uint64_t queue_wait_time_us{0};
};

// Wrapper brpc service closure for log.
template <typename T, typename U>
class ServiceClosure : public TraceClosure {
 public:
  ServiceClosure(const std::string& method_name, google::protobuf::Closure* done, const T* request, U* response)
      : method_name_(method_name), done_(done), request_(request), response_(response) {}

  ~ServiceClosure() override = default;

  void Run() override;

 private:
  std::string method_name_;

  google::protobuf::Closure* done_;
  const T* request_;
  U* response_;
};

template <typename T, typename U>
void ServiceClosure<T, U>::Run() {
  std::unique_ptr<ServiceClosure<T, U>> self_guard(this);
  brpc::ClosureGuard done_guard(done_);

  uint64_t elapsed_time_us = Helper::TimestampUs() - start_time_us;

  auto* mut_time = response_->mutable_info()->mutable_time();
  mut_time->set_total_time_us(elapsed_time_us);
  mut_time->set_queue_wait_time_us(queue_wait_time_us);

  if (response_->error().errcode() != 0) {
    if (response_->error().errcode() != pb::error::ENOT_FOUND) {
      LOG(ERROR) << fmt::format("[service.{}][request_id({})][{}us] Request fail, request({}) response({})",
                                method_name_, request_->info().request_id(), elapsed_time_us,
                                request_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length),
                                response_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length));
    }
  } else {
    if (BAIDU_UNLIKELY(elapsed_time_us >= FLAGS_mds_service_log_threshold_time_us)) {
      LOG(INFO) << fmt::format("[service.{}][request_id({})][{}us] Request finish, request({}) response({})",
                               method_name_, request_->info().request_id(), elapsed_time_us,
                               request_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length),
                               response_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length));
    } else {
      LOG(INFO) << fmt::format("[service.{}][request_id({})][{}us] Request finish, request({}) response({})",
                               method_name_, request_->info().request_id(), elapsed_time_us,
                               request_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length),
                               response_->ShortDebugString().substr(0, FLAGS_mds_service_log_print_max_length));
    }
  }
}

}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_SERVICE_HELPER_H_