/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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

#ifndef DATA_ACCESS_AWS_S3_COMMON_H
#define DATA_ACCESS_AWS_S3_COMMON_H

#include <aws/core/client/AsyncCallerContext.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>

#include <string>

#include "dataaccess/accesser_common.h"
#include "options/client/s3.h"
#include "options/options.h"
#include "utils/configuration.h"
#include "utils/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace dingofs {
namespace dataaccess {
namespace aws {

using options::client::S3Option;

inline void InitS3AdaptorOptionExceptS3InfoOption(utils::Configuration* conf,
                                                  S3Option* s3_option) {
  auto* global_option = &s3_option->global_option();
  auto* request_option = &s3_option->request_option();
  auto* throttle_option = &s3_option->throttle_option();

  // global
  LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &global_option->log_level()));
  LOG_IF(FATAL,
         !conf->GetStringValue("s3.logPrefix", &global_option->log_prefix()));
  if (!conf->GetBoolValue("s3.enableTelemetry",
                          &global_option->telemetry_enable())) {
    LOG(WARNING) << "Not found s3.enableTelemetry in conf,default to false";
    global_option->telemetry_enable() = false;
  }

  // request
  LOG_IF(FATAL,
         !conf->GetBoolValue("s3.verify_SSL", &request_option->verify_ssl()));
  LOG_IF(FATAL, !conf->GetIntValue("s3.maxConnections",
                                   &request_option->max_connections()));
  LOG_IF(FATAL, !conf->GetIntValue("s3.connectTimeout",
                                   &request_option->connect_timeout_ms()));
  LOG_IF(FATAL, !conf->GetIntValue("s3.requestTimeout",
                                   &request_option->request_timeout_ms()));

  if (!conf->GetBoolValue("s3.use_crt_client",
                          &request_option->use_crt_client())) {
    LOG(INFO) << "Not found s3.use_crt_client in conf, use default "
              << (request_option->use_crt_client() ? "true" : "false");
  }
  if (!conf->GetBoolValue("s3.use_thread_pool",
                          &request_option->use_thread_pool())) {
    LOG(INFO) << "Not found s3.use_thread_pool in conf, use default "
              << (request_option->use_thread_pool() ? "true" : "false");
  }
  if (!conf->GetIntValue("s3.async_thread_num_in_thread_pool",
                         &request_option->async_thread_num())) {
    LOG(INFO)
        << "Not found s3.async_thread_num_in_thread_pool in conf, use default"
        << request_option->async_thread_num();
  }
  if (!conf->GetUInt64Value(
          "s3.maxAsyncRequestInflightBytes",
          &request_option->max_async_request_inflight_bytes())) {
    LOG(WARNING) << "Not found s3.maxAsyncRequestInflightBytes in conf";
  }

  // throttle
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsTotalLimit",
                                      &throttle_option->iops_total_limit()));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsReadLimit",
                                      &throttle_option->iops_read_limit()));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsWriteLimit",
                                      &throttle_option->iops_write_limit()));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsTotalMB",
                                      &throttle_option->bps_total_mb()));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsReadMB",
                                      &throttle_option->bps_read_mb()));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.bpsWriteMB",
                                      &throttle_option->bps_write_mb()));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
                                    &request_option->use_virtual_addressing()));
  LOG_IF(FATAL, !conf->GetStringValue("s3.region", &request_option->region()));
}

struct AwsGetObjectAsyncContext;
struct AwsPutObjectAsyncContext;

using AwsGetObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<AwsGetObjectAsyncContext>&)>;

struct AwsGetObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::shared_ptr<GetObjectAsyncContext> get_obj_ctx;
  AwsGetObjectAsyncCallBack cb;
  int retCode;
  size_t actualLen;
};

using AwsPutObjectAsyncCallBack =
    std::function<void(const std::shared_ptr<AwsPutObjectAsyncContext>&)>;

struct AwsPutObjectAsyncContext : public Aws::Client::AsyncCallerContext {
  std::shared_ptr<PutObjectAsyncContext> put_obj_ctx;
  AwsPutObjectAsyncCallBack cb;
  int retCode;
};

// https://github.com/aws/aws-sdk-cpp/issues/1430
class PreallocatedIOStream : public Aws::IOStream {
 public:
  PreallocatedIOStream(char* buf, size_t size)
      : Aws::IOStream(new Aws::Utils::Stream::PreallocatedStreamBuf(
            reinterpret_cast<unsigned char*>(buf), size)) {}

  PreallocatedIOStream(const char* buf, size_t size)
      : PreallocatedIOStream(const_cast<char*>(buf), size) {}

  ~PreallocatedIOStream() override {
    // corresponding new in constructor
    delete rdbuf();
  }
};

// https://developer.mozilla.org/zh-CN/docs/Web/HTTP/Range_requests
inline Aws::String GetObjectRequestRange(uint64_t offset, uint64_t len) {
  CHECK_GT(len, 0);
  auto range = "bytes=" + std::to_string(offset) + "-" +
               std::to_string(offset + len - 1);
  return {range.data(), range.size()};
}

}  // namespace aws
}  // namespace dataaccess
}  // namespace dingofs

#endif  // DATA_ACCESS_AWS_S3_COMMON_H