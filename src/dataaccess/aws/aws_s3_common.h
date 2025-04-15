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
#include "utils/configuration.h"
#include "utils/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace dingofs {
namespace dataaccess {
namespace aws {

struct S3AdapterOption {
  std::string ak;
  std::string sk;
  std::string s3Address;
  std::string bucketName;
  std::string region;
  int loglevel;
  std::string logPrefix;
  bool verifySsl;
  int maxConnections;
  int connectTimeout;
  int requestTimeout;
  bool use_crt_client{true};
  bool use_thread_pool{true};  // this only work when use_crt_client is false
  int asyncThreadNum{256};     // this only work when use_crt_client is false
  uint64_t maxAsyncRequestInflightBytes;
  uint64_t iopsTotalLimit;
  uint64_t iopsReadLimit;
  uint64_t iopsWriteLimit;
  uint64_t bpsTotalMB;
  uint64_t bpsReadMB;
  uint64_t bpsWriteMB;
  bool useVirtualAddressing;
  bool enableTelemetry;
};

struct S3InfoOption {
  // should get from mds
  std::string ak;
  std::string sk;
  std::string s3Address;
  std::string bucketName;
  uint64_t blockSize;
  uint64_t chunkSize;
  uint32_t objectPrefix;
};

inline void InitS3AdaptorOptionExceptS3InfoOption(utils::Configuration* conf,
                                                  S3AdapterOption* s3_opt) {
  LOG_IF(FATAL, !conf->GetIntValue("s3.logLevel", &s3_opt->loglevel));
  LOG_IF(FATAL, !conf->GetStringValue("s3.logPrefix", &s3_opt->logPrefix));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.verify_SSL", &s3_opt->verifySsl));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.maxConnections", &s3_opt->maxConnections));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.connectTimeout", &s3_opt->connectTimeout));
  LOG_IF(FATAL,
         !conf->GetIntValue("s3.requestTimeout", &s3_opt->requestTimeout));

  if (!conf->GetBoolValue("s3.use_crt_client", &s3_opt->use_crt_client)) {
    LOG(INFO) << "Not found s3.use_crt_client in conf, use default "
              << (s3_opt->use_crt_client ? "true" : "false");
  }

  if (!conf->GetBoolValue("s3.use_thread_pool", &s3_opt->use_thread_pool)) {
    LOG(INFO) << "Not found s3.use_thread_pool in conf, use default "
              << (s3_opt->use_thread_pool ? "true" : "false");
  }

  if (!conf->GetIntValue("s3.async_thread_num_in_thread_pool",
                         &s3_opt->asyncThreadNum)) {
    LOG(INFO)
        << "Not found s3.async_thread_num_in_thread_pool in conf, use default"
        << s3_opt->asyncThreadNum;
  }

  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsTotalLimit",
                                      &s3_opt->iopsTotalLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsReadLimit",
                                      &s3_opt->iopsReadLimit));
  LOG_IF(FATAL, !conf->GetUInt64Value("s3.throttle.iopsWriteLimit",
                                      &s3_opt->iopsWriteLimit));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsTotalMB", &s3_opt->bpsTotalMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsReadMB", &s3_opt->bpsReadMB));
  LOG_IF(FATAL,
         !conf->GetUInt64Value("s3.throttle.bpsWriteMB", &s3_opt->bpsWriteMB));
  LOG_IF(FATAL, !conf->GetBoolValue("s3.useVirtualAddressing",
                                    &s3_opt->useVirtualAddressing));
  LOG_IF(FATAL, !conf->GetStringValue("s3.region", &s3_opt->region));

  if (!conf->GetUInt64Value("s3.maxAsyncRequestInflightBytes",
                            &s3_opt->maxAsyncRequestInflightBytes)) {
    LOG(WARNING) << "Not found s3.maxAsyncRequestInflightBytes in conf";
    s3_opt->maxAsyncRequestInflightBytes = 0;
  }
  if (!conf->GetBoolValue("s3.enableTelemetry", &s3_opt->enableTelemetry)) {
    LOG(WARNING) << "Not found s3.enableTelemetry in conf,default to false";
    s3_opt->enableTelemetry = false;
  }
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