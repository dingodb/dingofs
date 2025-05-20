/*
 *  Copyright (c) 2020 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*************************************************************************
> File Name: s3_adapter.cpp
> Author:
> Created Time: Wed Dec 19 15:19:40 2018
 ************************************************************************/

#include "dataaccess/s3/aws/s3_adapter.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>
#include <aws/core/Aws.h>
#include <butil/time.h>
#include <glog/logging.h>

#include <memory>
#include <string>

#include "dataaccess/s3/aws/client/aws_crt_s3_client.h"
#include "dataaccess/s3/aws/client/aws_legacy_s3_client.h"
#include "dataaccess/s3/aws/s3_access_log.h"
#include "utils/dingo_define.h"
#include "utils/macros.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace dingofs {
namespace dataaccess {
namespace aws {

using dingofs::utils::kMB;

static bvar::Adder<uint64_t> s3_object_put_async_num("s3_object_put_async_num");
static bvar::Adder<uint64_t> s3_object_put_sync_num("s3_object_put_sync_num");
static bvar::Adder<uint64_t> s3_object_get_async_num("s3_object_get_async_num");
static bvar::Adder<uint64_t> s3_object_get_sync_num("s3_object_get_sync_num");

static std::once_flag s3_init_flag;
static std::once_flag s3_shutdown_flag;
static Aws::SDKOptions aws_sdk_options;

void S3Adapter::Init(const S3Options& options) {
  // TODO: refact this
  auto init_sdk = [&]() {
    aws_sdk_options.loggingOptions.logLevel =
        Aws::Utils::Logging::LogLevel(options.aws_sdk_config.loglevel);
    aws_sdk_options.loggingOptions.defaultLogPrefix =
        options.aws_sdk_config.logPrefix.c_str();
    Aws::InitAPI(aws_sdk_options);
  };
  std::call_once(s3_init_flag, init_sdk);

  s3_options_ = options;
  bucket_ = options.s3_info.bucket_name;

  if (options.aws_sdk_config.use_crt_client) {
    s3_client_ = std::make_unique<AwsCrtS3Client>();
  } else {
    // init aws s3 client
    s3_client_ = std::make_unique<AwsLegacyS3Client>();
  }

  s3_client_->Init(s3_options_);

  {
    utils::ReadWriteThrottleParams params;
    params.iopsTotal.limit = options.aws_sdk_config.iopsTotalLimit;
    params.iopsRead.limit = options.aws_sdk_config.iopsReadLimit;
    params.iopsWrite.limit = options.aws_sdk_config.iopsWriteLimit;
    params.bpsTotal.limit = options.aws_sdk_config.bpsTotalMB * kMB;
    params.bpsRead.limit = options.aws_sdk_config.bpsReadMB * kMB;
    params.bpsWrite.limit = options.aws_sdk_config.bpsWriteMB * kMB;

    throttle_ = std::make_unique<utils::Throttle>();
    throttle_->UpdateThrottleParams(params);

    inflightBytesThrottle_ =
        std::make_unique<AsyncRequestInflightBytesThrottle>(
            options.aws_sdk_config.maxAsyncRequestInflightBytes == 0
                ? UINT64_MAX
                : options.aws_sdk_config.maxAsyncRequestInflightBytes);
  }
}

void S3Adapter::Shutdown() {
  // one program should only call once
  auto shutdown_sdk = [&]() { Aws::ShutdownAPI(aws_sdk_options); };
  std::call_once(s3_shutdown_flag, shutdown_sdk);
}

void S3Adapter::Reinit(const S3Options& options) {
  Init(options);
}

std::string S3Adapter::GetS3Ak() { return s3_client_->GetAk(); }

std::string S3Adapter::GetS3Sk() { return s3_client_->GetSk(); }

std::string S3Adapter::GetS3Endpoint() { return s3_client_->GetEndpoint(); }

bool S3Adapter::BucketExist() {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("head_bucket %s : %d", bucket_, rc);
  });

  rc = s3_client_->BucketExist(bucket_);
  return rc;
}

int S3Adapter::PutObject(const std::string& key, const char* buffer,
                         const size_t buffer_size) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("put_object %s (%d) : %d", key, buffer_size, rc);
  });

  s3_object_put_sync_num << 1;

  auto dec = ::absl::MakeCleanup([&]() { s3_object_put_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(false, buffer_size);
  }

  rc = s3_client_->PutObject(bucket_, key, buffer, buffer_size);
  return rc;
}

int S3Adapter::PutObject(const std::string& key, const std::string& data) {
  return PutObject(key, data.data(), data.size());
}

void S3Adapter::PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  s3_object_put_async_num << 1;

  auto aws_ctx = std::make_shared<AwsPutObjectAsyncContext>();
  aws_ctx->put_obj_ctx = context;
  aws_ctx->cb =
      [this, start_us](const std::shared_ptr<AwsPutObjectAsyncContext>& ctx) {
        S3AccessLogGuard log(start_us, [&]() {
          return absl::StrFormat(
              "async_put_object %s (%d) : %d", ctx->put_obj_ctx->key,
              ctx->put_obj_ctx->buffer_size, ctx->put_obj_ctx->ret_code);
        });

        s3_object_put_async_num << -1;
        inflightBytesThrottle_->OnComplete(ctx->put_obj_ctx->buffer_size);

        // for return
        ctx->put_obj_ctx->ret_code = ctx->retCode;
        ctx->put_obj_ctx->cb(ctx->put_obj_ctx);
      };

  if (throttle_) {
    throttle_->Add(false, context->buffer_size);
  }

  inflightBytesThrottle_->OnStart(context->buffer_size);

  s3_client_->PutObjectAsync(bucket_, aws_ctx);
}

int S3Adapter::GetObject(const std::string& key, std::string* data) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("get_object %s : %d", key, rc);
  });

  s3_object_get_sync_num << 1;
  auto dec = ::absl::MakeCleanup([&]() { s3_object_get_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(true, 1);
  }

  rc = s3_client_->GetObject(bucket_, key, data);
  return rc;
}

int S3Adapter::GetObject(const std::string& key, char* buf, off_t offset,
                         size_t len) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("range_object %s (%d,%d) : %d", key, offset, len,
                           rc);
  });

  s3_object_get_async_num << 1;
  auto dec = ::absl::MakeCleanup([&]() { s3_object_get_async_num << -1; });

  if (throttle_) {
    throttle_->Add(true, len);
  }

  rc = s3_client_->RangeObject(bucket_, key, buf, offset, len);
  return rc;
}

void S3Adapter::GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  s3_object_get_async_num << 1;

  auto aws_ctx = std::make_shared<AwsGetObjectAsyncContext>();
  aws_ctx->get_obj_ctx = context;
  aws_ctx->cb = [this, start_us](
                    const std::shared_ptr<AwsGetObjectAsyncContext>& ctx) {
    S3AccessLogGuard log(start_us, [&]() {
      return absl::StrFormat("async_get_object %s (%d,%d) : %d",
                             ctx->get_obj_ctx->key, ctx->get_obj_ctx->offset,
                             ctx->get_obj_ctx->len, ctx->get_obj_ctx->ret_code);
    });

    s3_object_get_async_num << -1;
    inflightBytesThrottle_->OnComplete(ctx->get_obj_ctx->len);

    // for return
    ctx->get_obj_ctx->actual_len = ctx->actualLen;
    ctx->get_obj_ctx->ret_code = ctx->retCode;
    ctx->get_obj_ctx->cb(ctx->get_obj_ctx);
  };

  if (throttle_) {
    throttle_->Add(true, context->len);
  }

  inflightBytesThrottle_->OnStart(context->len);

  s3_client_->GetObjectAsync(bucket_, aws_ctx);
}

int S3Adapter::DeleteObject(const std::string& key) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("delete_object %s : %d", key, rc);
  });

  rc = s3_client_->DeleteObject(bucket_, key);
  return rc;
}

int S3Adapter::DeleteObjects(const std::list<std::string>& key_list) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("delete_objects (%d) : %d", key_list.size(), rc);
  });

  rc = s3_client_->DeleteObjects(bucket_, key_list);
  return rc;
}

bool S3Adapter::ObjectExist(const std::string& key) {
  int rc = 0;
  S3AccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("head_object %s : %c", key, rc);
  });
  rc = s3_client_->ObjectExist(bucket_, key);
  return rc;
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnStart(uint64_t len) {
  std::unique_lock<std::mutex> lock(mtx_);
  while (inflightBytes_ + len > maxInflightBytes_) {
    cond_.wait(lock);
  }

  inflightBytes_ += len;
}

void S3Adapter::AsyncRequestInflightBytesThrottle::OnComplete(uint64_t len) {
  std::unique_lock<std::mutex> lock(mtx_);
  inflightBytes_ -= len;
  cond_.notify_all();
}

}  // namespace aws
}  // namespace dataaccess
}  // namespace dingofs
