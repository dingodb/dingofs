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

#include "blockaccess/s3/aws/s3_adapter.h"

#include <aws/core/Aws.h>

#include <cstdlib>

#include "blockaccess/s3/aws/client/aws_crt_s3_client.h"
#include "blockaccess/s3/aws/client/aws_legacy_s3_client.h"

#define AWS_ALLOCATE_TAG __FILE__ ":" STRINGIFY(__LINE__)

namespace dingofs {
namespace blockaccess {
namespace aws {

static std::once_flag s3_init_flag;
static std::once_flag s3_shutdown_flag;
static Aws::SDKOptions aws_sdk_options;

static void CleanUp() { Aws::ShutdownAPI(aws_sdk_options); }

void S3Adapter::Init(const S3Options& options) {
  auto init_sdk = [&]() {
    aws_sdk_options.loggingOptions.logLevel =
        Aws::Utils::Logging::LogLevel(options.aws_sdk_config.loglevel);
    aws_sdk_options.loggingOptions.defaultLogPrefix =
        options.aws_sdk_config.logPrefix.c_str();
    Aws::InitAPI(aws_sdk_options);

    if (std::atexit(CleanUp) != 0) {
      LOG(FATAL) << "Failed to register cleanup function for AWS SDK";
    }
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
}

void S3Adapter::Shutdown() {
//   // one program should only call once
//   auto shutdown_sdk = [&]() { Aws::ShutdownAPI(aws_sdk_options); };
//   std::call_once(s3_shutdown_flag, shutdown_sdk);
}

bool S3Adapter::BucketExist() { return s3_client_->BucketExist(bucket_); }

int S3Adapter::PutObject(const std::string& key, const char* buffer,
                         const size_t buffer_size) {
  return s3_client_->PutObject(bucket_, key, buffer, buffer_size);
}

int S3Adapter::PutObject(const std::string& key, const std::string& data) {
  return PutObject(key, data.data(), data.size());
}

void S3Adapter::PutObjectAsync(std::shared_ptr<PutObjectAsyncContext> context) {
  auto aws_ctx = std::make_shared<AwsPutObjectAsyncContext>();
  aws_ctx->put_obj_ctx = context;
  aws_ctx->cb = [this](const std::shared_ptr<AwsPutObjectAsyncContext>& ctx) {
    // for return
    ctx->put_obj_ctx->ret_code = ctx->retCode;
    ctx->put_obj_ctx->cb(ctx->put_obj_ctx);
  };

  s3_client_->PutObjectAsync(bucket_, aws_ctx);
}

int S3Adapter::GetObject(const std::string& key, std::string* data) {
  return s3_client_->GetObject(bucket_, key, data);
}

int S3Adapter::RangeObject(const std::string& key, char* buf, off_t offset,
                           size_t len) {
  return s3_client_->RangeObject(bucket_, key, buf, offset, len);
}

void S3Adapter::GetObjectAsync(std::shared_ptr<GetObjectAsyncContext> context) {
  auto aws_ctx = std::make_shared<AwsGetObjectAsyncContext>();

  aws_ctx->get_obj_ctx = context;
  aws_ctx->cb = [this](const std::shared_ptr<AwsGetObjectAsyncContext>& ctx) {
    // for return
    ctx->get_obj_ctx->actual_len = ctx->actualLen;
    ctx->get_obj_ctx->ret_code = ctx->retCode;
    ctx->get_obj_ctx->cb(ctx->get_obj_ctx);
  };

  s3_client_->GetObjectAsync(bucket_, aws_ctx);
}

int S3Adapter::DeleteObject(const std::string& key) {
  return s3_client_->DeleteObject(bucket_, key);
}

int S3Adapter::BatchDeleteObject(const std::list<std::string>& key_list) {
  return s3_client_->DeleteObjects(bucket_, key_list);
}

bool S3Adapter::ObjectExist(const std::string& key) {
  return s3_client_->ObjectExist(bucket_, key);
}

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs
