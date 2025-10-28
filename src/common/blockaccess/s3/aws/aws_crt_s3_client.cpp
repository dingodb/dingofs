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

#include "common/blockaccess/s3/aws/aws_crt_s3_client.h"

#include <aws/core/utils/logging/LogLevel.h>

#include <any>
#include <memory>

#include "aws/s3-crt/model/DeleteObjectRequest.h"
#include "aws/s3-crt/model/DeleteObjectsRequest.h"
#include "aws/s3-crt/model/GetObjectRequest.h"
#include "aws/s3-crt/model/HeadBucketRequest.h"
#include "aws/s3-crt/model/HeadObjectRequest.h"
#include "aws/s3-crt/model/PutObjectRequest.h"
#include "common/blockaccess/s3/aws/aws_s3_common.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h"
#include "smithy/tracing/impl/opentelemetry/OtelTelemetryProvider.h"

namespace dingofs {
namespace blockaccess {
namespace aws {

using namespace Aws::S3Crt;
using Aws::Client::AWSAuthV4Signer;

using namespace Aws::Utils::Logging;

static const int kLogBufferSize = 1024;

class CrtLog : public CRTLogSystemInterface {
 public:
  CrtLog(LogLevel log_level) : log_level_(log_level) {}

  LogLevel GetLogLevel() const override { return log_level_; }
  void SetLogLevel(LogLevel logLevel) override { log_level_ = logLevel; }

  void Log(LogLevel logLevel, const char* subjectName, const char* formatStr,
           va_list args) override {
    char message[kLogBufferSize] = {0};
    int ret = vsnprintf(message, kLogBufferSize, formatStr, args);
    if (ret < 0) {
      LOG(ERROR) << fmt::format("[s3_crt.aws_sdk.{}] vsnprintf error.",
                                subjectName);
      return;
    } else if (ret >= kLogBufferSize) {
      LOG(WARNING) << fmt::format(
          "[s3_crt.aws_sdk.{}] vsnprintf truncated message, original size({}) "
          "> buffer size({}).",
          subjectName, ret, kLogBufferSize);
      message[kLogBufferSize - 1] = '\0';  // Ensure null termination
    }

    switch (logLevel) {
      case LogLevel::Off:
        break;

      case LogLevel::Fatal:
        LOG(FATAL) << fmt::format("[s3_crt.aws_sdk.{}] {}.", subjectName,
                                  message);
        break;
      case LogLevel::Error:
        LOG(ERROR) << fmt::format("[s3_crt.aws_sdk.{}] {}.", subjectName,
                                  message);
        break;

      case LogLevel::Warn:
        LOG(WARNING) << fmt::format("[s3_crt.aws_sdk.{}] {}.", subjectName,
                                    message);
        break;

      case LogLevel::Info:
      case LogLevel::Debug:
      case LogLevel::Trace:
        LOG(INFO) << fmt::format("[s3_crt.aws_sdk.{}] {}.", subjectName,
                                 message);
        break;
      default:
        break;
    }
  }

  void CleanUp() override {}

 private:
  LogLevel log_level_;
};

AwsCrtS3Client::CRTLogSystemInterfaceSPtr AwsCrtS3Client::CreateLogger(
    int log_level) {
  return std::make_shared<CrtLog>(static_cast<LogLevel>(log_level));
}

void AwsCrtS3Client::Init(const S3Options& options) {
  LOG(INFO) << fmt::format(
      "[s3_crt] init aws_crt_s3_client ak:{} sk:{} s3_endpoint:{}.",
      options.s3_info.ak, options.s3_info.sk, options.s3_info.endpoint);

  s3_options_ = options;

  {
    auto config = std::make_unique<S3CrtClientConfiguration>();
    config->endpointOverride = options.s3_info.endpoint;
    config->verifySSL = options.aws_sdk_config.verify_ssl;
    config->region = options.aws_sdk_config.region;
    config->maxConnections = options.aws_sdk_config.max_connections;
    config->connectTimeoutMs = options.aws_sdk_config.connect_timeout;
    config->requestTimeoutMs = options.aws_sdk_config.request_timeout;
    config->useVirtualAddressing =
        options.aws_sdk_config.use_virtual_addressing;

    if (options.aws_sdk_config.enable_telemetry) {
      LOG(INFO) << "[s3_crt] enable telemetry.";
      using namespace ::opentelemetry::exporter::otlp;

      OtlpHttpExporterOptions opts;
      auto span_exporter = OtlpHttpExporterFactory::Create(opts);

      // otlp http  metric
      OtlpHttpMetricExporterOptions exporter_options;
      auto push_exporter =
          OtlpHttpMetricExporterFactory::Create(exporter_options);

      config->telemetryProvider = smithy::components::tracing::
          OtelTelemetryProvider::CreateOtelProvider(std::move(span_exporter),
                                                    std::move(push_exporter));
    }

    cfg_ = std::move(config);
  }

  client_ = std::make_unique<S3CrtClient>(
      Aws::Auth::AWSCredentials(s3_options_.s3_info.ak, s3_options_.s3_info.sk),
      *cfg_, AWSAuthV4Signer::PayloadSigningPolicy::Never);
}

bool AwsCrtS3Client::BucketExist(const std::string& bucket) {
  Model::HeadBucketRequest request;
  request.SetBucket(bucket);

  auto response = client_->HeadBucket(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_crt.{}] HeadBucket fail, error({} {}).",
                              bucket, response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return false;
  }

  return true;
}

int AwsCrtS3Client::PutObject(const std::string& bucket, const std::string& key,
                              const char* buffer, size_t buffer_size) {
  Model::PutObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG,
                                                        buffer, buffer_size));

  auto response = client_->PutObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format(
        "[s3_crt.{}] PutObject error, key({}) error({} {}).", bucket, key,
        response.GetError().GetExceptionName(),
        response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

void AwsCrtS3Client::AsyncPutObject(const std::string& bucket,
                                    PutObjectAsyncContextSPtr user_ctx) {
  auto aws_ctx = std::make_shared<AwsPutObjectAsyncContext>();

  aws_ctx->user_ctx = user_ctx;
  aws_ctx->request = std::make_any<Model::PutObjectRequest>();

  auto& request = std::any_cast<Model::PutObjectRequest&>(aws_ctx->request);
  request.SetBucket(bucket);
  request.SetKey(user_ctx->key);
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(
      AWS_ALLOCATE_TAG, user_ctx->buffer, user_ctx->buffer_size));

  PutObjectResponseReceivedHandler handler =
      [this, bucket](
          const S3CrtClient* /*client*/,
          const Model::PutObjectRequest& /*request*/,
          const Model::PutObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        AwsPutObjectAsyncContextSPtr aws_ctx =
            std::const_pointer_cast<AwsPutObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsPutObjectAsyncContext>(ctx));

        auto& user_ctx = aws_ctx->user_ctx;

        LOG_IF(ERROR, !response.IsSuccess()) << fmt::format(
            "[s3_crt.{}] AsyncPutObject fail, key({}) error({} {}).", bucket,
            user_ctx->key, response.GetError().GetExceptionName(),
            response.GetError().GetMessage());

        user_ctx->status =
            response.IsSuccess()
                ? Status::OK()
                : Status::IoError(response.GetError().GetMessage());
        user_ctx->cb(user_ctx);
      };

  client_->PutObjectAsync(request, handler, aws_ctx);
}

int AwsCrtS3Client::GetObject(const std::string& bucket, const std::string& key,
                              std::string* data) {
  Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  Model::GetObjectOutcome response = client_->GetObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_crt.{}] GetObject error({} {}).", bucket,
                              response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  std::stringstream ss;
  ss << response.GetResult().GetBody().rdbuf();
  *data = ss.str();

  return 0;
}

int AwsCrtS3Client::RangeObject(const std::string& bucket,
                                const std::string& key, char* buf, off_t offset,
                                size_t len) {
  Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  request.SetRange(GetObjectRequestRange(offset, len));
  request.SetResponseStreamFactory([buf, len]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, len);
  });

  auto response = client_->GetObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format(
        "[s3_crt.{}] RangeObject fail, key({}) error({} {}).", bucket, key,
        response.GetError().GetExceptionName(),
        response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

void AwsCrtS3Client::AsyncGetObject(const std::string& bucket,
                                    GetObjectAsyncContextSPtr user_ctx) {
  auto aws_ctx = std::make_shared<AwsGetObjectAsyncContext>();

  aws_ctx->user_ctx = user_ctx;

  aws_ctx->request = std::make_any<Model::GetObjectRequest>();
  auto& request = std::any_cast<Model::GetObjectRequest&>(aws_ctx->request);

  request.SetBucket(bucket);
  request.SetKey(user_ctx->key);
  request.SetRange(GetObjectRequestRange(user_ctx->offset, user_ctx->len));
  request.SetResponseStreamFactory([user_ctx]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, user_ctx->buf,
                                          user_ctx->len);
  });

  GetObjectResponseReceivedHandler handler =
      [this, bucket](
          const S3CrtClient* /*client*/,
          const Model::GetObjectRequest& /*request*/,
          const Model::GetObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        AwsGetObjectAsyncContextSPtr aws_ctx =
            std::const_pointer_cast<AwsGetObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsGetObjectAsyncContext>(ctx));

        auto& user_ctx = aws_ctx->user_ctx;

        LOG_IF(ERROR, !response.IsSuccess()) << fmt::format(
            "[s3_crt.{}] AsyncGetObject fail, key({}) error({} {}).", bucket,
            user_ctx->key, response.GetError().GetExceptionName(),
            response.GetError().GetMessage());

        user_ctx->actual_len = response.GetResult().GetContentLength();
        user_ctx->status =
            response.IsSuccess()
                ? Status::OK()
                : Status::IoError(response.GetError().GetMessage());
        user_ctx->cb(user_ctx);
      };

  client_->GetObjectAsync(request, handler, aws_ctx);
}

int AwsCrtS3Client::DeleteObject(const std::string& bucket,
                                 const std::string& key) {
  Model::DeleteObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  auto response = client_->DeleteObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_crt.{}] DeleteObject error({} {}).", bucket,
                              response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

int AwsCrtS3Client::DeleteObjects(const std::string& bucket,
                                  const std::list<std::string>& key_list) {
  Model::Delete delete_objects;
  delete_objects.SetQuiet(false);
  for (const auto& key : key_list) {
    Model::ObjectIdentifier obj_ident;
    obj_ident.SetKey(key);
    delete_objects.AddObjects(obj_ident);
  }

  Model::DeleteObjectsRequest request;
  request.WithBucket(bucket).WithDelete(delete_objects);

  auto response = client_->DeleteObjects(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_crt.{}] DeleteObjects error({} {}).", bucket,
                              response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  for (const auto& del : response.GetResult().GetDeleted()) {
    VLOG(1) << fmt::format("[s3_crt.{}] delete ok : {}", bucket, del.GetKey());
  }

  for (const auto& err : response.GetResult().GetErrors()) {
    LOG(WARNING) << fmt::format("[s3_crt.{}] delete fail, error({} {}).",
                                bucket, err.GetKey(), err.GetMessage());
  }

  if (response.GetResult().GetErrors().size() != 0) {
    return -1;
  }

  return 0;
}

bool AwsCrtS3Client::ObjectExist(const std::string& bucket,
                                 const std::string& key) {
  Model::HeadObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  auto response = client_->HeadObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_crt.{}] HeadObject error({} {}).", bucket,
                              key, response.GetError().GetMessage());
    return false;
  }

  return true;
}

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs