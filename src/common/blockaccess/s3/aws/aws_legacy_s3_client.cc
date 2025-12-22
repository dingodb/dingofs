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

#include "common/blockaccess/s3/aws/aws_legacy_s3_client.h"

#include <memory>
#include <string>
#include <utility>

#include "aws/core/auth/AWSCredentials.h"
#include "aws/core/client/ClientConfiguration.h"
#include "aws/core/utils/memory/AWSMemory.h"
#include "aws/core/utils/memory/stl/AWSString.h"
#include "aws/s3/S3Client.h"
#include "aws/s3/model/Delete.h"
#include "aws/s3/model/DeleteObjectRequest.h"
#include "aws/s3/model/DeleteObjectsRequest.h"
#include "aws/s3/model/GetObjectRequest.h"
#include "aws/s3/model/HeadBucketRequest.h"
#include "aws/s3/model/HeadObjectRequest.h"
#include "aws/s3/model/ObjectIdentifier.h"
#include "aws/s3/model/PutObjectRequest.h"
#include "glog/logging.h"
#include "opentelemetry/exporters/otlp/otlp_http_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h"
#include "opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h"
#include "smithy/tracing/impl/opentelemetry/OtelTelemetryProvider.h"

namespace dingofs {
namespace blockaccess {
namespace aws {

using namespace Aws::S3;
using namespace Aws::Utils::Logging;

static const int kLogBufferSize = 1024;

void AwsLegacyS3Client::Init(const S3Options& options) {
  LOG(INFO) << fmt::format(
      "[s3_legacy] init aws_legacy_s3_client ak:{} sk:{} s3_endpoint:{}.",
      options.s3_info.ak, options.s3_info.sk, options.s3_info.endpoint);

  s3_options_ = options;

  {
    // init config
    auto config = std::make_unique<Aws::Client::ClientConfiguration>(
        Aws::Client::ClientConfigurationInitValues{.shouldDisableIMDS = true});
    config->endpointOverride = s3_options_.s3_info.endpoint;
    // config->scheme = Aws::Http::Scheme(s3_options_.aws_sdk_config.scheme);
    config->verifySSL = s3_options_.aws_sdk_config.verify_ssl;
    config->userAgent = "S3 Browser";
    config->region = s3_options_.aws_sdk_config.region;
    config->maxConnections = s3_options_.aws_sdk_config.max_connections;
    config->connectTimeoutMs = s3_options_.aws_sdk_config.connect_timeout;
    config->requestTimeoutMs = s3_options_.aws_sdk_config.request_timeout;

    if (s3_options_.aws_sdk_config.use_thread_pool) {
      LOG(INFO) << fmt::format(
          "[s3_legacy] init async thread pool thread num({}).",
          s3_options_.aws_sdk_config.async_thread_num);

      config->executor =
          Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
              "AwsLegacyS3Client", s3_options_.aws_sdk_config.async_thread_num);
    }

    if (s3_options_.aws_sdk_config.enable_telemetry) {
      LOG(INFO) << "[s3_legacy] enable telemetry.";
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

  client_ = std::make_unique<S3Client>(
      Aws::Auth::AWSCredentials(options.s3_info.ak, options.s3_info.sk), *cfg_,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      s3_options_.aws_sdk_config.use_virtual_addressing);
}

bool AwsLegacyS3Client::BucketExist(const std::string& bucket) {
  Model::HeadBucketRequest request;
  request.SetBucket(bucket);
  auto response = client_->HeadBucket(request);

  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_legacy.{}] HeadBucket fail, error({} {}).",
                              bucket, response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return false;
  }

  return true;
}

int AwsLegacyS3Client::PutObject(const std::string& bucket,
                                 const std::string& key, const char* buffer,
                                 size_t buffer_size) {
  Model::PutObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG,
                                                        buffer, buffer_size));

  auto response = client_->PutObject(request);

  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format(
        "[s3_legacy.{}] PutObject error, key({}) error({} {}).", bucket, key,
        response.GetError().GetExceptionName(),
        response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

void AwsLegacyS3Client::AsyncPutObject(const std::string& bucket,
                                       PutObjectAsyncContextSPtr user_ctx) {
  auto aws_ctx = std::make_shared<AwsPutObjectAsyncContext>();

  aws_ctx->user_ctx = user_ctx;

  aws_ctx->request = std::make_any<Model::PutObjectRequest>();
  auto& request = std::any_cast<Model::PutObjectRequest&>(aws_ctx->request);
  request.SetBucket(bucket);
  request.SetKey(std::string{user_ctx->key.c_str(), user_ctx->key.size()});
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(
      AWS_ALLOCATE_TAG, user_ctx->buffer, user_ctx->buffer_size));

  PutObjectResponseReceivedHandler handler =
      [aws_ctx, this, bucket](
          const S3Client* /*client*/,
          const Model::PutObjectRequest& /*request*/,
          const Model::PutObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        AwsPutObjectAsyncContextSPtr aws_ctx =
            std::const_pointer_cast<AwsPutObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsPutObjectAsyncContext>(ctx));

        auto& user_ctx = aws_ctx->user_ctx;

        LOG_IF(ERROR, !response.IsSuccess()) << fmt::format(
            "[s3_legacy.{}] AsyncPutObject fail, key({}) error({} {}).", bucket,
            user_ctx->key, response.GetError().GetExceptionName(),
            response.GetError().GetMessage());

        user_ctx->status =
            (response.IsSuccess()
                 ? Status::OK()
                 : Status::IoError(response.GetError().GetMessage()));
        user_ctx->cb(user_ctx);
      };

  client_->PutObjectAsync(request, handler, aws_ctx);
}

int AwsLegacyS3Client::GetObject(const std::string& bucket,
                                 const std::string& key, std::string* data) {
  Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  Model::GetObjectOutcome response = client_->GetObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_legacy.{}] GetObject error({} {}).", bucket,
                              response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  std::stringstream ss;
  ss << response.GetResult().GetBody().rdbuf();
  *data = ss.str();

  return 0;
}

int AwsLegacyS3Client::RangeObject(const std::string& bucket,
                                   const std::string& key, char* buf,
                                   off_t offset, size_t len) {
  Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(std::string{key.c_str(), key.size()});
  request.SetRange(GetObjectRequestRange(offset, len));

  request.SetResponseStreamFactory([buf, len]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, len);
  });

  auto response = client_->GetObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format(
        "[s3_legacy.{}] RangeObject fail, key({}) error({} {}).", bucket, key,
        response.GetError().GetExceptionName(),
        response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

void AwsLegacyS3Client::AsyncGetObject(const std::string& bucket,
                                       GetObjectAsyncContextSPtr user_ctx) {
  auto aws_ctx = std::make_shared<AwsGetObjectAsyncContext>();

  aws_ctx->user_ctx = user_ctx;

  aws_ctx->request = std::make_any<Model::GetObjectRequest>();
  auto& request = std::any_cast<Model::GetObjectRequest&>(aws_ctx->request);

  request.SetBucket(bucket);
  request.SetKey(std::string{user_ctx->key.c_str(), user_ctx->key.size()});
  request.SetRange(GetObjectRequestRange(user_ctx->offset, user_ctx->len));
  request.SetResponseStreamFactory([user_ctx]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, user_ctx->buf,
                                          user_ctx->len);
  });

  GetObjectResponseReceivedHandler handler =
      [this, aws_ctx, bucket](
          const S3Client* /*client*/,
          const Model::GetObjectRequest& /*request*/,
          const Model::GetObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>& ctx) {
        AwsGetObjectAsyncContextSPtr aws_ctx =
            std::const_pointer_cast<AwsGetObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsGetObjectAsyncContext>(ctx));

        auto& user_ctx = aws_ctx->user_ctx;

        LOG_IF(ERROR, !response.IsSuccess()) << fmt::format(
            "[s3_legacy.{}] AsyncGetObject fail, key({}) error({} {}).", bucket,
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

int AwsLegacyS3Client::DeleteObject(const std::string& bucket,
                                    const std::string& key) {
  Model::DeleteObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  auto response = client_->DeleteObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_legacy.{}] DeleteObject error({} {}).",
                              bucket, response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  return 0;
}

int AwsLegacyS3Client::DeleteObjects(const std::string& bucket,
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
    LOG(ERROR) << fmt::format("[s3_legacy.{}] DeleteObjects error({} {}).",
                              bucket, response.GetError().GetExceptionName(),
                              response.GetError().GetMessage());
    return -1;
  }

  for (const auto& del : response.GetResult().GetDeleted()) {
    VLOG(1) << fmt::format("[s3_legacy.{}] delete ok : {}", bucket,
                           del.GetKey());
  }

  for (const auto& err : response.GetResult().GetErrors()) {
    LOG(WARNING) << fmt::format("[s3_legacy.{}] delete fail, error({} {}).",
                                bucket, err.GetKey(), err.GetMessage());
  }

  if (response.GetResult().GetErrors().size() != 0) {
    return -1;
  }

  return 0;
}

bool AwsLegacyS3Client::ObjectExist(const std::string& bucket,
                                    const std::string& key) {
  Model::HeadObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  auto response = client_->HeadObject(request);
  if (!response.IsSuccess()) {
    LOG(ERROR) << fmt::format("[s3_legacy.{}] HeadObject error({} {}).", bucket,
                              key, response.GetError().GetMessage());
    return false;
  }

  return true;
}

}  // namespace aws
}  // namespace blockaccess
}  // namespace dingofs