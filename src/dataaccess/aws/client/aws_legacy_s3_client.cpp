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

#include "dataaccess/aws/client/aws_legacy_s3_client.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/Scheme.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/BucketLocationConstraint.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CreateBucketConfiguration.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteBucketRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadBucketRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <glog/logging.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_metric_exporter_options.h>
#include <smithy/tracing/impl/opentelemetry/OtelTelemetryProvider.h>

#include <memory>
#include <utility>

namespace dingofs {
namespace dataaccess {
namespace aws {

void AwsLegacyS3Client::Init(const S3AdapterOption& option) {
  CHECK(!initialized_.load()) << "AwsLegacyS3Client already initialized";
  LOG(INFO) << "AwsLegacyS3Client init ak: " << option.ak
            << " sk: " << option.sk << " s3_address: " << option.s3Address
            << " bucket_name: " << option.bucketName;

  option_ = option;

  {
    // init config
    auto config = std::make_unique<Aws::Client::ClientConfiguration>();
    // config->scheme = Aws::Http::Scheme(option.scheme);
    config->verifySSL = option.verifySsl;
    config->userAgent = "S3 Browser";
    config->region = option.region;
    config->maxConnections = option.maxConnections;
    config->connectTimeoutMs = option.connectTimeout;
    config->requestTimeoutMs = option.requestTimeout;
    config->endpointOverride = option.s3Address;

    if (option.use_thread_pool) {
      LOG(INFO) << "AwsLegacyS3Client init async thread pool thread num = "
                << option.asyncThreadNum;
      config->executor =
          Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
              "AwsLegacyS3Client", option.asyncThreadNum);
    }

    if (option.enableTelemetry) {
      LOG(INFO) << "Enable telemetry for aws s3 adapter";
      ::opentelemetry::exporter::otlp::OtlpHttpExporterOptions opts;
      auto span_exporter =
          ::opentelemetry::exporter::otlp::OtlpHttpExporterFactory::Create(
              opts);

      // otlp http  metric
      ::opentelemetry::exporter::otlp::OtlpHttpMetricExporterOptions
          exporter_options;
      auto push_exporter = ::opentelemetry::exporter::otlp::
          OtlpHttpMetricExporterFactory::Create(exporter_options);

      config->telemetryProvider = smithy::components::tracing::
          OtelTelemetryProvider::CreateOtelProvider(std::move(span_exporter),
                                                    std::move(push_exporter));
    }

    cfg_ = std::move(config);
  }

  client_ = std::make_unique<Aws::S3::S3Client>(
      Aws::Auth::AWSCredentials(option_.ak, option_.sk), *cfg_,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      option.useVirtualAddressing);

  initialized_.store(true);
}

bool AwsLegacyS3Client::BucketExist(std::string bucket) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::HeadBucketRequest request;
  request.SetBucket(bucket);
  auto response = client_->HeadBucket(request);
  if (response.IsSuccess()) {
    return true;
  } else {
    LOG(ERROR) << "HeadBucket error:" << bucket << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return false;
  }
}

int AwsLegacyS3Client::PutObject(std::string bucket, const std::string& key,
                                 const char* buffer, size_t buffer_size) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(AWS_ALLOCATE_TAG,
                                                        buffer, buffer_size));

  auto response = client_->PutObject(request);

  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "PutObject error, bucket: " << bucket << ", key: " << key
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

void AwsLegacyS3Client::PutObjectAsync(
    std::string bucket, std::shared_ptr<AwsPutObjectAsyncContext> context) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::PutObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(std::string{context->put_obj_ctx->key.c_str(),
                             context->put_obj_ctx->key.size()});
  request.SetBody(Aws::MakeShared<PreallocatedIOStream>(
      AWS_ALLOCATE_TAG, context->put_obj_ctx->buffer,
      context->put_obj_ctx->buffer_size));

  Aws::S3::PutObjectResponseReceivedHandler handler =
      [context, this](
          const Aws::S3::S3Client* /*client*/,
          const Aws::S3::Model::PutObjectRequest& /*request*/,
          const Aws::S3::Model::PutObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
              aws_ctx) {
        std::shared_ptr<AwsPutObjectAsyncContext> ctx =
            std::const_pointer_cast<AwsPutObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsPutObjectAsyncContext>(
                    aws_ctx));

        LOG_IF(ERROR, !response.IsSuccess())
            << "PutObjectAsync error: "
            << response.GetError().GetExceptionName()
            << "message: " << response.GetError().GetMessage()
            << "resend: " << ctx->put_obj_ctx->key;

        ctx->retCode = (response.IsSuccess() ? 0 : -1);
        ctx->cb(ctx);
      };

  client_->PutObjectAsync(request, handler, context);
}

int AwsLegacyS3Client::GetObject(std::string bucket, const std::string& key,
                                 std::string* data) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);

  Aws::S3::Model::GetObjectOutcome response = client_->GetObject(request);
  if (response.IsSuccess()) {
    std::stringstream ss;
    ss << response.GetResult().GetBody().rdbuf();
    *data = ss.str();
    return 0;
  } else {
    LOG(ERROR) << "GetObject error: " << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int AwsLegacyS3Client::RangeObject(std::string bucket, const std::string& key,
                                   char* buf, off_t offset, size_t len) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(std::string{key.c_str(), key.size()});
  request.SetRange(GetObjectRequestRange(offset, len));

  request.SetResponseStreamFactory([buf, len]() {
    return Aws::New<PreallocatedIOStream>(AWS_ALLOCATE_TAG, buf, len);
  });

  auto response = client_->GetObject(request);

  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "RangeObject fail, bucket: " << bucket << ", key: " << key
               << " error: " << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

void AwsLegacyS3Client::GetObjectAsync(
    std::string bucket, std::shared_ptr<AwsGetObjectAsyncContext> context) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(std::string{context->get_obj_ctx->key.c_str(),
                             context->get_obj_ctx->key.size()});
  request.SetRange(GetObjectRequestRange(context->get_obj_ctx->offset,
                                         context->get_obj_ctx->len));
  request.SetResponseStreamFactory([context]() {
    return Aws::New<PreallocatedIOStream>(
        AWS_ALLOCATE_TAG, context->get_obj_ctx->buf, context->get_obj_ctx->len);
  });

  Aws::S3::GetObjectResponseReceivedHandler handler =
      [this, context](
          const Aws::S3::S3Client* /*client*/,
          const Aws::S3::Model::GetObjectRequest& /*request*/,
          const Aws::S3::Model::GetObjectOutcome& response,
          const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
              aws_ctx) {
        std::shared_ptr<AwsGetObjectAsyncContext> ctx =
            std::const_pointer_cast<AwsGetObjectAsyncContext>(
                std::dynamic_pointer_cast<const AwsGetObjectAsyncContext>(
                    aws_ctx));

        LOG_IF(ERROR, !response.IsSuccess())
            << "GetObjectAsync error: "
            << response.GetError().GetExceptionName()
            << response.GetError().GetMessage();

        ctx->actualLen = response.GetResult().GetContentLength();
        ctx->retCode = (response.IsSuccess() ? 0 : -1);
        ctx->cb(ctx);
      };

  client_->GetObjectAsync(request, handler, context);
}

int AwsLegacyS3Client::DeleteObject(std::string bucket,
                                    const std::string& key) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::DeleteObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  auto response = client_->DeleteObject(request);
  if (response.IsSuccess()) {
    return 0;
  } else {
    LOG(ERROR) << "DeleteObject error:" << bucket << "--" << key << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return -1;
  }
}

int AwsLegacyS3Client::DeleteObjects(std::string bucket,
                                     const std::list<std::string>& key_list) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::DeleteObjectsRequest delete_objects_request;
  Aws::S3::Model::Delete delete_objects;
  for (const auto& key : key_list) {
    Aws::S3::Model::ObjectIdentifier obj_ident;
    obj_ident.SetKey(key);
    delete_objects.AddObjects(obj_ident);
  }

  delete_objects.SetQuiet(false);
  delete_objects_request.WithBucket(bucket).WithDelete(delete_objects);

  auto response = client_->DeleteObjects(delete_objects_request);
  if (response.IsSuccess()) {
    for (const auto& del : response.GetResult().GetDeleted()) {
      VLOG(1) << "delete ok : " << del.GetKey();
    }

    for (const auto& err : response.GetResult().GetErrors()) {
      LOG(WARNING) << "delete err : " << err.GetKey() << " --> "
                   << err.GetMessage();
    }

    if (response.GetResult().GetErrors().size() != 0) {
      return -1;
    }

    return 0;
  } else {
    LOG(ERROR) << response.GetError().GetMessage() << " failed, "
               << delete_objects_request.SerializePayload();
    return -1;
  }
  return 0;
}

bool AwsLegacyS3Client::ObjectExist(std::string bucket,
                                    const std::string& key) {
  DCHECK(initialized_.load(std::memory_order_relaxed));
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  auto response = client_->HeadObject(request);
  if (response.IsSuccess()) {
    return true;
  } else {
    LOG(ERROR) << "HeadObject error bucket: " << bucket << "--" << key << "--"
               << response.GetError().GetExceptionName()
               << response.GetError().GetMessage();
    return false;
  }
}

}  // namespace aws
}  // namespace dataaccess
}  // namespace dingofs