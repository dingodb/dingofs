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

#include "common/blockaccess/s3/s3_accesser.h"

#include <cstdlib>
#include <memory>

#include "aws/core/Aws.h"
#include "aws/crt/io/Bootstrap.h"
#include "aws/crt/io/EventLoopGroup.h"
#include "aws/crt/io/HostResolver.h"
#include "common/blockaccess/s3/aws/aws_crt_s3_client.h"
#include "common/blockaccess/s3/aws/aws_legacy_s3_client.h"
#include "fmt/format.h"
#include "glog/logging.h"

namespace dingofs {
namespace blockaccess {

static Aws::SDKOptions aws_sdk_options;

bool S3Accesser::Init() {
  const auto& s3_info = options_.s3_info;

  LOG(INFO) << fmt::format(
      "[s3_accesser] init s3 accesser, endpoint({}) bucket({}) ak({}) sk({}).",
      s3_info.endpoint, s3_info.bucket_name, s3_info.ak, options_.s3_info.sk);

  if (s3_info.endpoint.empty() || s3_info.bucket_name.empty() ||
      s3_info.ak.empty() || s3_info.sk.empty()) {
    LOG(ERROR) << "[s3_accesser] param endpoint/bucket/ak/sk is empty.";
    return false;
  }

  // only init once
  auto init_aws_api_fn = [&]() {
    auto& logging_options = aws_sdk_options.loggingOptions;

    logging_options.logLevel =
        Aws::Utils::Logging::LogLevel(options_.aws_sdk_config.loglevel);
    logging_options.defaultLogPrefix =
        options_.aws_sdk_config.log_prefix.c_str();

    // Customize the CRT EventLoopGroup thread count when using the crt client.
    // Must be set before Aws::InitAPI so the SDK uses this bootstrap as the
    // process-wide default for all S3CrtClient instances.
    if (options_.aws_sdk_config.use_crt_client) {
      const int event_loop_threads =
          options_.aws_sdk_config.crt_event_loop_threads;
      LOG(INFO) << fmt::format(
          "[s3_accesser] init aws crt event loop group, threads={} "
          "(0 means all cpus).",
          event_loop_threads);
      aws_sdk_options.ioOptions.clientBootstrap_create_fn =
          [event_loop_threads]() {
            Aws::Crt::Io::EventLoopGroup event_loop_group(
                static_cast<uint16_t>(event_loop_threads));

            Aws::Crt::Io::DefaultHostResolver host_resolver(
                event_loop_group, /*maxHosts=*/8, /*maxTTL=*/30);

            auto bootstrap = Aws::MakeShared<Aws::Crt::Io::ClientBootstrap>(
                "Aws_Init_Cleanup", event_loop_group, host_resolver);
            bootstrap->EnableBlockingShutdown();

            return bootstrap;
          };
    }

    Aws::InitAPI(aws_sdk_options);

    if (std::atexit([]() { Aws::ShutdownAPI(aws_sdk_options); }) != 0) {
      LOG(FATAL) << "[s3_accesser] register shutdown function fail.";
    }
  };

  static std::once_flag aws_init_flag;
  std::call_once(aws_init_flag, init_aws_api_fn);

  bucket_ = options_.s3_info.bucket_name;

  if (options_.aws_sdk_config.use_crt_client)
    client_ = std::make_unique<aws::AwsCrtS3Client>();
  else
    client_ = std::make_unique<aws::AwsLegacyS3Client>();

  client_->Init(options_);

  return true;
}

bool S3Accesser::Destroy() { return true; }

Aws::String S3Accesser::S3Key(const std::string& key) {
  return Aws::String(key.c_str(), key.size());
}

bool S3Accesser::ContainerExist() { return client_->BucketExist(bucket_); }

Status S3Accesser::Put(const std::string& key, const char* buffer,
                       size_t length) {
  int rc = client_->PutObject(bucket_, S3Key(key), buffer, length);
  if (rc < 0) {
    LOG(ERROR) << fmt::format("[s3_accesser] put object({}) fail, retcode:{}.",
                              key, rc);
    return Status::IoError("put object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncPut(const std::string& key,
                          PutObjectAsyncContextSPtr context) {
  CHECK(context->cb) << "AsyncPut context callback is null";

  client_->AsyncPutObject(bucket_, key, context);
}

Status S3Accesser::Get(const std::string& key, std::string* data) {
  int rc = client_->GetObject(bucket_, S3Key(key), data);
  if (rc < 0) {
    if (!client_->ObjectExist(bucket_, S3Key(key))) {  // TODO: more efficient
      LOG(WARNING) << fmt::format("[s3_accesser] object({}) not found.", key);
      return Status::NotFound("object not found");
    }

    LOG(ERROR) << fmt::format("[s3_accesser] get object({}) fail, retcode:{}.",
                              key, rc);
    return Status::IoError("get object fail");
  }

  return Status::OK();
}

Status S3Accesser::Range(const std::string& key, off_t offset, size_t length,
                         char* buffer) {
  int rc = client_->RangeObject(bucket_, S3Key(key), buffer, offset, length);
  if (rc < 0) {
    if (!client_->ObjectExist(bucket_, S3Key(key))) {  // TODO: more efficient
      LOG(WARNING) << fmt::format("[s3_accesser] object({}) not found.", key);
      return Status::NotFound("object not found");
    }

    LOG(ERROR) << fmt::format("[s3_accesser] get object({}) fail, retcode:{}.",
                              key, rc);
    return Status::IoError("get object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncGet(const std::string& key,
                          GetObjectAsyncContextSPtr context) {
  CHECK(context->cb) << "AsyncGet context callback is null";

  auto origin_cb = context->cb;
  context->cb = [this, key,
                 origin_cb](const std::shared_ptr<GetObjectAsyncContext>& ctx) {
    if (!ctx->status.ok()) {
      if (!client_->ObjectExist(bucket_, S3Key(key))) {
        LOG(WARNING) << fmt::format(
            "[s3_accesser] object({}) not found in async get", key);
        ctx->status = Status::NotFound("object not found");
      }
    }
    origin_cb(ctx);
  };

  client_->AsyncGetObject(bucket_, key, context);
}

bool S3Accesser::BlockExist(const std::string& key) {
  return client_->ObjectExist(bucket_, key);
}

Status S3Accesser::Delete(const std::string& key) {
  int rc = client_->DeleteObject(bucket_, S3Key(key));
  if (rc < 0) {
    LOG(ERROR) << fmt::format(
        "[s3_accesser] delete object({}) fail, retcode:{}.", key, rc);
    return Status::IoError("delete object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncDelete(const std::string& key,
                             DeleteObjectAsyncContextSPtr context) {
  CHECK(context->cb) << "AsyncDelete context callback is null";

  client_->AsyncDeleteObject(bucket_, key, context);
}

Status S3Accesser::BatchDelete(const std::list<std::string>& keys) {
  // Empty batch: an empty DeleteObjects request is rejected by S3
  // (MalformedXML). Short-circuit so the public sync BatchDelete has the same
  // empty-keys semantics as AsyncBatchDelete (and rados/file backends).
  if (keys.empty()) {
    return Status::OK();
  }

  int rc = client_->DeleteObjects(bucket_, keys);
  if (rc < 0) {
    LOG(ERROR) << fmt::format(
        "[s3_accesser] batch delete object fail, count:{} retcode:{}.",
        keys.size(), rc);
    return Status::IoError("batch delete object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncBatchDelete(const std::list<std::string>& keys,
                                  BatchDeleteObjectAsyncContextSPtr context) {
  CHECK(context->cb) << "AsyncBatchDelete context callback is null";

  // Empty batch: an empty DeleteObjects request is rejected by S3
  // (MalformedXML), so short-circuit (matches rados/file behavior).
  if (keys.empty()) {
    context->status = Status::OK();
    context->cb(context);
    return;
  }

  // S3 DeleteObjects supports at most 1000 keys per request. GC pre-batches by
  // 1000, but guard here so other callers can't bypass it.
  if (keys.size() > 1000) {
    context->status =
        Status::InvalidParam("S3 DeleteObjects supports at most 1000 keys");
    context->cb(context);
    return;
  }

  client_->AsyncDeleteObjects(bucket_, keys, context);
}

}  // namespace blockaccess
}  // namespace dingofs
