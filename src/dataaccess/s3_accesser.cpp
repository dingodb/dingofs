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

#include "dataaccess/s3_accesser.h"

#include <memory>
#include <ostream>

#include "stub/metric/metric.h"

namespace dingofs {
namespace dataaccess {

using stub::metric::MetricGuard;
using stub::metric::S3Metric;

bool S3Accesser::Init() {
  client_ = std::make_unique<dataaccess::aws::S3Adapter>();
  client_->Init(option_);

  return true;
}

bool S3Accesser::Destroy() {
  client_->Deinit();

  return true;
}

Status S3Accesser::Put(const std::string& key, const char* buffer,
                       size_t length) {
  int rc;
  // write s3 metrics
  auto start = butil::cpuwide_time_us();
  MetricGuard guard(&rc, &S3Metric::GetInstance().write_s3, length, start);

  rc = client_->PutObject(S3Key(key), buffer, length);
  if (rc < 0) {
    LOG(ERROR) << "Put object(" << key << ") failed, retCode=" << rc;
    return Status::IoError("Put object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncPut(const std::string& key, const char* buffer,
                          size_t length, RetryCallback retry_cb) {
  auto aws_ctx = std::make_shared<aws::PutObjectAsyncContext>();
  aws_ctx->key = key;
  aws_ctx->buffer = buffer;
  aws_ctx->bufferSize = length;
  aws_ctx->startTime = butil::cpuwide_time_us();
  aws_ctx->cb =
      [&, retry_cb](const std::shared_ptr<aws::PutObjectAsyncContext>& ctx) {
        MetricGuard guard(&ctx->retCode, &S3Metric::GetInstance().write_s3,
                          ctx->bufferSize, ctx->startTime);

        if (retry_cb(ctx->retCode)) {
          client_->PutObjectAsync(ctx);
        }
      };

  client_->PutObjectAsync(aws_ctx);
}

void S3Accesser::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  auto aws_ctx = std::make_shared<aws::PutObjectAsyncContext>();
  aws_ctx->key = context->key;
  aws_ctx->buffer = context->buffer;
  aws_ctx->bufferSize = context->bufferSize;
  aws_ctx->startTime = context->startTime;
  aws_ctx->retCode = context->retCode;
  aws_ctx->cb =
      [context](const std::shared_ptr<aws::PutObjectAsyncContext>& aws_ctx) {
        context->buffer = aws_ctx->buffer;
        context->bufferSize = aws_ctx->bufferSize;
        context->retCode = aws_ctx->retCode;
        context->startTime = aws_ctx->startTime;

        context->cb(context);
      };

  client_->PutObjectAsync(aws_ctx);
}

Status S3Accesser::Get(const std::string& key, off_t offset, size_t length,
                       char* buffer) {
  int rc;
  // read s3 metrics
  auto start = butil::cpuwide_time_us();
  MetricGuard guard(&rc, &S3Metric::GetInstance().read_s3, length, start);

  rc = client_->GetObject(S3Key(key), buffer, offset, length);
  if (rc < 0) {
    if (!client_->ObjectExist(S3Key(key))) {  // TODO: more efficient
      LOG(WARNING) << "Object(" << key << ") not found.";
      return Status::NotFound("object not found");
    }

    LOG(ERROR) << "Get object(" << key << ") failed, retCode=" << rc;
    return Status::IoError("get object fail");
  }

  return Status::OK();
}

void S3Accesser::AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) {
  auto aws_ctx = std::make_shared<aws::GetObjectAsyncContext>();
  aws_ctx->key = context->key;
  aws_ctx->buf = context->buf;
  aws_ctx->offset = context->offset;
  aws_ctx->len = context->len;
  aws_ctx->retCode = context->retCode;
  aws_ctx->retry = context->retry;
  aws_ctx->actualLen = context->actualLen;

  // aws_ctx->cb =
  //     [context](const std::shared_ptr<aws::GetObjectAsyncContext>& aws_ctx) {
  //       context->cb(context);
  //     };

  client_->GetObjectAsync(aws_ctx);
}

}  // namespace dataaccess
}  // namespace dingofs
