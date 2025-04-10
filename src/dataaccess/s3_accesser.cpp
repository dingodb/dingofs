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

#include "fmt/format.h"
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

Aws::String S3Accesser::S3Key(const std::string& key) {
  return Aws::String(key.c_str(), key.size());
}

Status S3Accesser::Put(const std::string& key, const char* buffer,
                       size_t length) {
  int rc = 0;
  // write s3 metrics
  auto start = butil::cpuwide_time_us();
  MetricGuard guard(&rc, &S3Metric::GetInstance().write_s3, length, start);

  rc = client_->PutObject(S3Key(key), buffer, length);
  if (rc < 0) {
    LOG(ERROR) << fmt::format("[accesser] put object({}) fail, retcode: {}.",
                              key, rc);
    return Status::IoError("put object fail");
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
  aws_ctx->bufferSize = context->buffer_size;
  aws_ctx->startTime = context->start_time;
  aws_ctx->retCode = context->ret_code;
  aws_ctx->cb =
      [context](const std::shared_ptr<aws::PutObjectAsyncContext>& aws_ctx) {
        context->buffer = aws_ctx->buffer;
        context->buffer_size = aws_ctx->bufferSize;
        context->ret_code = aws_ctx->retCode;
        context->start_time = aws_ctx->startTime;

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
      LOG(WARNING) << fmt::format("[accesser] object({}) not found.", key);
      return Status::NotFound("object not found");
    }

    LOG(ERROR) << fmt::format("[accesser] get object({}) fail, ret_code: {}.",
                              key, rc);
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
  aws_ctx->retCode = context->ret_code;
  aws_ctx->retry = context->retry;
  aws_ctx->actualLen = context->actual_len;

  aws_ctx->cb =
      [context](const aws::S3Adapter*,
                const std::shared_ptr<aws::GetObjectAsyncContext>& aws_ctx) {
        context->buf = aws_ctx->buf;
        context->offset = aws_ctx->offset;
        context->len = aws_ctx->len;
        context->ret_code = aws_ctx->retCode;
        context->retry = aws_ctx->retry;
        context->actual_len = aws_ctx->actualLen;

        context->cb(context);
      };

  client_->GetObjectAsync(aws_ctx);
}

Status S3Accesser::Delete(const std::string& key) {
  int rc = client_->DeleteObject(S3Key(key));
  if (rc < 0) {
    LOG(ERROR) << fmt::format(
        "[accesser] delete object({}) fail, ret_code: {}.", key, rc);
    return Status::IoError("delete object fail");
  }

  return Status::OK();
}

}  // namespace dataaccess
}  // namespace dingofs
