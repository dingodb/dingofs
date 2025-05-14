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

#include "dataaccess/s3/s3_accesser.h"

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

bool S3Accesser::Destroy() { return true; }

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
  auto put_obj_ctx = std::make_shared<PutObjectAsyncContext>();
  put_obj_ctx->key = key;
  put_obj_ctx->buffer = buffer;
  put_obj_ctx->buffer_size = length;
  put_obj_ctx->start_time = butil::cpuwide_time_us();
  put_obj_ctx->cb =
      [&, retry_cb](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
        MetricGuard guard(&ctx->ret_code, &S3Metric::GetInstance().write_s3,
                          ctx->buffer_size, ctx->start_time);

        if (retry_cb(ctx->ret_code)) {
          client_->PutObjectAsync(ctx);
        }
      };

  client_->PutObjectAsync(put_obj_ctx);
}

void S3Accesser::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  client_->PutObjectAsync(context);
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

void S3Accesser::AsyncGet(const std::string& key, off_t offset, size_t length,
                          char* buffer, RetryCallback retry_cb) {
  auto get_obj_ctx = std::make_shared<GetObjectAsyncContext>();
  get_obj_ctx->key = key;
  get_obj_ctx->buf = buffer;
  get_obj_ctx->offset = offset;
  get_obj_ctx->len = length;
  get_obj_ctx->cb =
      [&, retry_cb](const std::shared_ptr<GetObjectAsyncContext>& ctx) {
        if (retry_cb(ctx->ret_code)) {
          client_->GetObjectAsync(ctx);
        }
      };

  client_->GetObjectAsync(get_obj_ctx);
}

void S3Accesser::AsyncGet(std::shared_ptr<GetObjectAsyncContext> context) {
  client_->GetObjectAsync(context);
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
