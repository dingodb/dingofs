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

#include "blockaccess/s3/s3_accesser.h"

#include <glog/logging.h>

#include <memory>

#include "fmt/format.h"
#include "stub/metric/metric.h"

namespace dingofs {
namespace blockaccess {

using stub::metric::MetricGuard;
using stub::metric::S3Metric;

bool S3Accesser::Init() {
  const auto& s3_info = options_.s3_info;
  LOG(INFO) << fmt::format(
      "[accesser] init s3 accesser, endpoint({}) bucket({}) ak({}) sk({}).",
      s3_info.endpoint, s3_info.bucket_name, s3_info.ak, options_.s3_info.sk);

  if (s3_info.endpoint.empty() || s3_info.bucket_name.empty() ||
      s3_info.ak.empty() || s3_info.sk.empty()) {
    LOG(ERROR) << "[accesser] param endpoint/bucket/ak/sk is empty.";
    return false;
  }

  client_ = std::make_unique<blockaccess::aws::S3Adapter>();
  client_->Init(options_);

  return true;
}

bool S3Accesser::Destroy() { return true; }

Aws::String S3Accesser::S3Key(const std::string& key) {
  return Aws::String(key.c_str(), key.size());
}

bool S3Accesser::ContainerExist() { return client_->BucketExist(); }

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

void S3Accesser::AsyncPut(std::shared_ptr<PutObjectAsyncContext> context) {
  auto origin_cb = context->cb;
  auto start_time = butil::cpuwide_time_us();
  context->cb = [&, start_time,
                 origin_cb](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
    MetricGuard guard(&ctx->ret_code, &S3Metric::GetInstance().write_s3,
                      ctx->buffer_size, start_time);
    ctx->cb = origin_cb;
    ctx->cb(ctx);
  };
  client_->PutObjectAsync(context);
}

Status S3Accesser::Get(const std::string& key, std::string* data) {
  int rc;  // read s3 metrics
  auto start = butil::cpuwide_time_us();
  MetricGuard guard(&rc, &S3Metric::GetInstance().read_s3, data->length(),
                    start);

  rc = client_->GetObject(S3Key(key), data);
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

Status S3Accesser::Range(const std::string& key, off_t offset, size_t length,
                         char* buffer) {
  int rc;  // read s3 metrics
  auto start = butil::cpuwide_time_us();
  MetricGuard guard(&rc, &S3Metric::GetInstance().read_s3, length, start);

  rc = client_->RangeObject(S3Key(key), buffer, offset, length);
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
  auto origin_cb = context->cb;
  auto start_time = butil::cpuwide_time_us();
  context->cb = [&, start_time,
                 origin_cb](const std::shared_ptr<GetObjectAsyncContext>& ctx) {
    MetricGuard guard(&ctx->ret_code, &S3Metric::GetInstance().read_s3,
                      ctx->len, start_time);
    ctx->cb = origin_cb;
    ctx->cb(ctx);
  };
  client_->GetObjectAsync(context);
}

bool S3Accesser::BlockExist(const std::string& key) {
  return client_->ObjectExist(key);
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

Status S3Accesser::BatchDelete(const std::list<std::string>& keys) {
  int rc = client_->BatchDeleteObject(keys);
  if (rc < 0) {
    LOG(ERROR) << fmt::format(
        "[accesser] batch delete object fail, count:{}, ret: {}.", keys.size(),
        rc);
    return Status::IoError("batch delete object fail");
  }

  return Status::OK();
}

}  // namespace blockaccess
}  // namespace dingofs
