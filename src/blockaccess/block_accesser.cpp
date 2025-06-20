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

#include "blockaccess/block_accesser.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>

#include <memory>

#include "blockaccess/block_access_log.h"
#include "blockaccess/rados/rados_accesser.h"
#include "blockaccess/s3/s3_accesser.h"
#include "common/status.h"
#include "utils/dingo_define.h"

namespace dingofs {
namespace blockaccess {

static bvar::Adder<uint64_t> block_put_async_num("block_put_async_num");
static bvar::Adder<uint64_t> block_put_sync_num("block_put_sync_num");
static bvar::Adder<uint64_t> block_get_async_num("block_get_async_num");
static bvar::Adder<uint64_t> block_get_sync_num("block_get_sync_num");

using dingofs::utils::kMB;
using stub::metric::ObjectMetric;

Status BlockAccesserImpl::Init() {
  if (options_.type == AccesserType::kS3) {
    data_accesser_ = std::make_unique<S3Accesser>(options_.s3_options);
    container_name_ = options_.s3_options.s3_info.bucket_name;

  } else if (options_.type == AccesserType::kRados) {
    data_accesser_ = std::make_unique<RadosAccesser>(options_.rados_options);
    container_name_ = options_.rados_options.pool_name;

  } else {
    return Status::InvalidParam("Unsupported accesser type");
  }

  if (!data_accesser_->Init()) {
    return Status::Internal("init data accesser fail");
  }

  {
    utils::ReadWriteThrottleParams params;
    params.iopsTotal.limit = options_.throttle_options.iopsTotalLimit;
    params.iopsRead.limit = options_.throttle_options.iopsReadLimit;
    params.iopsWrite.limit = options_.throttle_options.iopsWriteLimit;
    params.bpsTotal.limit = options_.throttle_options.bpsTotalMB * kMB;
    params.bpsRead.limit = options_.throttle_options.bpsReadMB * kMB;
    params.bpsWrite.limit = options_.throttle_options.bpsWriteMB * kMB;

    throttle_ = std::make_unique<utils::Throttle>();
    throttle_->UpdateThrottleParams(params);

    inflight_bytes_throttle_ =
        std::make_unique<AsyncRequestInflightBytesThrottle>(
            options_.throttle_options.maxAsyncRequestInflightBytes == 0
                ? UINT64_MAX
                : options_.throttle_options.maxAsyncRequestInflightBytes);
  }

  return Status::OK();
}

Status BlockAccesserImpl::Destroy() {
  if (data_accesser_ != nullptr) {
    data_accesser_->Destroy();
    data_accesser_.reset(nullptr);
  }
  return Status::OK();
}

bool BlockAccesserImpl::ContainerExist() {
  bool ok = false;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("container_exist (%s) : %s", container_name_,
                           (ok ? "ok" : "fail"));
  });
  ok = data_accesser_->ContainerExist();
  return ok;
}

Status BlockAccesserImpl::Put(const std::string& key, const std::string& data) {
  return Put(key, data.data(), data.size());
}

Status BlockAccesserImpl::Put(const std::string& key, const char* buffer,
                              size_t length) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("put_block (%s, %d) : %s", key, length,
                           (s.ok() ? "ok" : "fail"));
  });

  // object storage write metrics
  BlockAccessMetricGuard metric_guard(&s,
                                      &ObjectMetric::GetInstance().write_object,
                                      length, butil::cpuwide_time_us());

  block_put_sync_num << 1;

  auto dec = ::absl::MakeCleanup([&]() { block_put_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(false, length);
  }

  s = data_accesser_->Put(key, buffer, length);
  return s;
}

void BlockAccesserImpl::AsyncPut(
    std::shared_ptr<PutObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_put_async_num << 1;

  auto origin_cb = context->cb;

  context->cb = [this, start_us,
                 origin_cb](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
    BlockAccessLogGuard log(start_us, [&]() {
      return absl::StrFormat("async_put_block (%s, %d) : %d", ctx->key,
                             ctx->buffer_size, ctx->ret_code);
    });
    // object storage write metrics
    Status s = (ctx->ret_code == 0 ? Status::OK() : Status::Unknown(""));
    BlockAccessMetricGuard metric_guard(
        &s, &ObjectMetric::GetInstance().write_object, ctx->buffer_size,
        start_us);

    block_put_async_num << -1;
    inflight_bytes_throttle_->OnComplete(ctx->buffer_size);

    // for return
    ctx->cb = origin_cb;
    ctx->cb(ctx);
  };

  if (throttle_) {
    throttle_->Add(false, context->buffer_size);
  }

  inflight_bytes_throttle_->OnStart(context->buffer_size);

  data_accesser_->AsyncPut(context);
}

void BlockAccesserImpl::AsyncPut(const std::string& key, const char* buffer,
                                 size_t length, RetryCallback retry_cb) {
  auto put_obj_ctx = std::make_shared<PutObjectAsyncContext>();
  put_obj_ctx->key = key;
  put_obj_ctx->buffer = buffer;
  put_obj_ctx->buffer_size = length;
  put_obj_ctx->start_time = butil::cpuwide_time_us();
  put_obj_ctx->cb =
      [&, retry_cb](const std::shared_ptr<PutObjectAsyncContext>& ctx) {
        if (retry_cb(ctx->ret_code) == RetryStrategy::kRetry) {
          AsyncPut(ctx);
        }
      };

  AsyncPut(put_obj_ctx);
}

Status BlockAccesserImpl::Get(const std::string& key, std::string* data) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("get_block (%s) : %s", key,
                           (s.ok() ? "ok" : "fail"));
  });

  // object storage read metrics
  BlockAccessMetricGuard metric_guard(&s,
                                      &ObjectMetric::GetInstance().read_object,
                                      data->length(), butil::cpuwide_time_us());

  block_get_sync_num << 1;
  auto dec = ::absl::MakeCleanup([&]() { block_get_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(true, 1);
  }

  s = data_accesser_->Get(key, data);
  return s;
}

void BlockAccesserImpl::AsyncGet(
    std::shared_ptr<GetObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_get_async_num << 1;

  auto origin_cb = context->cb;

  context->cb = [this, start_us,
                 origin_cb](const std::shared_ptr<GetObjectAsyncContext>& ctx) {
    BlockAccessLogGuard log(start_us, [&]() {
      return absl::StrFormat("async_get_block (%s, %d, %d) : %d", ctx->key,
                             ctx->offset, ctx->len, ctx->ret_code);
    });

    // object storage read metrics
    Status s = (ctx->ret_code == 0 ? Status::OK() : Status::Unknown(""));
    BlockAccessMetricGuard metric_guard(
        &s, &ObjectMetric::GetInstance().read_object, ctx->len, start_us);

    block_get_async_num << -1;
    inflight_bytes_throttle_->OnComplete(ctx->len);

    // for return
    ctx->cb = origin_cb;
    ctx->cb(ctx);
  };

  if (throttle_) {
    throttle_->Add(true, context->len);
  }

  inflight_bytes_throttle_->OnStart(context->len);

  data_accesser_->AsyncGet(context);
}

Status BlockAccesserImpl::Range(const std::string& key, off_t offset,
                                size_t length, char* buffer) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("range_block (%s, %d, %d) : %s", key, offset, length,
                           (s.ok() ? "ok" : "fail"));
  });

  // object storage read metrics
  BlockAccessMetricGuard metric_guard(&s,
                                      &ObjectMetric::GetInstance().read_object,
                                      length, butil::cpuwide_time_us());

  block_get_sync_num << 1;
  auto dec = ::absl::MakeCleanup([&]() { block_get_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(true, length);
  }

  s = data_accesser_->Range(key, offset, length, buffer);
  return s;
}

void BlockAccesserImpl::AsyncRange(const std::string& key, off_t offset,
                                   size_t length, char* buffer,
                                   RetryCallback retry_cb) {
  auto get_obj_ctx = std::make_shared<GetObjectAsyncContext>();
  get_obj_ctx->key = key;
  get_obj_ctx->buf = buffer;
  get_obj_ctx->offset = offset;
  get_obj_ctx->len = length;
  get_obj_ctx->cb =
      [&, retry_cb](const std::shared_ptr<GetObjectAsyncContext>& ctx) {
        if (retry_cb(ctx->ret_code) == RetryStrategy::kRetry) {
          AsyncGet(ctx);
        }
      };

  AsyncGet(get_obj_ctx);
}

bool BlockAccesserImpl::BlockExist(const std::string& key) {
  bool ok = false;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("block_exist (%s) : %s", key, (ok ? "ok" : "fail"));
  });

  return (ok = data_accesser_->BlockExist(key));
}

Status BlockAccesserImpl::Delete(const std::string& key) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("delete_block (%s) : %s", key,
                           (s.ok() ? "ok" : "fail"));
  });

  s = data_accesser_->Delete(key);
  if (s.ok() || !BlockExist(key)) {
    s = Status::OK();
  }

  return s;
}

Status BlockAccesserImpl::BatchDelete(const std::list<std::string>& keys) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return absl::StrFormat("delete_objects (%d) : %s", keys.size(),
                           (s.ok() ? "ok" : "fail"));
  });

  return (s = data_accesser_->BatchDelete(keys));
}

}  // namespace blockaccess
}  // namespace dingofs
