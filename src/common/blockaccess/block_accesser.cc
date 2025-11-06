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

#include "common/blockaccess/block_accesser.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_format.h>

#include <memory>
#include <utility>

#include "common/blockaccess/block_access_log.h"
#include "common/blockaccess/rados/rados_accesser.h"
#include "common/blockaccess/s3/s3_accesser.h"
#include "common/metrics/blockaccess/block_accesser.h"
#include "common/metrics/metric_guard.h"
#include "common/status.h"
#include "utils/dingo_define.h"

namespace dingofs {
namespace blockaccess {

using metrics::MetricGuard;
using metrics::blockaccess::BlockMetric;

static auto& g_block_metric = BlockMetric::GetInstance();

static bvar::Adder<uint64_t> block_put_async_num("block_put_async_num");
static bvar::Adder<uint64_t> block_put_sync_num("block_put_sync_num");
static bvar::Adder<uint64_t> block_get_async_num("block_get_async_num");
static bvar::Adder<uint64_t> block_get_sync_num("block_get_sync_num");

using dingofs::utils::kMB;

static const char* PrettyBool(bool b) { return b ? "true" : "false"; }

Status BlockAccesserImpl::Init() {
  if (options_.type == AccesserType::kS3) {
    data_accesser_ = std::make_unique<S3Accesser>(options_.s3_options);
    container_name_ = options_.s3_options.s3_info.bucket_name;

  } else if (options_.type == AccesserType::kRados) {
    data_accesser_ = std::make_unique<RadosAccesser>(options_.rados_options);
    container_name_ = options_.rados_options.pool_name;

  } else {
    return Status::InvalidParam("unsupported accesser type");
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
    return fmt::format("container_exist ({}) : {}", container_name_,
                       PrettyBool(ok));
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
    return fmt::format("put_block ({}, {}) : {}", key, length,
                       PrettyBool(s.ok()));
  });
  // write block metrics
  MetricGuard<Status> guard(&s, &g_block_metric.write_block, length,
                            butil::cpuwide_time_us());

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

  auto origin_cb = std::move(context->cb);

  context->cb = [this, start_us, origin = std::move(origin_cb)](
                    const std::shared_ptr<PutObjectAsyncContext>& ctx) {
    BlockAccessLogGuard log(start_us, [&]() {
      return fmt::format("async_put_block ({}, {}) : {}", ctx->key,
                         ctx->buffer_size, ctx->status.ToString());
    });
    MetricGuard<Status> guard(&ctx->status, &g_block_metric.write_block,
                              ctx->buffer_size, start_us);

    block_put_async_num << -1;
    inflight_bytes_throttle_->OnComplete(ctx->buffer_size);

    // NOTE: this is necessary because caller reuse context when retry
    ctx->cb = origin;
    ctx->cb(ctx);
  };

  if (throttle_) {
    throttle_->Add(false, context->buffer_size);
  }

  inflight_bytes_throttle_->OnStart(context->buffer_size);

  data_accesser_->AsyncPut(context);
}

Status BlockAccesserImpl::Get(const std::string& key, std::string* data) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return fmt::format("get_block ({}) : {}", key, PrettyBool(s.ok()));
  });
  // read block metrics
  MetricGuard<Status> guard(&s, &g_block_metric.read_block, data->length(),
                            butil::cpuwide_time_us());

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

  auto origin_cb = std::move(context->cb);

  context->cb = [this, start_us, origin = std::move(origin_cb)](
                    const std::shared_ptr<GetObjectAsyncContext>& ctx) {
    BlockAccessLogGuard log(start_us, [&]() {
      return fmt::format("async_get_block ({}, {}, {}) : {}", ctx->key,
                         ctx->offset, ctx->len, ctx->status.ToString());
    });
    MetricGuard<Status> guard(&ctx->status, &g_block_metric.read_block,
                              ctx->len, start_us);

    block_get_async_num << -1;
    inflight_bytes_throttle_->OnComplete(ctx->len);

    // NOTE: this is necessary because caller reuse context when retry
    ctx->cb = origin;
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
    return fmt::format("range_block ({}, {}, {}) : {}", key, offset, length,
                       PrettyBool(s.ok()));
  });
  // read s3 metrics
  MetricGuard<Status> guard(&s, &g_block_metric.read_block, length,
                            butil::cpuwide_time_us());

  block_get_sync_num << 1;
  auto dec = ::absl::MakeCleanup([&]() { block_get_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(true, length);
  }

  s = data_accesser_->Range(key, offset, length, buffer);
  return s;
}

bool BlockAccesserImpl::BlockExist(const std::string& key) {
  bool ok = false;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return fmt::format("block_exist ({}) : {}", key, PrettyBool(ok));
  });

  return (ok = data_accesser_->BlockExist(key));
}

Status BlockAccesserImpl::Delete(const std::string& key) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return fmt::format("delete_block ({}) : {}", key, PrettyBool(s.ok()));
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
    return fmt::format("delete_objects ({}) : {}", keys.size(),
                       PrettyBool(s.ok()));
  });

  return (s = data_accesser_->BatchDelete(keys));
}

}  // namespace blockaccess
}  // namespace dingofs
