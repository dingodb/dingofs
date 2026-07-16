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

#include <absl/strings/str_format.h>

#include <memory>
#include <utility>

#include "common/blockaccess/block_access_log.h"
#include "common/blockaccess/fake/fake_accesser.h"
#include "common/blockaccess/files/file_accesser.h"
#include "common/blockaccess/rados/rados_accesser.h"
#include "common/blockaccess/s3/s3_accesser.h"
#include "common/metrics/blockaccess/block_accesser.h"
#include "common/metrics/metric_guard.h"
#include "common/status.h"
#include "utils/dingo_define.h"
#include "utils/scoped_cleanup.h"

namespace dingofs {
namespace blockaccess {

using metrics::MetricGuard;
using metrics::blockaccess::BlockMetric;

static auto& g_block_metric = BlockMetric::GetInstance();

static bvar::Adder<uint64_t> block_put_async_num("block_put_async_num");
static bvar::Adder<uint64_t> block_put_sync_num("block_put_sync_num");
static bvar::Adder<uint64_t> block_get_async_num("block_get_async_num");
static bvar::Adder<uint64_t> block_get_sync_num("block_get_sync_num");
static bvar::Adder<uint64_t> block_delete_async_num("block_delete_async_num");
static bvar::Adder<uint64_t> block_batch_delete_async_num(
    "block_batch_delete_async_num");

using dingofs::utils::kMB;

static const char* PrettyBool(bool b) { return b ? "true" : "false"; }

Status BlockAccesserImpl::Init() {
  if (FLAGS_use_fake_block_access) {
    data_accesser_ = std::make_unique<FakeAccesser>();
    container_name_ = "fake_block";
    LOG(WARNING) << "use fake block accesser";
  } else {
    if (options_.type == AccesserType::kS3) {
      data_accesser_ = std::make_unique<S3Accesser>(options_.s3_options);
      container_name_ = options_.s3_options.s3_info.bucket_name;
    } else if (options_.type == AccesserType::kRados) {
      data_accesser_ = std::make_unique<RadosAccesser>(options_.rados_options);
      container_name_ = options_.rados_options.pool_name;
    } else if (options_.type == AccesserType::kLocalFile) {
      data_accesser_ =
          std::make_unique<FileAccesser>(options_.file_options.path);
      container_name_ = options_.file_options.path;
    } else {
      return Status::InvalidParam("unsupported accesser type");
    }
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

Status BlockAccesserImpl::Put(const std::string& key,
                              const PutPayload& payload) {
  const size_t length = payload.Size();
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return fmt::format("put_block ({}, {}) : {}", key, length,
                       PrettyBool(s.ok()));
  });
  // write block metrics
  MetricGuard<Status> guard(&s, &g_block_metric.write_block, length,
                            butil::cpuwide_time_us());

  block_put_sync_num << 1;

  auto dec = MakeScopedCleanup([&]() { block_put_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(false, length);
  }

  s = data_accesser_->Put(key, payload);
  return s;
}

void BlockAccesserImpl::AsyncPut(
    const std::string& key, std::shared_ptr<PutObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_put_async_num << 1;

  auto origin_cb = std::move(context->cb);

  // Capture key for the log guard so logging shows the key actually
  // requested by this call (post-wrapper transforms), independent of any
  // ctx->origin_key value the caller may have set.
  context->cb = [this, start_us, key, origin = std::move(origin_cb)](
                    const std::shared_ptr<PutObjectAsyncContext>& ctx) {
    // Snapshot this attempt before invoking the caller. The caller may enqueue
    // a retry immediately, and that retry reuses and rewrites the same context.
    Status attempt_status = ctx->status;
    const size_t payload_size = ctx->payload.Size();
    {
      BlockAccessLogGuard log(start_us, [key, payload_size, attempt_status]() {
        return fmt::format("async_put_block ({}, {}) : {}", key, payload_size,
                           attempt_status.ToString());
      });
      MetricGuard<Status> guard(&attempt_status, &g_block_metric.write_block,
                                payload_size, start_us);

      block_put_async_num << -1;
      inflight_bytes_throttle_->OnComplete(payload_size);
    }

    // NOTE: this is necessary because caller reuse context when retry
    auto callback = origin;
    ctx->cb = callback;
    callback(ctx);
  };

  if (throttle_) {
    throttle_->Add(false, context->payload.Size());
  }

  inflight_bytes_throttle_->OnStart(context->payload.Size());

  data_accesser_->AsyncPut(key, context);
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
  auto dec = MakeScopedCleanup([&]() { block_get_sync_num << -1; });

  if (throttle_) {
    throttle_->Add(true, 1);
  }

  s = data_accesser_->Get(key, data);
  return s;
}

void BlockAccesserImpl::AsyncGet(
    const std::string& key, std::shared_ptr<GetObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_get_async_num << 1;

  auto origin_cb = std::move(context->cb);

  context->cb = [this, start_us, key, origin = std::move(origin_cb)](
                    const std::shared_ptr<GetObjectAsyncContext>& ctx) {
    Status attempt_status = ctx->status;
    const off_t offset = ctx->offset;
    const size_t length = ctx->len;
    {
      BlockAccessLogGuard log(
          start_us, [key, offset, length, attempt_status]() {
            return fmt::format("async_get_block ({}, {}, {}) : {}", key, offset,
                               length, attempt_status.ToString());
          });
      MetricGuard<Status> guard(&attempt_status, &g_block_metric.read_block,
                                length, start_us);

      block_get_async_num << -1;
      inflight_bytes_throttle_->OnComplete(length);
    }

    // NOTE: this is necessary because caller reuse context when retry
    auto callback = origin;
    ctx->cb = callback;
    callback(ctx);
  };

  if (throttle_) {
    throttle_->Add(true, context->len);
  }

  inflight_bytes_throttle_->OnStart(context->len);

  data_accesser_->AsyncGet(key, context);
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
  auto dec = MakeScopedCleanup([&]() { block_get_sync_num << -1; });

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

void BlockAccesserImpl::AsyncDelete(
    const std::string& key, std::shared_ptr<DeleteObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_delete_async_num << 1;

  auto origin_cb = std::move(context->cb);
  context->cb = [start_us, key, origin = std::move(origin_cb)](
                    const std::shared_ptr<DeleteObjectAsyncContext>& ctx) {
    Status attempt_status = ctx->status;
    {
      BlockAccessLogGuard log(start_us, [key, attempt_status]() {
        return fmt::format("async_delete_block ({}) : {}", key,
                           attempt_status.ToString());
      });

      block_delete_async_num << -1;
    }

    // NOTE: restore origin cb because caller may reuse context on retry.
    auto callback = origin;
    ctx->cb = callback;
    callback(ctx);
  };

  data_accesser_->AsyncDelete(key, context);
}

Status BlockAccesserImpl::BatchDelete(const std::list<std::string>& keys) {
  Status s;
  BlockAccessLogGuard log(butil::cpuwide_time_us(), [&]() {
    return fmt::format("delete_objects ({}) : {}", keys.size(),
                       PrettyBool(s.ok()));
  });

  return (s = data_accesser_->BatchDelete(keys));
}

void BlockAccesserImpl::AsyncBatchDelete(
    const std::list<std::string>& keys,
    std::shared_ptr<BatchDeleteObjectAsyncContext> context) {
  int64_t start_us = butil::cpuwide_time_us();
  block_batch_delete_async_num << 1;
  // Capture only the count (not the list) into the log closure.
  size_t count = keys.size();

  auto origin_cb = std::move(context->cb);
  context->cb = [start_us, count, origin = std::move(origin_cb)](
                    const std::shared_ptr<BatchDeleteObjectAsyncContext>& ctx) {
    Status attempt_status = ctx->status;
    {
      BlockAccessLogGuard log(start_us, [count, attempt_status]() {
        return fmt::format("async_delete_objects ({}) : {}", count,
                           attempt_status.ToString());
      });

      block_batch_delete_async_num << -1;
    }

    // NOTE: restore origin cb because caller may reuse context on retry.
    auto callback = origin;
    ctx->cb = callback;
    callback(ctx);
  };

  data_accesser_->AsyncBatchDelete(keys, context);
}

}  // namespace blockaccess
}  // namespace dingofs
