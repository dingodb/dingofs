/*
 * Copyright (c) 2024 dingodb.com, Inc. All Rights Reserved
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

/*
 * Project: DingoFS
 * Created Date: 2024-09-26
 * Author: Jingli Chen (Wine93)
 */

#include "cache/blockcache/block_cache_throttle.h"

#include <brpc/reloadable_flags.h>

#include "cache/common/const.h"
#include "cache/common/macro.h"
#include "utils/executor/bthread/bthread_executor.h"

namespace dingofs {
namespace cache {

DEFINE_bool(
    upload_stage_throttle_enable, false,
    "Whether to enable throttling for uploading stage blocks to storage.");
DEFINE_validator(upload_stage_throttle_enable, brpc::PassValidate);

DEFINE_uint64(
    upload_stage_throttle_bandwidth_mb, 256,
    "Maximum bandwidth for uploading stage blocks to storage in MB/s");
DEFINE_validator(upload_stage_throttle_bandwidth_mb, brpc::PassValidate);

DEFINE_uint64(upload_stage_throttle_iops, 100,
              "Maximum IOPS for uploading stage blocks to storage");
DEFINE_validator(upload_stage_throttle_iops, brpc::PassValidate);

UploadStageThrottle::UploadStageThrottle()
    : running_(false),
      current_throttle_bandwidth_mb_(0),
      current_throttle_iops_(0),
      throttle_(std::make_unique<dingofs::utils::Throttle>()),
      executor_(std::make_unique<BthreadExecutor>()) {
  UpdateThrottleParam();
}

void UploadStageThrottle::Start() {
  CHECK_NOTNULL(throttle_);
  CHECK_NOTNULL(executor_);

  if (running_) {
    return;
  }

  LOG(INFO) << "Upload stage throttle is starting...";

  CHECK(executor_->Start());
  executor_->Schedule([this] { UpdateThrottleParam(); }, 100);

  running_ = true;

  LOG(INFO) << "Upload stage throttle is up.";

  CHECK_RUNNING("Upload stage throttle");
}

void UploadStageThrottle::Shutdown() {
  if (!running_.exchange(false)) {
    return;
  }

  LOG(INFO) << "Upload stage throttle is shutting down...";

  executor_->Stop();

  LOG(INFO) << "Upload stage throttle is down.";

  CHECK_DOWN("Upload stage throttle");
}

void UploadStageThrottle::Add(uint64_t upload_bytes) {
  DCHECK_RUNNING("Upload stage throttle");

  if (FLAGS_upload_stage_throttle_enable) {
    std::lock_guard<BthreadMutex> lk(mutex_);
    throttle_->Add(false, upload_bytes);
  }
}

void UploadStageThrottle::UpdateThrottleParam() {
  if (current_throttle_bandwidth_mb_ !=
          FLAGS_upload_stage_throttle_bandwidth_mb ||
      current_throttle_iops_ != FLAGS_upload_stage_throttle_iops) {
    current_throttle_bandwidth_mb_ = FLAGS_upload_stage_throttle_bandwidth_mb;
    current_throttle_iops_ = FLAGS_upload_stage_throttle_iops;

    dingofs::utils::ReadWriteThrottleParams params;
    params.iopsWrite = dingofs::utils::ThrottleParams(
        current_throttle_iops_, current_throttle_iops_, 0);
    params.bpsWrite = dingofs::utils::ThrottleParams(
        current_throttle_bandwidth_mb_ * kMiB,
        current_throttle_bandwidth_mb_ * kMiB, 0);

    LOG(INFO) << "Update upload stage throttle params: "
              << "bandwidth_mb = " << current_throttle_bandwidth_mb_
              << ", iops = " << current_throttle_iops_;

    std::lock_guard<BthreadMutex> lk(mutex_);
    throttle_->UpdateThrottleParams(params);
  }
  executor_->Schedule([this] { UpdateThrottleParam(); }, 100);
}

}  // namespace cache
}  // namespace dingofs
