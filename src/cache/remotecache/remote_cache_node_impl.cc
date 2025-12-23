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

/*
 * Project: DingoFS
 * Created Date: 2025-02-10
 * Author: Jingli Chen (Wine93)
 */

#include "cache/remotecache/remote_cache_node_impl.h"

#include <absl/strings/str_format.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/common/macro.h"
#include "cache/common/state_machine.h"
#include "cache/common/state_machine_impl.h"
#include "cache/common/type.h"
#include "cache/remotecache/remote_cache_node_health_checker.h"
#include "cache/remotecache/rpc_client.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "common/status.h"

namespace dingofs {
namespace cache {

DEFINE_bool(subrequest_ranges, true,
            "whether split range request into subrequests");
DEFINE_validator(subrequest_ranges, brpc::PassValidate);

DEFINE_uint32(subrequest_range_size, 1048576, "range size for each subrequest");
DEFINE_validator(subrequest_range_size, brpc::PassValidate);

RemoteCacheNodeImpl::RemoteCacheNodeImpl(const CacheGroupMember& member)
    : running_(false),
      member_(member),
      rpc_(std::make_unique<RPCClient>(member.ip, member.port)),
      state_machine_(std::make_shared<StateMachineImpl>(kNodeStateMachine)),
      health_checker_(std::make_unique<RemoteCacheNodeHealthChecker>(
          member, state_machine_)),
      joiner_(std::make_unique<BthreadJoiner>()) {}

Status RemoteCacheNodeImpl::Start() {
  CHECK_NOTNULL(rpc_);
  CHECK_NOTNULL(state_machine_);
  CHECK_NOTNULL(health_checker_);
  CHECK_NOTNULL(joiner_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote cache node is starting: member = " << member_.ToString();

  auto status = rpc_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "Init rpc client failed: " << status.ToString();
  }

  if (!state_machine_->Start()) {
    LOG(ERROR) << "State machine start failed.";
  }

  health_checker_->Start();

  CHECK(joiner_->Start().ok());

  running_ = true;

  LOG(INFO) << "Remote cache node is up: member = " << member_.ToString();

  CHECK_RUNNING("Remote cache node");
  return Status::OK();
}

Status RemoteCacheNodeImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node is shutting down: member = " << member_.ToString();

  health_checker_->Shutdown();

  if (!state_machine_->Shutdown()) {
    LOG(ERROR) << "State machine shutdown failed.";
  }

  LOG(INFO) << "Remote node is down: member = " << member_.ToString();

  CHECK_DOWN("Remote cache node");
  return Status::OK();
}

Status RemoteCacheNodeImpl::Put(ContextSPtr ctx, const BlockKey& key,
                                const Block& block) {
  if (!IsRunning()) {
    return Status::Internal("remote cache node is not running");
  }

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Put(ctx, key, block);
  if (!status.ok()) {
    GENERIC_LOG_PUT_ERROR("cache node");
  }
  return CheckStatus(status);
}

Status RemoteCacheNodeImpl::Range(ContextSPtr ctx, const BlockKey& key,
                                  off_t offset, size_t length, IOBuffer* buffer,
                                  RangeOption option) {
  if (!IsRunning()) {
    return Status::Internal("remote cache node is not running");
  }

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  if (FLAGS_subrequest_ranges) {
    status = SubrequestRanges(ctx, key, offset, length, buffer, option);
  } else {
    status = rpc_->Range(ctx, key, offset, length, buffer, option);
  }
  if (!status.ok()) {
    GENERIC_LOG_RANGE_ERROR("remote cache node");
  }
  return CheckStatus(status);
}

Status RemoteCacheNodeImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                                  const Block& block) {
  if (!IsRunning()) {
    return Status::Internal("remote cache node is not running");
  }

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Cache(ctx, key, block);
  if (!status.ok()) {
    GENERIC_LOG_CACHE_ERROR("remote cache node");
  }
  return status;  // Skip CheckStatus(...) here
}

Status RemoteCacheNodeImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                     size_t length) {
  if (!IsRunning()) {
    return Status::Internal("remote cache node is not running");
  }

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Prefetch(ctx, key, length);
  if (!status.ok()) {
    GENERIC_LOG_PREFETCH_ERROR("remote cache node");
  }
  return status;  // Skip CheckStatus(...) here
}

std::vector<SubRangeRequest> RemoteCacheNodeImpl::SplitRange(off_t offset,
                                                             size_t length,
                                                             size_t blksize) {
  std::vector<SubRangeRequest> requests;
  while (length > 0) {
    off_t bound = (offset / blksize + 1) * blksize;
    size_t bytes = std::min(static_cast<size_t>(bound - offset), length);
    requests.emplace_back(SubRangeRequest{.offset = offset, .length = bytes});

    offset += bytes;
    length -= bytes;
  }
  return requests;
}

void RemoteCacheNodeImpl::Subrequest(std::function<void()> func) {
  auto tid = RunInBthread([func]() { func(); });
  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Status RemoteCacheNodeImpl::SubrequestRanges(ContextSPtr ctx,
                                             const BlockKey& key, off_t offset,
                                             size_t length, IOBuffer* buffer,
                                             RangeOption option) {
  auto requests = SplitRange(offset, length, FLAGS_subrequest_range_size);

  BthreadCountdownEvent countdown(requests.size());
  for (int i = 0; i < requests.size(); i++) {
    auto* request = &requests[i];
    Subrequest([&, request]() {
      option.is_subrequest = true;
      request->status = rpc_->Range(ctx, key, request->offset, request->length,
                                    &request->buffer, option);
      countdown.signal(1);
    });
  }
  countdown.wait();

  butil::IOBuf iobuf;
  for (auto& request : requests) {
    auto status = request.status;
    if (!status.ok()) {
      return status;
    }
    iobuf.append(request.buffer.ConstIOBuf());
  }

  *buffer = IOBuffer(iobuf);
  return Status::OK();
}

Status RemoteCacheNodeImpl::CheckHealth(ContextSPtr ctx) const {
  if (state_machine_->GetState() != State::kStateNormal) {
    LOG_EVERY_SECOND_CTX(WARNING) << "Remote cache node is unhealthy: state = "
                                  << StateToString(state_machine_->GetState())
                                  << ", member = " << member_.ToString();
    return Status::CacheUnhealthy("remote cache node is unhealthy");
  }
  return Status::OK();
}

Status RemoteCacheNodeImpl::CheckStatus(Status status) {
  if (status.ok() || status.IsNotFound()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
