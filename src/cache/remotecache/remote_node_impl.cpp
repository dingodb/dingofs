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

#include "cache/remotecache/remote_node_impl.h"

#include <absl/strings/str_format.h>
#include <butil/iobuf.h>
#include <butil/logging.h>
#include <glog/logging.h>
#include <sys/types.h>

#include <memory>

#include "cache/blockcache/block_cache.h"
#include "cache/common/macro.h"
#include "cache/common/proto.h"
#include "cache/common/type.h"
#include "cache/remotecache/remote_node_health_checker.h"
#include "cache/remotecache/rpc_client.h"
#include "cache/utils/bthread.h"
#include "cache/utils/context.h"
#include "cache/utils/state_machine_impl.h"
#include "common/status.h"
#include "options/cache/tiercache.h"

namespace dingofs {
namespace cache {

DEFINE_bool(subrequest_ranges, true,
            "Whether split range request into subrequests");
DEFINE_uint32(subrequest_range_size, 262144, "Range size for each subrequest");
DEFINE_validator(subrequest_range_size, brpc::PassValidate);

RemoteNodeImpl::RemoteNodeImpl(const PBCacheGroupMember& member,
                               RemoteBlockCacheOption /*option*/)
    : running_(false),
      member_info_(member),
      rpc_(std::make_unique<RPCClient>(member.ip(), member.port())),
      state_machine_(std::make_shared<StateMachineImpl>()),
      health_checker_(
          std::make_unique<RemoteNodeHealthChecker>(member, state_machine_)),
      joiner_(std::make_unique<BthreadJoiner>()) {}

Status RemoteNodeImpl::Start() {
  CHECK_NOTNULL(rpc_);
  CHECK_NOTNULL(state_machine_);
  CHECK_NOTNULL(health_checker_);
  CHECK_NOTNULL(joiner_);

  if (running_) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node is starting: "
            << "id = " << member_info_.id()
            << ", endpoint = " << member_info_.ip() << ":"
            << member_info_.port();

  auto status = rpc_->Init();
  if (!status.ok()) {
    LOG(ERROR) << "RPC client init failed: " << status.ToString();
  }

  if (!state_machine_->Start()) {
    LOG(ERROR) << "State machine start failed.";
  }

  health_checker_->Start();

  CHECK(joiner_->Start().ok());

  running_ = true;

  LOG(INFO) << "Remote node is up: "
            << "id = " << member_info_.id()
            << ", endpoint = " << member_info_.ip() << ":"
            << member_info_.port();

  CHECK_RUNNING("Remote node");
  return Status::OK();
}

Status RemoteNodeImpl::Shutdown() {
  if (!running_.exchange(false)) {
    return Status::OK();
  }

  LOG(INFO) << "Remote node is shutting down: "
            << "id = " << member_info_.id()
            << ", endpoint = " << member_info_.ip() << ":"
            << member_info_.port();

  if (!joiner_->Shutdown().ok()) {
    LOG(ERROR) << "Shutdown bthread joiner failed.";
  }

  health_checker_->Shutdown();

  if (!state_machine_->Shutdown()) {
    LOG(ERROR) << "State machine shutdown failed.";
  }

  LOG(INFO) << "Remote node is down: "
            << "id = " << member_info_.id()
            << ", endpoint = " << member_info_.ip() << ":"
            << member_info_.port();

  CHECK_DOWN("Remote node");
  return Status::OK();
}

Status RemoteNodeImpl::Put(ContextSPtr ctx, const BlockKey& key,
                           const Block& block) {
  CHECK_RUNNING("Remote node");

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Put(ctx, key, block);
  if (!status.ok()) {
    LOG_PUT_ERROR();
  }
  return CheckStatus(status);
}

Status RemoteNodeImpl::Range(ContextSPtr ctx, const BlockKey& key, off_t offset,
                             size_t length, IOBuffer* buffer,
                             RangeOption option) {
  CHECK_RUNNING("Remote node");

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
    LOG_RANGE_ERROR();
  }
  return CheckStatus(status);
}

Status RemoteNodeImpl::Cache(ContextSPtr ctx, const BlockKey& key,
                             const Block& block) {
  CHECK_RUNNING("Remote node");

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Cache(ctx, key, block);
  if (!status.ok()) {
    LOG_CACHE_ERROR();
  }
  return status;  // Skip CheckStatus(...) here
}

Status RemoteNodeImpl::Prefetch(ContextSPtr ctx, const BlockKey& key,
                                size_t length) {
  CHECK_RUNNING("Remote node");

  auto status = CheckHealth(ctx);
  if (!status.ok()) {
    return status;
  }

  status = rpc_->Prefetch(ctx, key, length);
  if (!status.ok() && !status.IsExist()) {
    LOG_PREFETCH_ERROR();
  }
  return status;  // Skip CheckStatus(...) here
}

std::vector<SubRangeRequest> RemoteNodeImpl::SplitRange(off_t offset,
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

void RemoteNodeImpl::Subrequest(std::function<void()> func) {
  auto tid = RunInBthread([func]() { func(); });
  if (tid != 0) {
    joiner_->BackgroundJoin(tid);
  }
}

Status RemoteNodeImpl::SubrequestRanges(ContextSPtr ctx, const BlockKey& key,
                                        off_t offset, size_t length,
                                        IOBuffer* buffer, RangeOption option) {
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

Status RemoteNodeImpl::CheckHealth(ContextSPtr ctx) const {
  if (member_info_.state() !=
      PBCacheGroupMemberState::CacheGroupMemberStateOnline) {
    LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
        "[%s] Remote node is not online: "
        "id = %d, endpoint = %s:%d, status = %d",
        ctx->TraceId(), member_info_.id(), member_info_.ip(),
        member_info_.port(), member_info_.state());
    return Status::CacheUnhealthy("remote node is unstable");
  } else if (state_machine_->GetState() != State::kStateNormal) {
    LOG_EVERY_SECOND(WARNING) << absl::StrFormat(
        "[%s] Remote node is unhealthy: "
        "id = %d, endpoint = %s:%d, status = %s",
        ctx->TraceId(), member_info_.id(), member_info_.ip(),
        member_info_.port(), StateToString(state_machine_->GetState()));
    return Status::CacheUnhealthy("remote node is unhealthy");
  }
  return Status::OK();
}

Status RemoteNodeImpl::CheckStatus(Status status) {
  if (status.ok() || status.IsNotFound()) {
    state_machine_->Success();
  } else {
    state_machine_->Error();
  }
  return status;
}

}  // namespace cache
}  // namespace dingofs
