/*
 * Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include "cache/v2/core/net/rdma/rpc/request.h"

#include <glog/logging.h>

#include <cerrno>
#include <utility>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/connection.h"
#include "cache/v2/core/net/rdma/domain.h"
#include "cache/v2/core/net/rdma/memory_registry.h"

namespace dingofs {
namespace cache {
namespace v2 {

// `index` and `local.len` are peer-driven claims: error back, never CHECK.
OpAwaiter RdmaRequest::WriteTo(verbs::LocalBuf local, size_t index) {
  const OpAwaiter::Rejection why = CheckRegion(local, index);
  if (why != OpAwaiter::kAccepted) {
    return OpAwaiter(why);
  }
  return conn_->Write(local, RemoteOf(regions_[index]));
}

OpAwaiter RdmaRequest::ReadFrom(verbs::LocalBuf local, size_t index) {
  const OpAwaiter::Rejection why = CheckRegion(local, index);
  if (why != OpAwaiter::kAccepted) {
    return OpAwaiter(why);
  }
  return conn_->Read(local, RemoteOf(regions_[index]));
}

OpAwaiter::Rejection RdmaRequest::CheckRegion(const verbs::LocalBuf& local,
                                              size_t index) const {
  if (index >= regions_.size()) {
    return OpAwaiter::kNoSuchRegion;
  }
  if (local.len > regions_[index].len) {
    return OpAwaiter::kRegionTooSmall;
  }
  return OpAwaiter::kAccepted;
}

// One batch across regions; the local buffer must lie in a registered range.
Future<Status> RdmaRequest::FetchBody(BufferView dst) {
  return MoveBody(dst, /*read=*/true);
}

Future<Status> RdmaRequest::MoveBody(BufferView buffer, bool read) {
  if (buffer.empty()) {
    co_return ToStatus(EINVAL, "move a body: the buffer is empty");
  }
  // Resolve the registration here; unregistered memory would fail on the HCA.
  MemoryRegistry& mrs = conn_->domain()->memory();
  StatusOr<uint32_t> lkey = mrs.LkeyOf(buffer.data, buffer.size);
  if (!lkey.ok()) {
    co_return ToStatus(EINVAL, "move a body: the buffer is not registered");
  }
  if (regions_.empty()) {
    co_return ToStatus(EINVAL, "move a body: the peer advertised no region");
  }

  if (buffer.size > RegionBytes(regions_)) {
    co_return ToStatus(EMSGSIZE,
                          "move a body: it exceeds what the peer advertised");
  }

  char* at = static_cast<char*>(buffer.data);
  uint32_t left = buffer.size;
  OpBatch batch = conn_->Batch();
  for (size_t i = 0; i < regions_.size() && left != 0;) {
    const RegionDesc& region = regions_[i];
    const uint32_t n = region.len < left ? region.len : left;
    const verbs::LocalBuf local{at, n, lkey.value()};
    const verbs::RemoteBuf remote = RemoteOf(region).Slice(0, n);
    if (read ? batch.AddRead(local, remote) : batch.AddWrite(local, remote)) {
      at += n;
      left -= n;
      ++i;
      continue;
    }

    // The send queue is full. Retry THIS region rather than advancing past
    // it: skipping moves none of its bytes, which stayed invisible only
    // while a body was always a single region.
    if (batch.count() == 0) {
      // Nothing built yet, so there is nothing here to wait on -- an empty
      // batch is ready immediately. Retrying it would spin this shard
      // without ever yielding, and the poller that frees the slots runs on
      // this very shard, so the queue could never drain: a livelock, not a
      // stall. The single-op path parks on the queue and truly suspends.
      const Status parked = co_await (read ? conn_->Read(local, remote)
                                           : conn_->Write(local, remote));
      if (!parked.ok()) {
        co_return parked;
      }
      at += n;
      left -= n;
      ++i;
      continue;
    }
    // Awaiting what is built releases the slots it holds.
    const Status submitted = co_await batch.Submit();
    if (!submitted.ok()) {
      co_return submitted;
    }
    batch = conn_->Batch();
  }
  const Status status = co_await batch.Submit();
  if (!status.ok()) {
    // A bare remote-access error is unactionable; log both ends' addresses.
    LOG_EVERY_N(ERROR, 100)
        << "Fail to " << (read ? "read" : "write")
        << " a body: local=" << buffer.data << "+" << buffer.size
        << " lkey=" << lkey.value() << " remote=0x" << std::hex
        << regions_[0].addr << std::dec << "+" << regions_[0].len
        << " rkey=" << regions_[0].rkey << " regions=" << regions_.size()
        << ": " << status.ToString();
  }
  co_return status;
}

// The WRITE must complete before the reply: RC cannot order across QPs.
Future<Status> RdmaRequest::Reply(ReplyCode code, std::string_view payload) {
  MarkReplied();
  return conn_->messenger().SendFrame(FrameType::kResponse, BodyShape::kNone,
                                      opcode_, code, correlation_, {}, payload);
}

bool RdmaRequest::alive() const { return conn_->alive(); }

// A coroutine on purpose: the RegionDesc array must outlive the suspending
// call, so it lives in this frame rather than the caller's.
Future<StatusOr<Reply>> RdmaConnection::Call(Opcode opcode,
                                             std::string_view payload,
                                             Body body,
                                             uint64_t /*route_hint*/) {
  if (body.none()) {
    co_return co_await Call(opcode, payload, std::span<const RegionDesc>());
  }
  if (body.empty()) {
    co_return ToStatus(EINVAL, "send a request: the body is empty");
  }

  // One region per range. The peer sees a single stream and pulls it into
  // one buffer of its own, so a body scattered here costs nothing there.
  RegionDesc regions[kMaxRegions];
  uint8_t count = 0;
  const auto add = [this, &regions, &count](BufferView range) -> Status {
    if (range.empty()) {
      return ToStatus(EINVAL, "send a request: an empty body range");
    }
    if (count == kMaxRegions) {
      return ToStatus(E2BIG, "send a request: too many body ranges");
    }
    StatusOr<uint32_t> rkey = domain_->memory().RkeyOf(range.data, range.size);
    if (!rkey.ok()) {
      return ToStatus(EINVAL, "send a request: the body is not registered");
    }
    regions[count++] =
        RegionDesc{.addr = reinterpret_cast<uint64_t>(range.data),
                   .len = range.size,
                   .rkey = rkey.value()};
    return Status::OK();
  };

  if (body.shape() == BodyShape::kToServer) {
    for (const BufferView& range : body.source()) {
      const Status status = add(range);
      if (!status.ok()) {
        co_return status;
      }
    }
  } else {
    const Status status = add(body.destination());
    if (!status.ok()) {
      co_return status;
    }
  }

  // The shape travels: both directions look identical on the wire otherwise.
  co_return co_await Call(opcode, payload,
                          std::span<const RegionDesc>(regions, count),
                          body.shape());
}

Future<Status> RdmaRequest::DoReplyWithBody(ReplyCode code,
                                            std::string_view payload,
                                            BufferView body) {
  const Status moved = co_await MoveBody(body, /*read=*/false);
  if (!moved.ok()) {
    // The caller is owed one answer even when its region was unusable.
    (void)co_await Reply(kReplyHandlerError, {});
    co_return moved;
  }
  co_return co_await Reply(code, payload);
}

}  // namespace v2
}  // namespace cache
}  // namespace dingofs
