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

#include "blockcache/net/brpc/brpc_channel.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/reloadable_flags.h>
#include <butil/iobuf.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <string>
#include <thread>
#include <utility>

#include "blockcache/common/flag_decls.h"
#include "blockcache/common/status.h"
#include "blockcache/core/runtime/smp.h"
#include "blockcache/core/runtime/worker_pool.h"
#include "blockcache/net/brpc/brpc_server.h"

namespace dingofs {
namespace blockcache {

DEFINE_int32(remote_rpc_max_retry, 0,
             "brpc-level retries per cache-node rpc; <=0 disables them");
DEFINE_validator(remote_rpc_max_retry, brpc::PassValidate);

struct NativeClientCall : InboxWork, public google::protobuf::Closure {
  explicit NativeClientCall(BrpcChannel* c) : channel(c) {
    run = &NativeClientCall::OnShard;
  }

  void Run() override {
    if (PostTo(channel->shard(), this)) {
      return;
    }
    LOG(ERROR) << "Fail to deliver an rpc reply to shard " << channel->shard()
               << ": it is stopping";
    channel->CallFinished();
    delete this;
  }

  static void OnShard(InboxWork* base) {
    auto* self = static_cast<NativeClientCall*>(base);
    if (self->cntl.Failed() || self->recv.data == nullptr) {
      self->Finish();
      return;
    }
    const butil::IOBuf& attachment = self->cntl.response_attachment();
    self->copy_bytes = attachment.size() < self->recv.size ? attachment.size()
                                                           : self->recv.size;
    WorkerPool* workers = GetGlobalWorkers();
    if (workers == nullptr || !WorkerPool::ShouldOffload(self->copy_bytes)) {
      attachment.copy_to(self->recv.data, self->copy_bytes);
      self->Finish();
      return;
    }
    workers->CountCopy(self->copy_bytes);
    self->run = &NativeClientCall::OnCopied;
    workers->Post(self);
  }

  static void OnCopied(InboxWork* base) {
    auto* self = static_cast<NativeClientCall*>(base);
    self->cntl.response_attachment().copy_to(self->recv.data, self->copy_bytes);
    self->run = &NativeClientCall::OnResolve;
    if (PostTo(self->channel->shard(), self)) {
      return;
    }
    LOG(ERROR) << "Fail to deliver a copied rpc reply to shard "
               << self->channel->shard() << ": it is stopping";
    self->channel->CallFinished();
    delete self;
  }

  static void OnResolve(InboxWork* base) {
    static_cast<NativeClientCall*>(base)->Finish();
  }

  void Finish() {
    channel->CallFinished();
    if (cntl.Failed()) {
      promise.SetValue(Status::NetError("Fail to complete an rpc (brpc error " +
                                        std::to_string(cntl.ErrorCode()) +
                                        "): " + cntl.ErrorText()));
    } else {
      promise.SetValue(Status::OK());
    }
    delete this;
  }

  BrpcChannel* channel;
  BufferView recv;
  size_t copy_bytes = 0;
  ::brpc::Controller cntl;
  Promise<Status> promise;
};

BrpcChannel::BrpcChannel(unsigned shard) : shard_(shard) {}

BrpcChannel::~BrpcChannel() { Close(); }

Future<Status> BrpcChannel::Init(ChannelOption option) {
  option_ = std::move(option);

  ::brpc::ChannelOptions options;
  options.connect_timeout_ms =
      static_cast<int32_t>(FLAGS_remote_connect_timeout_ms);
  options.timeout_ms = static_cast<int32_t>(FLAGS_remote_rpc_timeout_ms);
  options.max_retry = FLAGS_remote_rpc_max_retry;
  options.connection_type = "single";
  options.connection_group = option_.tag;

  channel_ = std::make_unique<::brpc::Channel>();
  if (channel_->Init(option_.server.c_str(), &options) != 0) {
    channel_.reset();
    return MakeReadyFuture<Status>(
        Status::NetError("Fail to connect to " + option_.server));
  }
  return MakeReadyFuture<Status>(Status::OK());
}

Future<> BrpcChannel::Shutdown() {
  Close();
  return MakeReadyFuture<>();
}

void BrpcChannel::Close() {
  DrainInflight(inflight_);
  channel_.reset();
}

Future<Status> BrpcChannel::CallMethod(blockcache::Call call) {
  auto* native = new NativeClientCall(this);
  native->recv = call.recv;

  for (const BufferView& range : call.send) {
    native->cntl.request_attachment().append_user_data_with_meta(
        range.data, range.size, [](void*) {}, 0);
  }

  Future<Status> done = native->promise.GetFuture();
  CallStarted();
  channel_->CallMethod(call.method, &native->cntl, call.request, call.response,
                       native);
  return done;
}

}  // namespace blockcache
}  // namespace dingofs
