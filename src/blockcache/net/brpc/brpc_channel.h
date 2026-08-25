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

#ifndef DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_NET_BRPC_BRPC_CHANNEL_H_

#include <atomic>
#include <memory>

#include "blockcache/common/status.h"
#include "blockcache/net/channel.h"

namespace brpc {
class Channel;
}

namespace dingofs {
namespace blockcache {

class BrpcChannel final : public Channel {
 public:
  explicit BrpcChannel(unsigned shard);
  ~BrpcChannel() override;

  BrpcChannel(const BrpcChannel&) = delete;
  BrpcChannel& operator=(const BrpcChannel&) = delete;

  Future<Status> Init(ChannelOption option) override;

  Future<> Shutdown() override;

  Future<Status> CallMethod(blockcache::Call call) override;

  void CallStarted() { inflight_.fetch_add(1, std::memory_order_relaxed); }
  void CallFinished() { inflight_.fetch_sub(1, std::memory_order_release); }

  unsigned shard() const { return shard_; }

 private:
  void Close();

  unsigned shard_;
  ChannelOption option_;
  std::unique_ptr<::brpc::Channel> channel_;
  std::atomic<int64_t> inflight_{0};
};

}  // namespace blockcache
}  // namespace dingofs

#endif
