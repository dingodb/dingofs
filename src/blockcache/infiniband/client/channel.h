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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CHANNEL_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CLIENT_CHANNEL_H_

#include <cstdint>

#include "blockcache/common/status.h"
#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/infiniband/base/memory_registry.h"
#include "blockcache/infiniband/client/context.h"
#include "blockcache/infiniband/client/dialer.h"
#include "blockcache/infiniband/client/inflight_rpc.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/connection.h"
#include "blockcache/net/channel.h"
#include "blockcache/net/types.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class Channel final : public blockcache::Channel {
 public:
  Channel();
  ~Channel() override;

  Channel(const Channel&) = delete;
  Channel& operator=(const Channel&) = delete;

  Future<Status> Init(ChannelOption option) override;
  Future<> Shutdown() override;

  Future<Status> CallMethod(blockcache::Call call) override;
  bool Alive() const override { return conn_ != nullptr && conn_->Alive(); }

 private:
  explicit Channel(InfinibandContext* context);

  struct Attachment {
    RegionType type = RegionType::kNone;
    BufferViews ranges;
  };

  static StatusOr<Attachment> ParseAttachment(const blockcache::Call& call);
  Future<StatusOr<uint16_t>> AcquireRequestSlot(
      google::protobuf::Message* response);
  Future<Status> SendRequest(uint64_t correlation_id,
                             const blockcache::Call& call,
                             const Attachment& attachment);
  Status EncodeRequest(uint64_t correlation_id, const blockcache::Call& call,
                       const Attachment& attachment, SendBuffer* buffer) const;
  StatusOr<uint8_t> AddRegions(RemoteRegion* regions, BufferViews ranges) const;

  Status OnMessageReceived(ReceiveBuffer* buffer, const MessageView& message);
  void OnResponseReceived(const MessageView& message);
  void FailAll(const Status& error);

  ChannelOption option_;
  EventHandler handler_;
  DialerUPtr dialer_;
  ConnectionUPtr conn_;
  const MemoryRegistry* registry_;
  InflightRpcTable<StatusOr<ReplyCode>> inflight_rpcs_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif
