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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <span>

#include "blockcache/core/memory/buffer_view.h"
#include "blockcache/core/reactor/coroutine.h"
#include "blockcache/core/reactor/timer.h"
#include "blockcache/infiniband/base/region.h"
#include "blockcache/infiniband/common/protocol.h"
#include "blockcache/infiniband/connection/flow_control.h"
#include "blockcache/infiniband/connection/queue_pairs.h"
#include "blockcache/infiniband/connection/receive_buffer.h"
#include "blockcache/infiniband/connection/receiver.h"
#include "blockcache/infiniband/connection/send_buffer.h"
#include "blockcache/infiniband/connection/sender.h"
#include "common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

class BufferPool;
class InfinibandPoller;

struct ConnectionOption {
  uint16_t max_send_credits = 0;
  uint64_t heartbeat_interval_ns = 0;
};

struct EventHandler {
  std::function<Status(ReceiveBuffer* buffer, const MessageView& message)>
      on_message_received;
  std::function<void(const Status& status)> on_error;
};

class Connection {
 public:
  Connection(QueuePairGroup qps, BufferPool* buffer_pool,
             ConnectionOption option);
  ~Connection();

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  Future<Status> Start(EventHandler handler);
  Future<bool> Shutdown();

  Future<SendBuffer*> AcquireSendBuffer(MessageType type);
  void ReleaseSendBuffer(SendBuffer* buffer, MessageType type);
  void ReleaseRecvBuffer(ReceiveBuffer* buffer);

  Status RDMASend(SendBuffer* buffer);
  Future<Status> RDMARead(std::span<const RemoteRegion> src, BufferView dst);
  Future<Status> RDMAWrite(BufferView src, std::span<const RemoteRegion> dst);

  bool Alive() const { return !has_error_; }
  void FailIfIdle(uint64_t idle_ns);

 private:
  friend class InfinibandPoller;  // hands both On* below a reaped completion

  void StartHeartbeat();
  void SendHeartbeat();

  Future<> Drain();

  void OnMessageReceived(ReceiveBuffer* buffer, const ibv_wc& wc);
  void OnMessageSent(SendBuffer* buffer, int wc_status);

  void ReturnCreditsIfNeeded();
  Future<Status> SendCreditReturn();
  Status SendControl(SendBuffer* buffer);

  void SetError(const Status& status);

  uint32_t msg_qpn() const;
  uint32_t inflights() const;

  ConnectionOption option_;
  QueuePairGroupUPtr qps_;
  ReceiveBufferPoolUPtr recv_buffers_;
  SendBufferPoolUPtr send_buffers_;
  ReceiverUPtr receiver_;
  MsgSenderUPtr msg_sender_;
  BulkSenderUPtr bulk_sender_;
  CreditFlowControlUPtr credit_flow_control_;
  TimerUPtr heartbeat_timer_;
  EventHandler handler_;

  bool running_ = false;
  bool has_error_ = false;
  uint64_t last_heard_ns_ = 0;
  uint64_t last_sent_ns_ = 0;
  bool return_credit_inflight_ = false;
  uint32_t unreleased_recv_buffers_ = 0;
};

using ConnectionUPtr = std::unique_ptr<Connection>;

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_CONNECTION_CONNECTION_H_
