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

#include "blockcache/infiniband/common/protocol.h"

#include <glog/logging.h>

#include <cstring>

namespace dingofs {
namespace blockcache {
namespace infiniband {

void HandshakeMsg::ToPb(pb::blockcache::EndpointInfo* out) const {
  out->set_version(version);
  out->set_shard(shard);
  out->set_link_layer(link_layer);
  out->set_rpc_credits(rpc_credits);
  out->set_message_bytes(message_bytes);
  for (uint8_t i = 0; i < num_qps; ++i) {
    const QueuePairInfo& qp = qp_infos[i];
    pb::blockcache::QueuePairInfo* out_qp = out->add_queue_pair_infos();
    out_qp->set_qpn(qp.qpn);
    out_qp->set_psn(qp.psn);
    out_qp->set_lid(qp.lid);
    out_qp->set_port_num(qp.port_num);
    out_qp->set_mtu(qp.mtu);
    out_qp->set_gid(qp.gid, sizeof(qp.gid));
  }
}

bool HandshakeMsg::FromPb(const pb::blockcache::EndpointInfo& in) {
  if (in.queue_pair_infos_size() < 1 ||
      in.queue_pair_infos_size() > 1 + kMaxBulkQps) {
    return false;
  }
  std::memset(this, 0, sizeof(*this));
  version = static_cast<uint16_t>(in.version());
  shard = static_cast<uint16_t>(in.shard());
  num_qps = static_cast<uint8_t>(in.queue_pair_infos_size());
  link_layer = static_cast<uint8_t>(in.link_layer());
  rpc_credits = static_cast<uint16_t>(in.rpc_credits());
  message_bytes = static_cast<uint16_t>(in.message_bytes());
  for (int i = 0; i < in.queue_pair_infos_size(); ++i) {
    const pb::blockcache::QueuePairInfo& in_qp = in.queue_pair_infos(i);
    if (in_qp.gid().size() != sizeof(qp_infos[i].gid)) {
      return false;
    }
    QueuePairInfo& qp = qp_infos[i];
    qp.qpn = in_qp.qpn();
    qp.psn = in_qp.psn();
    qp.lid = static_cast<uint16_t>(in_qp.lid());
    qp.port_num = static_cast<uint8_t>(in_qp.port_num());
    qp.mtu = static_cast<uint8_t>(in_qp.mtu());
    std::memcpy(qp.gid, in_qp.gid().data(), sizeof(qp.gid));
  }
  return true;
}

const char* HandshakeMsg::Check(uint8_t local_qps,
                                LinkLayer local_link_layer) const {
  if (version != Protocol::kHandshakeVersion) {
    return "handshake version mismatch";
  } else if (num_qps != local_qps) {
    return "peer advertises a different number of QPs";
  } else if (num_qps == 0 || num_qps > 1 + kMaxBulkQps) {
    return "peer advertises an invalid number of QPs";
  } else if (static_cast<LinkLayer>(link_layer) != local_link_layer) {
    return "link layer mismatch";
  } else if (rpc_credits < Protocol::kMinRecvCredits) {
    return "peer advertises too few receive credits";
  } else if (rpc_credits < FLAGS_rdma_max_inflight_rpcs) {
    return "peer advertises fewer credits than our in-flight limit";
  } else if (message_bytes == 0) {
    return "peer advertises no receive capacity";
  } else if (message_bytes < FLAGS_rdma_message_bytes) {
    return "peer advertises a smaller message than ours";
  }
  return nullptr;
}

void Protocol::SetCredit(MessageBuffer message, uint16_t credit) {
  DCHECK_GE(message.capacity, sizeof(MessageHeader));
  static_cast<MessageHeader*>(message.data)->credit = credit;
}

RemoteRegion* Protocol::GetRegions(MessageBuffer message) {
  DCHECK_GE(message.capacity, sizeof(MessageHeader));
  return reinterpret_cast<RemoteRegion*>(static_cast<char*>(message.data) +
                                         sizeof(MessageHeader));
}

uint32_t Protocol::EncodeMessage(MessageBuffer message,
                                 const HeaderParam& param,
                                 const google::protobuf::Message* body) {
  const size_t payload_len = body != nullptr ? body->ByteSizeLong() : 0;
  if (param.region_count > kMaxRegions || payload_len > UINT32_MAX) {
    return 0;
  }
  const size_t total =
      MessageSize(param.region_count, static_cast<uint32_t>(payload_len));
  if (total > message.capacity) {
    return 0;
  }

  auto* header = static_cast<MessageHeader*>(message.data);
  header->type = static_cast<uint8_t>(param.type);
  header->region_type = static_cast<uint8_t>(param.region_type);
  header->region_count = param.region_count;
  header->reserved0 = 0;
  header->opcode = param.opcode;
  header->code = param.code;
  header->payload_len = static_cast<uint32_t>(payload_len);
  header->credit = 0;
  header->reserved1 = 0;
  header->correlation_id = param.correlation_id;

  if (payload_len > 0) {
    char* payload =
        static_cast<char*>(message.data) + MessageSize(param.region_count, 0);
    if (!body->SerializeToArray(payload, static_cast<int>(payload_len))) {
      return 0;
    }
  }
  return static_cast<uint32_t>(total);
}

const char* Protocol::DecodeMessage(BufferView message, MessageView* out) {
  if (message.size < sizeof(MessageHeader)) {
    return "message shorter than its header";
  }
  const auto* header = static_cast<const MessageHeader*>(message.data);
  if (header->type < static_cast<uint8_t>(MessageType::kRequest) ||
      header->type > static_cast<uint8_t>(MessageType::kControl)) {
    return "unknown message type";
  } else if (header->region_count > kMaxRegions) {
    return "too many regions";
  } else if (header->region_type >
             static_cast<uint8_t>(RegionType::kToClient)) {
    return "unknown region type";
  } else if (MessageSize(header->region_count, header->payload_len) >
             message.size) {
    return "message longer than the bytes received";
  }

  const auto* data = static_cast<const char*>(message.data);
  out->header = header;
  out->payload = std::string_view(data + MessageSize(header->region_count, 0),
                                  header->payload_len);
  return nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
