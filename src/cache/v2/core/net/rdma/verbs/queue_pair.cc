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

#include "cache/v2/core/net/rdma/verbs/queue_pair.h"

#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "cache/v2/common/status.h"
#include "cache/v2/core/net/rdma/verbs/wc_status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

StatusOr<QueuePair> QueuePair::Create(Device& device, ibv_cq* cq,
                                      const QpOption& option) {
  ibv_qp_init_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.send_cq = cq;
  attr.recv_cq = cq;
  attr.qp_type = IBV_QPT_RC;
  attr.sq_sig_all = 0;  // signalling is decided per work request
  attr.cap.max_send_wr = option.max_send_wr;
  attr.cap.max_recv_wr = option.max_recv_wr;
  attr.cap.max_send_sge = option.max_send_sge;
  attr.cap.max_recv_sge = option.max_recv_sge;
  attr.cap.max_inline_data = option.max_inline_data;

  ibv_qp* qp = ibv_create_qp(device.pd(), &attr);
  if (qp == nullptr && option.max_inline_data != 0) {
    // Inline is an optimization; retry without it if the device refuses.
    attr.cap.max_inline_data = 0;
    qp = ibv_create_qp(device.pd(), &attr);
  }
  if (qp == nullptr) {
    return ToStatus(errno, "create queue pair");
  }

  // attr.cap is filled in by the driver with what it actually granted.
  return QueuePair(qp, attr.cap.max_inline_data);
}

void QueuePair::Reset() noexcept {
  if (qp_ != nullptr) {
    // Not PLOG: ibv_destroy_* returns the errno value, it does not set errno.
    if (const int rc = ibv_destroy_qp(qp_); rc != 0) {
      LOG(ERROR) << "Fail to destroy queue pair: " << std::strerror(rc);
    }
    qp_ = nullptr;
  }
}

Status QueuePair::ToInit(uint8_t port_num, unsigned access) {
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = port_num;
  attr.qp_access_flags = static_cast<int>(access);

  const int mask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  int rc = ibv_modify_qp(qp_, &attr, mask);
  if (rc != 0) {
    return ToStatus(rc, "move queue pair to init");
  }
  return Status::OK();
}

Status QueuePair::ToRtr(const QpPeer& remote, const PortInfo& local,
                        uint8_t rd_atomic) {
  const auto remote_layer = static_cast<LinkLayer>(remote.link_layer);
  if (remote_layer != local.link_layer) {
    LOG(ERROR) << "Fail to move queue pair to rtr: link layer mismatch, local="
               << LinkLayerName(local.link_layer)
               << " remote=" << LinkLayerName(remote_layer);
    return ToStatus(EPROTONOSUPPORT, "match the peer link layer");
  }

  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  // Both ends must agree, and the smaller MTU always works.
  attr.path_mtu = std::min(local.active_mtu, static_cast<ibv_mtu>(remote.mtu));
  attr.dest_qp_num = remote.qpn;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = rd_atomic;
  attr.min_rnr_timer = 12;

  attr.ah_attr.port_num = remote.port_num;
  attr.ah_attr.sl = 0;
  attr.ah_attr.src_path_bits = 0;
  if (local.link_layer == LinkLayer::kIB) {
    attr.ah_attr.is_global = 0;
    attr.ah_attr.dlid = remote.lid;
  } else {
    // RoCE: address by GID, carried in a global route header.
    attr.ah_attr.is_global = 1;
    std::memcpy(attr.ah_attr.grh.dgid.raw, remote.gid, sizeof(remote.gid));
    attr.ah_attr.grh.sgid_index = static_cast<uint8_t>(local.gid_index);
    attr.ah_attr.grh.hop_limit = 255;
    attr.ah_attr.grh.flow_label = 0;
    attr.ah_attr.grh.traffic_class = 0;
  }

  const int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                   IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                   IBV_QP_MIN_RNR_TIMER;
  int rc = ibv_modify_qp(qp_, &attr, mask);
  if (rc != 0) {
    return ToStatus(rc, "move queue pair to rtr");
  }
  return Status::OK();
}

Status QueuePair::ToRts(uint32_t local_psn, uint8_t rd_atomic,
                        const QpOption& option) {
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.sq_psn = local_psn;
  attr.timeout = option.timeout;
  attr.retry_cnt = option.retry_cnt;
  attr.rnr_retry = option.rnr_retry;
  attr.max_rd_atomic = rd_atomic;

  const int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                   IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  int rc = ibv_modify_qp(qp_, &attr, mask);
  if (rc != 0) {
    return ToStatus(rc, "move queue pair to rts");
  }
  return Status::OK();
}

// ERROR flushes every posted WR into the CQ, so a drain loop is bounded.
// It is also the rpc-timeout fence: late one-sided writes can't land.
void QueuePair::ToError() noexcept {
  if (qp_ == nullptr) {
    return;
  }
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_ERR;
  int rc = ibv_modify_qp(qp_, &attr, IBV_QP_STATE);
  if (rc != 0) {
    LOG(ERROR) << "Fail to move queue pair to error state: "
               << std::strerror(rc);
  }
}

void QueuePair::FillPeer(const PortInfo& local, uint32_t psn, uint8_t rd_atomic,
                         QpPeer* out) const {
  std::memset(out, 0, sizeof(*out));
  out->qpn = qpn();
  out->psn = psn;
  out->lid = local.lid;
  out->port_num = local.port_num;
  out->link_layer = static_cast<uint8_t>(local.link_layer);
  out->mtu = static_cast<uint8_t>(local.active_mtu);
  out->rd_atomic = rd_atomic;
  std::memcpy(out->gid, local.gid.raw, sizeof(out->gid));
}

uint8_t QueuePair::QueryRdAtomic() const {
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  std::memset(&attr, 0, sizeof(attr));
  std::memset(&init_attr, 0, sizeof(init_attr));
  if (ibv_query_qp(qp_, &attr, IBV_QP_MAX_QP_RD_ATOMIC, &init_attr) != 0) {
    return 0;
  }
  return attr.max_rd_atomic;
}

ibv_qp_state QueuePair::QueryState() const {
  ibv_qp_attr attr;
  ibv_qp_init_attr init_attr;
  std::memset(&attr, 0, sizeof(attr));
  std::memset(&init_attr, 0, sizeof(init_attr));
  if (ibv_query_qp(qp_, &attr, IBV_QP_STATE, &init_attr) != 0) {
    return IBV_QPS_ERR;
  }
  return attr.qp_state;
}

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs
