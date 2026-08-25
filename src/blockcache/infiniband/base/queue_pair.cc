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

#include "blockcache/infiniband/base/queue_pair.h"

#include <butil/fast_rand.h>
#include <glog/logging.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

StatusOr<QueuePair> QueuePair::Create(Device& device, ibv_cq* cq,
                                      const QueuePairOption& option) {
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

  QueuePairOption granted = option;
  granted.max_inline_data = attr.cap.max_inline_data;
  QueuePair queue_pair(&device, qp, granted);

  LOG(INFO) << "Successfully create QueuePair{device=" << device.name()
            << " qpn=" << qp->qp_num << " start_psn=" << queue_pair.start_psn_
            << " max_send_wr=" << attr.cap.max_send_wr
            << " max_recv_wr=" << attr.cap.max_recv_wr
            << " max_inline_data=" << attr.cap.max_inline_data << "}";
  return queue_pair;
}

Status QueuePair::ModifyToInit() {
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_INIT;
  attr.pkey_index = 0;
  attr.port_num = device_->port_info().port_num;
  attr.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

  const int mask =
      IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
  int rc = ibv_modify_qp(qp_, &attr, mask);
  if (rc != 0) {
    LOG(ERROR) << "Fail to modify queue pair to init: " << std::strerror(rc);
    return ToStatus(rc, "modify queue pair to init");
  }

  LOG(INFO) << "Successfully modify QueuePair to init{qpn=" << qp_->qp_num
            << " port_num=" << static_cast<int>(attr.port_num) << "}";
  return Status::OK();
}

Status QueuePair::ModifyToRtr(const QueuePairInfo& remote) {
  const PortInfo& local = device_->port_info();

  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTR;
  // Both ends must agree, and the smaller MTU always works.
  attr.path_mtu = std::min(local.active_mtu, static_cast<ibv_mtu>(remote.mtu));
  attr.dest_qp_num = remote.qpn;
  attr.rq_psn = remote.psn;
  attr.max_dest_rd_atomic = kMaxRdAtomic;
  attr.min_rnr_timer = option_.min_rnr_timer;

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
    LOG(ERROR) << "Fail to modify queue pair to rtr: " << std::strerror(rc);
    return ToStatus(rc, "modify queue pair to rtr");
  }

  LOG(INFO) << "Successfully modify QueuePair to rtr{qpn=" << qp_->qp_num
            << " dest_qpn=" << remote.qpn << " rq_psn=" << remote.psn
            << " mtu=" << static_cast<int>(attr.path_mtu) << "}";
  return Status::OK();
}

Status QueuePair::ModifyToRts() {
  ibv_qp_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  attr.qp_state = IBV_QPS_RTS;
  attr.sq_psn = start_psn_;
  attr.timeout = option_.timeout;
  attr.retry_cnt = option_.retry_cnt;
  attr.rnr_retry = option_.rnr_retry;
  attr.max_rd_atomic = kMaxRdAtomic;

  const int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                   IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  int rc = ibv_modify_qp(qp_, &attr, mask);
  if (rc != 0) {
    LOG(ERROR) << "Fail to modify queue pair to rts: " << std::strerror(rc);
    return ToStatus(rc, "modify queue pair to rts");
  }

  LOG(INFO) << "Successfully modify QueuePair to rts{qpn=" << qp_->qp_num
            << " sq_psn=" << start_psn_ << "}";
  return Status::OK();
}

// ERROR flushes every posted WR into the CQ, so a drain loop is bounded.
// It is also the rpc-timeout fence: late one-sided writes can't land.
void QueuePair::ModifyToError() noexcept {
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
    return;
  }

  LOG(INFO) << "Successfully modify QueuePair to error{qpn=" << qp_->qp_num
            << "}";
}

QueuePairInfo QueuePair::GetInfo() const {
  const PortInfo& local = device_->port_info();
  QueuePairInfo info;
  std::memset(&info, 0, sizeof(info));
  info.qpn = qp_->qp_num;
  info.psn = start_psn_;
  info.lid = local.lid;
  info.port_num = local.port_num;
  info.mtu = static_cast<uint8_t>(local.active_mtu);
  std::memcpy(info.gid, local.gid.raw, sizeof(info.gid));
  return info;
}

QueuePair::QueuePair(Device* device, ibv_qp* qp, const QueuePairOption& option)
    : device_(device),
      qp_(qp),
      option_(option),
      start_psn_(butil::fast_rand() & 0xffffff) {}

void QueuePair::Reset() noexcept {
  if (qp_ == nullptr) {
    return;
  }

  const int rc = ibv_destroy_qp(qp_);
  if (rc != 0) {
    LOG(ERROR) << "Fail to destroy queue pair: " << std::strerror(rc);
  }
  qp_ = nullptr;
}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
