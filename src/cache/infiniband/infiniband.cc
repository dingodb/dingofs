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

/*
 * Project: DingoFS
 * Created Date: 2026-04-28
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/infiniband.h"

#include <butil/memory/scope_guard.h>
#include <fcntl.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstddef>
#include <cstring>
#include <iomanip>
#include <memory>

#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {

DEFINE_bool(use_rdma, false, "Enable Infiniband/RDMA transport for cache RPCs");

DEFINE_string(rdma_device, "", "IB device used by the cache RDMA path");

DEFINE_uint32(rdma_port_num, 1,
              "HCA port (1-based) used by the cache RDMA path");

namespace infiniband {

DEFINE_int32(rdma_gid_idx, 0,
             "GID table index for RoCE (use `show_gids` to pick the entry "
             "matching the desired RoCE version and IP family)");

std::unordered_map<std::string, DeviceUPtr> Infiniband::devices_;
std::unordered_map<std::string, PortUPtr> Infiniband::ports_;
std::unordered_map<std::string, ProtectDomainUPtr> Infiniband::protect_domains_;

Port::Port(uint8_t port_num, ibv_port_attr port_attr, uint16_t lid, ibv_gid gid)
    : port_num_(port_num), port_attr_(port_attr), lid_(lid), gid_(gid) {}

PortUPtr Port::Query(Device* device, uint8_t port_num) {
  ibv_port_attr port_attr;
  int rc = ibv_query_port(device->GetIbContext(), port_num, &port_attr);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to query port=" << (int)port_num
                << " of device=" << device->GetName();
    return nullptr;
  }

  ibv_gid gid;
  rc =
      ibv_query_gid(device->GetIbContext(), port_num, FLAGS_rdma_gid_idx, &gid);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to query gid of port=" << (int)port_num
                << " of device=" << device->GetName()
                << " with gid_idx=" << FLAGS_rdma_gid_idx;
    return nullptr;
  }

  LOG(INFO) << "Successfully query port=" << (int)port_num
            << " of device=" << device->GetName() << ": lid=" << port_attr.lid
            << ", gid=" << fmt::format("{:02x}", fmt::join(gid.raw, ""));

  return std::make_unique<Port>(port_num, port_attr, port_attr.lid, gid);
}

PortState Port::GetPortState() const {
  if (port_attr_.state == IBV_PORT_ACTIVE) {
    return PortState::kActive;
  } else if (port_attr_.state == IBV_PORT_DOWN) {
    return PortState::kDown;
  } else {
    return PortState::kUnknown;
  }
}

LinkLayer Port::GetLinkLayer() const {
  if (port_attr_.link_layer == IBV_LINK_LAYER_ETHERNET) {
    return LinkLayer::kEthernet;
  } else if (port_attr_.link_layer == IBV_LINK_LAYER_INFINIBAND) {
    return LinkLayer::kIB;
  } else {
    return LinkLayer::kUnspecified;
  }
}

Device::Device(const std::string& name, ibv_context* context)
    : name_(name), context_(context) {
  CHECK_NOTNULL(context_);
}

DeviceUPtr Device::Open(const std::string& device_name) {
  int num_devices = 0;
  ibv_device** devices = ibv_get_device_list(&num_devices);
  if (nullptr == devices) {
    PLOG(ERROR) << "Fail to get infiniband device list";
    return nullptr;
  }

  BRPC_SCOPE_EXIT { ibv_free_device_list(devices); };

  ibv_device* device = nullptr;
  for (int i = 0; i < num_devices; ++i) {
    if (ibv_get_device_name(devices[i]) == device_name) {
      device = devices[i];
      break;
    }
  }

  if (nullptr == device) {
    LOG(ERROR) << "Fail to find device with name=" << device_name;
    return nullptr;
  }

  ibv_device_attr device_attr;
  ibv_context* context = ibv_open_device(device);
  int rc = ibv_query_device(context, &device_attr);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to query device=" << device_name;
    return nullptr;
  }

  LOG(INFO) << "Successfully open device=" << device_name;
  return std::make_unique<Device>(device_name, context);
}

ProtectDomain::ProtectDomain(ibv_pd* pd) : pd_(pd) { CHECK_NOTNULL(pd); }

ProtectDomainUPtr ProtectDomain::Alloc(Device* device) {
  ibv_pd* pd = ibv_alloc_pd(device->GetIbContext());
  if (nullptr == pd) {
    PLOG(ERROR) << "Fail to allocate protection domain in device="
                << device->GetName();
    return nullptr;
  }
  return std::make_unique<ProtectDomain>(pd);
}

MemoryRegion::MemoryRegion(ibv_mr* mr) : mr_(CHECK_NOTNULL(mr)) {}

MemoryRegion::~MemoryRegion() {
  if (ibv_dereg_mr(mr_) != 0) {
    PLOG(ERROR) << "Fail to deregister memory region";
  }
}

MemoryRegionUPtr MemoryRegion::Register(ProtectDomain* protect_domain,
                                        void* addr, size_t length) {
  ibv_mr* mr = ibv_reg_mr(protect_domain->GetIbPd(), addr, length,
                          IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                              IBV_ACCESS_REMOTE_WRITE);
  if (mr == nullptr) {
    PLOG(ERROR) << "Fail to register memory region";
    return nullptr;
  }
  return std::make_unique<MemoryRegion>(mr);
}

QueuePair::QueuePair(ibv_qp* qp, Device* device, Port* port,
                     ProtectDomain* protect_domain)
    : qp_(qp), device_(device), port_(port), protect_domain_(protect_domain) {
  CHECK_NOTNULL(qp);
  CHECK_NOTNULL(device);
  CHECK_NOTNULL(port);
  CHECK_NOTNULL(protect_domain);
}

QueuePairUPtr QueuePair::Create(Device* device, Port* port,
                                ProtectDomain* protect_domain,
                                CompletionQueue* completion_queue) {
  // queue pair capacity
  // TODO: configurable (max_send_wr, max_recv_wr, max_send_sge)
  ibv_qp_cap cap;
  cap.max_send_wr = 4096;
  cap.max_recv_wr = 4096;
  cap.max_send_sge = 16;
  cap.max_recv_sge = 1;
  cap.max_inline_data = 0;

  // queue pair init attribute
  ibv_qp_init_attr attr;
  memset(&attr, 0, sizeof(attr));

  attr.qp_context = nullptr;
  attr.send_cq = completion_queue->GetIbCq();
  attr.recv_cq = completion_queue->GetIbCq();
  attr.srq = nullptr;
  attr.cap = cap;
  attr.qp_type = IBV_QPT_RC;
  attr.sq_sig_all = 0;

  ibv_qp* qp = ibv_create_qp(protect_domain->GetIbPd(), &attr);
  if (nullptr == qp) {
    PLOG(ERROR) << "Fail to create QueuePair for " << device->GetName() << ":"
                << port->GetPortNum();
    ibv_destroy_qp(qp);
    return nullptr;
  }

  auto queue_pair =
      std::make_unique<QueuePair>(qp, device, port, protect_domain);
  LOG(INFO) << "Sucessfully create " << *queue_pair;
  return queue_pair;
}

Status QueuePair::ModifyQpToInit() {
  ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));

  qp_attr.qp_state = IBV_QPS_INIT;
  qp_attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
  qp_attr.pkey_index = 0;
  qp_attr.port_num = port_->GetPortNum();
  int attr_mask =
      IBV_QP_STATE | IBV_QP_ACCESS_FLAGS | IBV_QP_PKEY_INDEX | IBV_QP_PORT;
  int rc = ibv_modify_qp(qp_, &qp_attr, attr_mask);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to modify " << this << " to init state";
    return Status::Internal("modify qp to init state failed");
  }

  LOG(INFO) << "Successfully modify " << *this << " to init state";
  return Status::OK();
}

Status QueuePair::ModifyQpToRtr(ConnManagmentMeta remote_cm_meta) {
  // queue pair attributes
  ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTR;
  qp_attr.path_mtu = std::min(port_->GetActiveMtu(), remote_cm_meta.mtu);
  qp_attr.dest_qp_num = remote_cm_meta.qpn;
  qp_attr.rq_psn = 0;  // receive queue: start packet sequence number
  qp_attr.max_dest_rd_atomic = 16;  // max inflight read
  qp_attr.min_rnr_timer = 1;  // tell remote: nerver wait, retry immediately

  if (port_->GetLinkLayer() != remote_cm_meta.link_type) {
    LOG(ERROR) << "Local device port=" << port_->GetPortNum() << " in "
               << port_->GetLinkLayer() << " mode, but remote is in "
               << remote_cm_meta.link_type << " mode";
    return Status::InvalidParam("different link layer mode");
  }

  // address handle attributes
  ibv_ah_attr ah_attr;
  std::memset(&ah_attr, 0, sizeof(ah_attr));
  ah_attr.port_num = remote_cm_meta.port_num;
  ah_attr.src_path_bits = 0;
  if (port_->GetLinkLayer() == LinkLayer::kIB) {
    ah_attr.is_global = 0;  // 0: IB, 1: RoCE or cross IB subnet
    ah_attr.dlid = remote_cm_meta.lid;
    ah_attr.sl = 0;
  } else if (port_->GetLinkLayer() == LinkLayer::kEthernet) {
    ah_attr.is_global = 1;

    // global route header
    ibv_global_route grh;
    std::memset(&grh, 0, sizeof(grh));
    grh.dgid = remote_cm_meta.gid;
    grh.flow_label = 0;
    grh.sgid_index = FLAGS_rdma_gid_idx;  // TODO: automic
    grh.hop_limit = 255;                  // reach maxinum
    grh.traffic_class = 0;
    ah_attr.grh = grh;
  } else {
    LOG(ERROR) << "Unsupport link layer=" << port_->GetLinkLayer();
    return Status::NotSupport("unsupport link layer");
  }
  qp_attr.ah_attr = ah_attr;

  int attr_mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                  IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
                  IBV_QP_MIN_RNR_TIMER;

  int rc = ibv_modify_qp(qp_, &qp_attr, attr_mask);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to modify " << *this << " to rtr state";
    return Status::Internal("modify qp to rtr state failed");
  }

  LOG(INFO) << "Successfully modify " << *this << " to rtr state";
  return Status::OK();
}

Status QueuePair::ModifyQpToRts() {
  ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_RTS;
  qp_attr.sq_psn = 0;
  qp_attr.timeout = 14;  // send request ack timeout, 14 ~= 67ms
  qp_attr.retry_cnt = 7;
  qp_attr.rnr_retry = 0;
  qp_attr.sq_psn = 0;
  qp_attr.max_rd_atomic = 16;

  int attr_mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                  IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
  int rc = ibv_modify_qp(qp_, &qp_attr, attr_mask);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to modify " << *this << " to rts state";
    return Status::Internal("modify qp to rts state failed");
  }

  LOG(INFO) << "Successfully modify " << *this << " to rts state";
  return Status::OK();
}

Status QueuePair::ModifyQpToError() {
  ibv_qp_attr qp_attr;
  memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.qp_state = IBV_QPS_ERR;

  int rc = ibv_modify_qp(qp_, &qp_attr, IBV_QP_STATE);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to modify " << this << " to error state";
    return Status::Internal("modify qp to error state failed");
  }

  LOG(INFO) << "Successfully modify " << this << " to error state";
  return Status::OK();
}

ConnManagmentMeta QueuePair::GetConnManagmentMeta() {
  ConnManagmentMeta cm_meta;
  cm_meta.qpn = GetQpNum();
  cm_meta.lid = port_->GetLid();
  cm_meta.gid = port_->GetGid();
  cm_meta.port_num = port_->GetPortNum();
  cm_meta.link_type = port_->GetLinkLayer();
  cm_meta.mtu = port_->GetActiveMtu();

  return cm_meta;
}

CompletionQueue::CompletionQueue(ibv_cq* cq, ibv_comp_channel* channel)
    : num_unack_events_(0), cq_(cq), channel_(channel) {
  CHECK_NOTNULL(cq);
  CHECK_NOTNULL(channel);
}

CompletionQueue::~CompletionQueue() {
  if (cq_) {
    ibv_destroy_cq(cq_);
  }
  if (channel_) {
    ibv_destroy_comp_channel(channel_);
  }
}

CompletionQueueUPtr CompletionQueue::Create(Device* device) {
  ibv_comp_channel* channel = ibv_create_comp_channel(device->GetIbContext());
  if (nullptr == channel) {
    PLOG(ERROR) << "Fail to create completion channel in device="
                << device->GetName();
    return nullptr;
  }

  int rc = fcntl(channel->fd, F_GETFL);
  if (rc == -1) {
    PLOG(ERROR) << "Fail to get flags of completion channel fd";
    ibv_destroy_comp_channel(channel);
    return nullptr;
  }

  int flags = rc;
  rc = fcntl(channel->fd, F_SETFL, flags | O_NONBLOCK);
  if (rc == -1) {
    PLOG(ERROR) << "Fail to set flags of completion channel fd";
    ibv_destroy_comp_channel(channel);
    return nullptr;
  }

  ibv_cq* cq = ibv_create_cq(device->GetIbContext(), 8192, nullptr, channel, 0);
  if (!cq) {
    PLOG(ERROR) << "Fail to create completion queue in device="
                << device->GetName();
    ibv_destroy_comp_channel(channel);
    return nullptr;
  }

  rc = ibv_req_notify_cq(cq, 0);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to request notify for completion queue";
    ibv_destroy_cq(cq);
    ibv_destroy_comp_channel(channel);
    return nullptr;
  }

  return std::make_unique<CompletionQueue>(cq, channel);
}

bool CompletionQueue::GetCqEvent() {
  ibv_cq* cq = nullptr;
  void* context = nullptr;
  int rc = ibv_get_cq_event(channel_, &cq, &context);
  if (rc != 0) {
    if (errno != EAGAIN && errno != EWOULDBLOCK) {
      return false;
    }
    return true;
  }

  num_unack_events_++;
  if (num_unack_events_ >= 1024) {
    ibv_ack_cq_events(cq_, num_unack_events_);
    num_unack_events_ = 0;
  }
  return true;
}

bool CompletionQueue::RearmNotify() {
  int rc = ibv_req_notify_cq(cq_, 0);
  if (rc != 0) {
    PLOG(ERROR) << "Fail to rearm notify completion queue";
    return false;
  }

  return true;
}

Device* Infiniband::GetOrOpen(const std::string& device_name) {
  if (devices_.count(device_name) == 0) {
    auto device = Device::Open(device_name);
    if (device == nullptr) {
      return nullptr;
    }
    devices_.emplace(device_name, std::move(device));
  }
  return devices_[device_name].get();
}

Port* Infiniband::GetOrQuery(Device* device, uint8_t port_num) {
  auto key = fmt::format("{}:{}", device->GetName(), port_num);
  if (ports_.count(key) == 0) {
    auto port = Port::Query(device, port_num);
    if (port == nullptr) {
      return nullptr;
    }
    ports_.emplace(key, std::move(port));
  }
  return ports_[key].get();
}

ProtectDomain* Infiniband::GetOrAlloc(Device* device) {
  if (protect_domains_.count(device->GetName()) == 0) {
    auto protect_domain = ProtectDomain::Alloc(device);
    if (protect_domain == nullptr) {
      return nullptr;
    }
    protect_domains_.emplace(device->GetName(), std::move(protect_domain));
  }
  return protect_domains_[device->GetName()].get();
}

Status Infiniband::Init(const std::string& device_name, uint8_t port_num,
                        Context* context) {
  auto* device = Infiniband::GetOrOpen(device_name);
  if (nullptr == device) {
    LOG(ERROR) << "Fail to open device=" << device_name;
    return Status::Internal("open device failed");
  }

  auto* port = Infiniband::GetOrQuery(device, port_num);
  if (nullptr == port) {
    LOG(ERROR) << "Fail to query port=" << (int)port_num
               << " of device=" << device_name;
    return Status::Internal("query port failed");
  } else if (port->GetPortState() != PortState::kActive) {
    LOG(ERROR) << "Port=" << (int)port_num << " of device=" << device_name
               << " is not active";
    return Status::Internal("port is not active");
  } else if (port->GetLinkLayer() == LinkLayer::kUnspecified) {
    LOG(ERROR) << "Port=" << (int)port_num << " of device=" << device_name
               << " has unspecified link layer";
    return Status::Internal("unspecified link layer");
  }

  auto* protect_domain = Infiniband::GetOrAlloc(device);
  if (nullptr == protect_domain) {
    LOG(ERROR) << "Fail to alloc protect domain of device=" << device_name;
    return Status::Internal("alloc protect domain failed");
  }

  context->device = device;
  context->port = port;
  context->protect_domain = protect_domain;
  return Status::OK();
}

void SerializeToPb(const ConnManagmentMeta& cm_meta,
                   pb::infiniband::ConnManagementMeta* pb_cm_meta) {
  pb_cm_meta->set_qpn(cm_meta.qpn);
  pb_cm_meta->set_lid(cm_meta.lid);
  pb_cm_meta->set_gid(&cm_meta.gid, sizeof(cm_meta.gid));
  pb_cm_meta->set_port_num(cm_meta.port_num);
  pb_cm_meta->set_link_type(static_cast<uint32_t>(cm_meta.link_type));
  pb_cm_meta->set_mtu(static_cast<uint32_t>(cm_meta.mtu));
}

Status ParseFromPb(const pb::infiniband::ConnManagementMeta& pb_cm_meta,
                   ConnManagmentMeta* cm_meta) {
  if (pb_cm_meta.gid().size() != sizeof(cm_meta->gid)) {
    LOG(ERROR) << "Invalid gid size=" << pb_cm_meta.gid().size()
               << " in connection managment meta";
    return Status::Internal("invalid gid size in connection managment meta");
  }

  cm_meta->qpn = pb_cm_meta.qpn();
  cm_meta->lid = static_cast<uint16_t>(pb_cm_meta.lid());
  std::memcpy(&cm_meta->gid, pb_cm_meta.gid().data(), sizeof(ibv_gid));
  cm_meta->port_num = static_cast<uint8_t>(pb_cm_meta.port_num());
  cm_meta->link_type = static_cast<LinkLayer>(pb_cm_meta.link_type());
  cm_meta->mtu = static_cast<ibv_mtu>(pb_cm_meta.mtu());
  return Status::OK();
}

std::ostream& operator<<(std::ostream& os, const QueuePair& queue_pair) {
  os << "QueuePair{qpn=" << queue_pair.GetQpNum()
     << " device=" << queue_pair.GetDevice()->GetName()
     << " port=" << static_cast<int>(queue_pair.GetPort()->GetPortNum()) << "}";

  return os;
}

std::ostream& operator<<(std::ostream& os, LinkLayer link_layer) {
  if (link_layer == LinkLayer::kIB) {
    os << "IB";
  } else if (link_layer == LinkLayer::kEthernet) {
    os << "Ethernet";
  } else {
    os << "Unspecified";
  }

  return os;
}

std::ostream& operator<<(std::ostream& os, const ConnManagmentMeta& cm_meta) {
  auto flags = os.flags();
  auto fill = os.fill();

  os << "ConnManagmentMeta{qpn=" << cm_meta.qpn << " lid=" << cm_meta.lid
     << " port_num=" << static_cast<int>(cm_meta.port_num)
     << " link_type=" << cm_meta.link_type
     << " mtu=" << static_cast<int>(cm_meta.mtu) << " gid=" << std::hex
     << std::setfill('0');
  for (int i = 0; i < 8; i++) {
    if (i != 0) {
      os << ":";
    }
    uint16_t word = (static_cast<uint16_t>(cm_meta.gid.raw[i * 2]) << 8) |
                    static_cast<uint16_t>(cm_meta.gid.raw[(i * 2) + 1]);
    os << std::setw(4) << word;
  }
  os << "}";

  os.flags(flags);
  os.fill(fill);

  return os;
}

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
