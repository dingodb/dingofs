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
 * Created Date: 2026-04-23
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_INFINIBAND_INFINIBAND_H_
#define DINGOFS_SRC_CACHE_INFINIBAND_INFINIBAND_H_

#include <infiniband/verbs.h>

#include <memory>
#include <ostream>
#include <unordered_map>

#include "common/status.h"
#include "dingofs/infiniband.pb.h"

namespace dingofs {
namespace cache {
namespace infiniband {

class Device;
class Port;
class ProtectDomain;
class MemoryRegion;
class QueuePair;
class CompletionQueue;

using DeviceUPtr = std::unique_ptr<Device>;
using PortUPtr = std::unique_ptr<Port>;
using ProtectDomainUPtr = std::unique_ptr<ProtectDomain>;
using MemoryRegionUPtr = std::unique_ptr<MemoryRegion>;
using QueuePairUPtr = std::unique_ptr<QueuePair>;
using CompletionQueueUPtr = std::unique_ptr<CompletionQueue>;

enum class LinkLayer : uint8_t {
  kUnspecified = 0,
  kEthernet = 1,
  kIB = 2,
};

enum class PortState : uint8_t {
  kUnknown = 0,
  kActive = 1,
  kDown = 2,
};

struct ConnManagementMeta {
  uint32_t qpn;
  uint16_t lid;
  ibv_gid gid;

  uint8_t port_num;
  LinkLayer link_type;
  ibv_mtu mtu;
};

class Port {
 public:
  Port(uint8_t port_num, ibv_port_attr port_attr, uint16_t lid, ibv_gid gid);
  static PortUPtr Query(Device* device, uint8_t port_num);

  uint8_t GetPortNum() const { return port_num_; }
  uint16_t GetLid() const { return lid_; }
  ibv_gid GetGid() const { return gid_; }
  ibv_mtu GetActiveMtu() const { return port_attr_.active_mtu; }
  PortState GetPortState() const;
  LinkLayer GetLinkLayer() const;

 private:
  uint8_t port_num_;
  ibv_port_attr port_attr_;
  uint16_t lid_;
  ibv_gid gid_;
};

class Device {
 public:
  Device(const std::string& name, ibv_context* context);
  static DeviceUPtr Open(const std::string& device_name);

  std::string GetName() const { return name_; }
  ibv_context* GetIbContext() const { return context_; }

 private:
  std::string name_;
  ibv_context* context_;
};

class ProtectDomain {
 public:
  explicit ProtectDomain(ibv_pd* pd);
  static ProtectDomainUPtr Alloc(Device* device);

  ibv_pd* GetIbPd() const { return pd_; }

 private:
  ibv_pd* pd_;
};

class MemoryRegion {
 public:
  explicit MemoryRegion(ibv_mr* mr);
  ~MemoryRegion();
  static MemoryRegionUPtr Register(ProtectDomain* protect_domain, void* addr,
                                   size_t length);

  uint32_t GetLkey() const { return mr_->lkey; }
  uint32_t GetRkey() const { return mr_->rkey; }
  ibv_mr* GetIbMr() const { return mr_; }

 private:
  ibv_mr* mr_;
};

class QueuePair {
 public:
  QueuePair(ibv_qp* qp, Device* device, Port* port,
            ProtectDomain* protect_domain);
  ~QueuePair();
  static QueuePairUPtr Create(Device* device, Port* port,
                              ProtectDomain* protect_domain,
                              CompletionQueue* completion_queue);

  Status ModifyQpToInit();
  Status ModifyQpToRtr(ConnManagementMeta remote_cm_meta);
  Status ModifyQpToRts();
  Status ModifyQpToError();

  ibv_qp* GetIbQp() const { return qp_; }
  uint32_t GetQpNum() const { return qp_->qp_num; }
  Device* GetDevice() const { return device_; }
  Port* GetPort() const { return port_; }
  ProtectDomain* GetProtectDomain() const { return protect_domain_; }
  ConnManagementMeta GetConnManagementMeta();

 private:
  ibv_qp* qp_;
  Device* device_;
  Port* port_;
  ProtectDomain* protect_domain_;
};

class CompletionQueue {
 public:
  CompletionQueue(ibv_cq* cq, ibv_comp_channel* channel);
  ~CompletionQueue();
  static CompletionQueueUPtr Create(Device* device);

  ibv_cq* GetIbCq() const { return cq_; }
  int GetFd() const { return channel_->fd; }
  bool GetCqEvent();
  bool RearmNotify();

 private:
  uint32_t num_unack_events_;
  ibv_cq* cq_;
  ibv_comp_channel* channel_;
};

class Infiniband {
 public:
  struct Context {
    Device* device;
    Port* port;
    ProtectDomain* protect_domain;
  };

  static Device* GetOrOpen(const std::string& device_name);
  static Port* GetOrQuery(Device* device, uint8_t port_num);
  static ProtectDomain* GetOrAlloc(Device* device);
  static Status Init(const std::string& device_name, uint8_t port_num,
                     Context* context);

 public:
  static std::unordered_map<std::string, DeviceUPtr> devices_;
  static std::unordered_map<std::string, PortUPtr> ports_;
  static std::unordered_map<std::string, ProtectDomainUPtr> protect_domains_;
};

void SerializeToPb(const ConnManagementMeta& cm_meta,
                   pb::infiniband::ConnManagementMeta* pb_cm_meta);
Status ParseFromPb(const pb::infiniband::ConnManagementMeta& pb_cm_meta,
                   ConnManagementMeta* cm_meta);

std::ostream& operator<<(std::ostream& os, const QueuePair& queue_pair);
std::ostream& operator<<(std::ostream& os, LinkLayer link_layer);
std::ostream& operator<<(std::ostream& os, const ConnManagementMeta& cm_meta);

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_INFINIBAND_INFINIBAND_H_
