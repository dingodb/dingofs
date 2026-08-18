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

#ifndef DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_DEVICE_H_
#define DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_DEVICE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <string>

#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

enum class LinkLayer : uint8_t {
  kUnspecified = 0,
  kEthernet = 1,  // RoCE: address by GID
  kIB = 2,        // InfiniBand: address by LID
};

const char* LinkLayerName(LinkLayer layer);

// Everything a QP needs to know about the local port; queried once at open.
struct PortInfo {
  uint8_t port_num = 1;
  uint16_t lid = 0;
  ibv_gid gid{};
  int gid_index = 0;
  ibv_mtu active_mtu = IBV_MTU_1024;
  LinkLayer link_layer = LinkLayer::kUnspecified;
  bool active = false;
};

struct DeviceOption {
  // Empty picks the first device with an active port.
  std::string name;
  uint8_t port_num = 1;
  // RoCE only; -1 auto-selects a RoCEv2 IPv4 GID, else index 0.
  int gid_index = -1;
};

// A device context + pd + port; one per shard so doorbells stay uncontended.
// MLX5_SINGLE_THREADED stays unset: it is process-global.
class Device {
 public:
  // Owner for contexts no shard owns: tests/tools on ordinary threads.
  static constexpr unsigned kAnyShard = ~0u;

  // Opens (or returns the open) device for `owner`; lives for the process.
  static StatusOr<Device*> Open(const DeviceOption& option, unsigned shard);

  ibv_context* context() const { return context_; }
  ibv_pd* pd() const { return pd_; }
  const std::string& name() const { return name_; }
  const PortInfo& port() const { return port_; }

  // Per-QP cap on outstanding READ/ATOMIC; the verbs default 1 serializes.
  uint8_t max_rd_atomic() const { return max_rd_atomic_; }
  uint32_t max_cqe() const { return max_cqe_; }

 private:
  Device() = default;

  std::string name_;
  ibv_context* context_ = nullptr;
  ibv_pd* pd_ = nullptr;
  PortInfo port_;
  uint8_t max_rd_atomic_ = 1;
  uint32_t max_cqe_ = 0;
};

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_CACHE_V2_CORE_NET_RDMA_VERBS_DEVICE_H_
