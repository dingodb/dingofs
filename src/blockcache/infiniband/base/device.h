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

#ifndef DINGOFS_BLOCKCACHE_INFINIBAND_BASE_DEVICE_H_
#define DINGOFS_BLOCKCACHE_INFINIBAND_BASE_DEVICE_H_

#include <infiniband/verbs.h>

#include <cstdint>
#include <string>

#include "blockcache/common/status.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

enum class LinkLayer : uint8_t {
  kUnspecified = 0,
  kEthernet = 1,
  kIB = 2,
};

const char* LinkLayerName(LinkLayer layer);

struct PortInfo {
  uint8_t port_num = 1;
  uint16_t lid = 0;
  ibv_gid gid{};
  int gid_index = 0;
  ibv_mtu active_mtu = IBV_MTU_1024;
  LinkLayer link_layer = LinkLayer::kUnspecified;
  bool active = false;
};

class Device {
 public:
  static StatusOr<Device*> Open(std::string name);

  Device(const Device&) = delete;
  Device& operator=(const Device&) = delete;

  const std::string& name() const { return name_; }
  ibv_context* context() const { return context_; }
  ibv_pd* pd() const { return pd_; }
  uint32_t max_cqe() const { return static_cast<uint32_t>(attr_.max_cqe); }
  const PortInfo& port_info() const { return port_info_; }

 private:
  Device(std::string name, ibv_context* context, ibv_pd* pd,
         const ibv_device_attr& attr, const PortInfo& port_info);

  std::string name_;
  ibv_context* context_ = nullptr;
  ibv_pd* pd_ = nullptr;
  ibv_device_attr attr_{};
  PortInfo port_info_;
};

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs

#endif  // DINGOFS_BLOCKCACHE_INFINIBAND_BASE_DEVICE_H_
