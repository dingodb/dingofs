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

#include "blockcache/infiniband/base/device.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "blockcache/common/status.h"
#include "blockcache/core/reactor/reactor.h"

namespace dingofs {
namespace blockcache {
namespace infiniband {

DEFINE_uint32(rdma_port_num, 1, "hca port to use");
DEFINE_validator(rdma_port_num, [](const char* /*name*/, uint32_t value) {
  return value > 0;
});

DEFINE_int32(rdma_gid_index, -1,
             "gid index; -1 auto-selects a RoCEv2 IPv4 gid (RoCE only)");

static uint8_t PortNum() { return static_cast<uint8_t>(FLAGS_rdma_port_num); }

static LinkLayer ToLinkLayer(uint8_t verbs_link_layer) {
  switch (verbs_link_layer) {
    case IBV_LINK_LAYER_INFINIBAND:
      return LinkLayer::kIB;
    case IBV_LINK_LAYER_ETHERNET:
      return LinkLayer::kEthernet;
    default:
      return LinkLayer::kUnspecified;
  }
}

const char* LinkLayerName(LinkLayer layer) {
  switch (layer) {
    case LinkLayer::kIB:
      return "IB";
    case LinkLayer::kEthernet:
      return "Ethernet";
    default:
      return "Unspecified";
  }
}

static std::string RegistryKey(const std::string& name) {
  const std::string shard =
      HasReactor() ? std::to_string(ThisShardId()) : "shared";
  return name + ":" + std::to_string(PortNum()) + ":" + shard;
}

static bool IsIpv4MappedGid(const ibv_gid& gid) {
  static const uint8_t kPrefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
  return std::memcmp(gid.raw, kPrefix, sizeof(kPrefix)) == 0;
}

static bool GidTypeIsRoceV2(const std::string& name, int index) {
  char path[256];
  (void)std::snprintf(path, sizeof(path),
                      "/sys/class/infiniband/%s/ports/%u/gid_attrs/types/%d",
                      name.c_str(), PortNum(), index);
  FILE* f = std::fopen(path, "r");
  if (f == nullptr) {
    return false;
  }
  absl::Cleanup defer = [f] { (void)std::fclose(f); };

  char line[64] = {};
  if (std::fgets(line, sizeof(line), f) == nullptr) {
    return false;
  }
  return std::strncmp(line, "RoCE v2", 7) == 0;
}

static int SelectGidIndex(ibv_context* context, const std::string& name) {
  if (FLAGS_rdma_gid_index >= 0) {
    return FLAGS_rdma_gid_index;
  }
  for (int i = 0; i < 16; ++i) {
    ibv_gid gid;
    if (ibv_query_gid(context, PortNum(), i, &gid) != 0) {
      continue;
    }
    if (IsIpv4MappedGid(gid) && GidTypeIsRoceV2(name, i)) {
      return i;
    }
  }
  return 0;
}

static Status QueryPort(ibv_context* context, const std::string& name,
                        PortInfo* port_info) {
  ibv_port_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  if (ibv_query_port(context, PortNum(), &attr) != 0) {
    PLOG(ERROR) << "Fail to query rdma port";
    return ToStatus(errno, "query rdma port");
  }

  port_info->port_num = PortNum();
  port_info->lid = attr.lid;
  port_info->active_mtu = attr.active_mtu;
  port_info->link_layer = ToLinkLayer(attr.link_layer);
  port_info->active = attr.state == IBV_PORT_ACTIVE;
  if (port_info->link_layer == LinkLayer::kEthernet) {
    port_info->gid_index = SelectGidIndex(context, name);
  } else {
    port_info->gid_index = 0;
  }
  if (ibv_query_gid(context, PortNum(), port_info->gid_index,
                    &port_info->gid) != 0) {
    PLOG(ERROR) << "Fail to query rdma gid";
    return ToStatus(errno, "query rdma gid");
  }
  return Status::OK();
}

static StatusOr<ibv_context*> OpenDevice(const std::string& name) {
  int num_devices = 0;
  ibv_device** devices = ibv_get_device_list(&num_devices);
  if (devices == nullptr) {
    PLOG(ERROR) << "Fail to get rdma device list";
    return ToStatus(errno, "get rdma device list");
  }
  absl::Cleanup defer = [devices] { ibv_free_device_list(devices); };

  for (int i = 0; i < num_devices; ++i) {
    if (name != ibv_get_device_name(devices[i])) {
      continue;
    }

    ibv_context* context = ibv_open_device(devices[i]);
    if (context == nullptr) {
      PLOG(ERROR) << "Fail to open rdma device " << name;
      return ToStatus(errno, "open rdma device");
    }

    return context;
  }

  LOG(ERROR) << "Fail to find rdma device " << name;
  return ToStatus(ENODEV, "find the requested rdma device");
}

StatusOr<Device*> Device::Open(std::string name) {
  static std::mutex mutex;
  static auto* registry = new std::map<std::string, Device*>();

  if (name.empty()) {
    LOG(ERROR) << "Fail to open rdma device: the device name is required";
    return ToStatus(EINVAL, "open rdma device: the device name is required");
  }

  std::lock_guard<std::mutex> guard(mutex);
  const std::string key = RegistryKey(name);
  auto it = registry->find(key);
  if (it != registry->end()) {
    return it->second;
  }

  // context
  StatusOr<ibv_context*> open = OpenDevice(name);
  if (!open.ok()) {
    return open.status();
  }
  ibv_context* context = open.value();
  absl::Cleanup defer = [context] { ibv_close_device(context); };

  // attr
  ibv_device_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  if (ibv_query_device(context, &attr) != 0) {
    PLOG(ERROR) << "Fail to query rdma device";
    return ToStatus(errno, "query rdma device");
  }

  // port info
  PortInfo port_info;
  Status status = QueryPort(context, name, &port_info);
  if (!status.ok()) {
    return status;
  }
  if (!port_info.active || port_info.link_layer == LinkLayer::kUnspecified) {
    LOG(ERROR) << "Fail to use rdma port: it is not active";
    return ToStatus(ENETDOWN, "use rdma port: it is not active");
  }

  // protect domain
  ibv_pd* pd = ibv_alloc_pd(context);
  if (pd == nullptr) {
    PLOG(ERROR) << "Fail to allocate protection domain";
    return ToStatus(errno, "allocate protection domain");
  }
  std::move(defer).Cancel();

  auto* device = new Device(std::move(name), context, pd, attr, port_info);
  LOG(INFO) << "Successfully open rdma device{name=" << device->name()
            << " port=" << static_cast<int>(port_info.port_num)
            << " link=" << LinkLayerName(port_info.link_layer)
            << " lid=" << port_info.lid << " gid_index=" << port_info.gid_index
            << " mtu=" << static_cast<int>(port_info.active_mtu)
            << " max_rd_atomic=" << attr.max_qp_rd_atom << "}";

  registry->emplace(key, device);
  return device;
}

Device::Device(std::string name, ibv_context* context, ibv_pd* pd,
               const ibv_device_attr& attr, const PortInfo& port_info)
    : name_(std::move(name)),
      context_(context),
      pd_(pd),
      attr_(attr),
      port_info_(port_info) {}

}  // namespace infiniband
}  // namespace blockcache
}  // namespace dingofs
