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

#include "cache/v2/core/net/rdma/verbs/device.h"

#include <glog/logging.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <map>
#include <mutex>
#include <string>

#include "absl/cleanup/cleanup.h"
#include "cache/v2/common/status.h"

namespace dingofs {
namespace cache {
namespace v2 {
namespace verbs {

// MLX5_SINGLE_THREADED must stay unset: it is process-global, and shared
// doorbells then corrupt work requests (IBV_WC_REM_ACCESS_ERR, wedged conn).

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

static bool IsIpv4MappedGid(const ibv_gid& gid) {
  static const uint8_t kPrefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff};
  return std::memcmp(gid.raw, kPrefix, sizeof(kPrefix)) == 0;
}

static bool GidTypeIsRoceV2(const std::string& dev, uint8_t port_num,
                            int index) {
  char path[256];
  (void)std::snprintf(path, sizeof(path),
                      "/sys/class/infiniband/%s/ports/%u/gid_attrs/types/%d",
                      dev.c_str(), port_num, index);
  FILE* f = std::fopen(path, "r");
  if (f == nullptr) {
    return false;
  }
  absl::Cleanup close_file = [f] { (void)std::fclose(f); };

  char line[64] = {};
  if (std::fgets(line, sizeof(line), f) == nullptr) {
    return false;
  }
  return std::strncmp(line, "RoCE v2", 7) == 0;
}

// RoCE only; prefer RoCEv2+IPv4-mapped: a wrong GID silently drops packets.
static int SelectGidIndex(ibv_context* ctx, const std::string& dev,
                          uint8_t port_num, int requested) {
  if (requested >= 0) {
    return requested;
  }
  for (int i = 0; i < 16; ++i) {
    ibv_gid gid;
    if (ibv_query_gid(ctx, port_num, i, &gid) != 0) {
      continue;
    }
    if (IsIpv4MappedGid(gid) && GidTypeIsRoceV2(dev, port_num, i)) {
      return i;
    }
  }
  return 0;
}

static Status QueryPort(ibv_context* ctx, const std::string& dev,
                        const DeviceOption& option, PortInfo* out) {
  ibv_port_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  if (ibv_query_port(ctx, option.port_num, &attr) != 0) {
    return ToStatus(errno, "query rdma port");
  }

  out->port_num = option.port_num;
  out->lid = attr.lid;
  out->active_mtu = attr.active_mtu;
  out->link_layer = ToLinkLayer(attr.link_layer);
  out->active = attr.state == IBV_PORT_ACTIVE;
  out->gid_index =
      out->link_layer == LinkLayer::kEthernet
          ? SelectGidIndex(ctx, dev, option.port_num, option.gid_index)
          : 0;
  if (ibv_query_gid(ctx, option.port_num, out->gid_index, &out->gid) != 0) {
    return ToStatus(errno, "query rdma gid");
  }
  return Status::OK();
}

// Picks `option.name`, or the first device whose port_num is ACTIVE.
static StatusOr<ibv_context*> OpenContext(const DeviceOption& option,
                                          std::string* name_out,
                                          PortInfo* port_out) {
  int count = 0;
  ibv_device** list = ibv_get_device_list(&count);
  if (list == nullptr) {
    return ToStatus(errno, "get rdma device list");
  }
  absl::Cleanup free_list = [list] { ibv_free_device_list(list); };

  for (int i = 0; i < count; ++i) {
    std::string name = ibv_get_device_name(list[i]);
    if (!option.name.empty() && name != option.name) {
      continue;
    }

    ibv_context* ctx = ibv_open_device(list[i]);
    if (ctx == nullptr) {
      if (!option.name.empty()) {
        return ToStatus(errno, "open rdma device");
      }
      continue;
    }

    PortInfo port;
    Status status = QueryPort(ctx, name, option, &port);
    if (!status.ok()) {
      ibv_close_device(ctx);
      if (!option.name.empty()) {
        return status;
      }
      continue;
    }

    // A named device must be usable; an auto-picked one may be skipped.
    if (!port.active || port.link_layer == LinkLayer::kUnspecified) {
      ibv_close_device(ctx);
      if (!option.name.empty()) {
        return ToStatus(ENETDOWN, "use rdma port: it is not active");
      }
      continue;
    }

    *name_out = std::move(name);
    *port_out = port;
    return ctx;
  }

  return ToStatus(ENODEV, option.name.empty()
                                 ? "find any active rdma device"
                                 : "find the requested rdma device");
}

static std::string RegistryKey(const DeviceOption& option, unsigned shard) {
  return option.name + ":" + std::to_string(option.port_num) + ":" +
         (shard == Device::kAnyShard ? "shared" : std::to_string(shard));
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

StatusOr<Device*> Device::Open(const DeviceOption& option, unsigned shard) {
  // Setup path only: one mutex, never touched on the data path.
  static std::mutex mutex;
  static std::map<std::string, Device*>* registry =
      new std::map<std::string, Device*>();

  std::lock_guard<std::mutex> guard(mutex);
  const std::string key = RegistryKey(option, shard);
  auto it = registry->find(key);
  if (it != registry->end()) {
    return it->second;
  }

  // Never freed: pd teardown during static destruction races the provider.
  auto* device = new Device();
  StatusOr<ibv_context*> context =
      OpenContext(option, &device->name_, &device->port_);
  if (!context.ok()) {
    delete device;
    return context.status();
  }
  device->context_ = context.value();

  device->pd_ = ibv_alloc_pd(device->context_);
  if (device->pd_ == nullptr) {
    Status status = ToStatus(errno, "allocate protection domain");
    ibv_close_device(device->context_);
    delete device;
    return status;
  }

  ibv_device_attr attr;
  std::memset(&attr, 0, sizeof(attr));
  if (ibv_query_device(device->context_, &attr) != 0) {
    Status status = ToStatus(errno, "query rdma device");
    ibv_dealloc_pd(device->pd_);
    ibv_close_device(device->context_);
    delete device;
    return status;
  }
  device->max_rd_atomic_ = static_cast<uint8_t>(
      attr.max_qp_rd_atom > 255 ? 255 : attr.max_qp_rd_atom);
  device->max_cqe_ = static_cast<uint32_t>(attr.max_cqe);

  LOG(INFO) << "Successfully open rdma device: name=" << device->name_
            << " port=" << static_cast<int>(device->port_.port_num)
            << " link=" << LinkLayerName(device->port_.link_layer)
            << " lid=" << device->port_.lid
            << " gid_index=" << device->port_.gid_index
            << " mtu=" << static_cast<int>(device->port_.active_mtu)
            << " max_rd_atomic=" << static_cast<int>(device->max_rd_atomic_);

  registry->emplace(key, device);
  return device;
}

}  // namespace verbs
}  // namespace v2
}  // namespace cache
}  // namespace dingofs
