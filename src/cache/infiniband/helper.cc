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
 * Created Date: 2026-05-28
 * Author: Jingli Chen (Wine93)
 */

#include "cache/infiniband/helper.h"

#include <fmt/format.h>
#include <glog/logging.h>
#include <infiniband/verbs.h>

#include <cstring>

#include "cache/infiniband/infiniband.h"

namespace dingofs {
namespace cache {
namespace infiniband {

static std::unordered_map<std::string, MemoryRegionUPtr> g_usr_mrs;

Status RegisterMemoryForRdma(const std::string& device_name, void* addr,
                             size_t length) {
  auto key = fmt::format("{}:{}", device_name, addr);
  if (g_usr_mrs.count(key) != 0) {
    return Status::Exist("memory already registerd");
  }

  auto* device = Infiniband::GetOrOpen(device_name);
  if (device == nullptr) {
    return Status::Internal("open device failed");
  }

  auto* protect_domain = Infiniband::GetOrAlloc(device);
  if (protect_domain == nullptr) {
    return Status::Internal("alloc protect domain failed");
  }

  auto memory_region = MemoryRegion::Register(protect_domain, addr, length);
  if (memory_region == nullptr) {
    PLOG(ERROR) << "Fail to register memory region";
    return Status::Internal("register memory region failed");
  }

  g_usr_mrs.emplace(key, std::move(memory_region));
  return Status::OK();
}

Status DeregisterMemoryForRdma(const std::string& device_name, void* addr) {
  auto key = fmt::format("{}:{}", device_name, addr);
  if (g_usr_mrs.count(key) == 0) {
    return Status::NotFound("memory region not found");
  }

  g_usr_mrs.erase(key);
  return Status::OK();
}

void Helper::SerializeToPb(const ConnManagmentMeta& cm_meta,
                           pb::infiniband::ConnManagementMeta* pb_cm_meta) {
  pb_cm_meta->set_qpn(cm_meta.qpn);
  pb_cm_meta->set_lid(cm_meta.lid);
  pb_cm_meta->set_gid(&cm_meta.gid, sizeof(cm_meta.gid));
  pb_cm_meta->set_port_num(cm_meta.port_num);
  pb_cm_meta->set_link_type(static_cast<uint32_t>(cm_meta.link_type));
  pb_cm_meta->set_mtu(static_cast<uint32_t>(cm_meta.mtu));
}

Status Helper::ParseFromPb(const pb::infiniband::ConnManagementMeta& pb_cm_meta,
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

}  // namespace infiniband
}  // namespace cache
}  // namespace dingofs
