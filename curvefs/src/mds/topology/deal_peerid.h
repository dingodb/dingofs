/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * Project: curve
 * Created Date: 2021-09-23
 * Author: wanghai01
 */
#ifndef CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_
#define CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_

#include <string>
#include <vector>

#include "braft/configuration.h"
#include "butil/endpoint.h"
#include "src/common/string_util.h"

namespace curvefs {
namespace mds {
namespace topology {

using curve::common::SplitString;
using curve::common::StringToUl;

/**
 * @brief the peerid format is ip:port:index, and be used in braft.
 */
inline std::string BuildPeerIdWithIpPort(const std::string& ip, uint32_t port,
                                         uint32_t idx = 0) {
  return ip + ":" + std::to_string(port) + ":" + std::to_string(idx);
}

inline std::string BuildPeerIdWithAddr(const std::string& addr,
                                       uint32_t idx = 0) {
  return addr + ":" + std::to_string(idx);
}

inline bool SplitPeerId(const std::string& peer_id, std::string* ip,
                        uint32_t* port, uint32_t* idx = nullptr) {
  braft::PeerId node;
  if (node.parse(peer_id) != 0) {
    LOG(ERROR) << "parse peer_id failed: " << peer_id;
    return false;
  }

  *ip = butil::ip2str(node.addr.ip).c_str();
  *port = node.addr.port;

  if (idx != nullptr) {
    *idx = node.idx;
  }

  return true;
}

inline bool SplitPeerId(const std::string& peer_id, std::string* addr) {
  braft::PeerId node;
  if (node.parse(peer_id) != 0) {
    LOG(ERROR) << "parse peer_id failed: " << peer_id;
    return false;
  }

  *addr = butil::endpoint2str(node.addr).c_str();

  return true;
}

inline bool SplitAddrToIpPort(const std::string& addr, std::string* ipstr,
                              uint32_t* port) {
  std::vector<std::string> items;
  curve::common::SplitString(addr, ":", &items);
  if (2 == items.size() && curve::common::StringToUl(items[1], port)) {
    *ipstr = items[0];
    return true;
  }
  return false;
}

}  // namespace topology
}  // namespace mds
}  // namespace curvefs

#endif  // CURVEFS_SRC_MDS_TOPOLOGY_DEAL_PEERID_H_
