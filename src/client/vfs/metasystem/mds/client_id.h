// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_SRC_CLIENT_VFS_META_V2_CLIENT_ID_H_
#define DINGOFS_SRC_CLIENT_VFS_META_V2_CLIENT_ID_H_

#include <cstdint>
#include <string>

#include "client/vfs/metasystem/mds/helper.h"
#include "fmt/format.h"
#include "utils/uuid.h"

namespace dingofs {
namespace client {
namespace vfs {
namespace v2 {

class ClientId {
 public:
  ClientId(const std::string& hostname, uint32_t port,
           const std::string& mountpoint)
      : uuid_(utils::UUIDGenerator::GenerateUUID()),
        hostname_(hostname),
        ip_(Helper::HostName2IP(hostname)),
        port_(port),
        mountpoint_(mountpoint) {}
  ~ClientId() = default;

  std::string ID() const { return uuid_; }
  std::string Uuid() const { return uuid_; }
  std::string Hostname() const { return hostname_; }
  std::string IP() const { return ip_; }
  uint32_t Port() const { return port_; }
  std::string Mountpoint() const { return mountpoint_; }
  std::string Description() const {
    return fmt::format("ClientId[{}]: {}:{}{}", uuid_, hostname_, port_,
                       mountpoint_);
  }

  bool operator==(const ClientId& other) const { return uuid_ == other.uuid_; }
  bool operator!=(const ClientId& other) const { return uuid_ != other.uuid_; }
  bool operator<(const ClientId& other) const { return uuid_ < other.uuid_; }

 private:
  std::string uuid_;
  std::string hostname_;
  std::string ip_;
  uint32_t port_;
  std::string mountpoint_;
};

}  // namespace v2
}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_META_V2_CLIENT_ID_H_
