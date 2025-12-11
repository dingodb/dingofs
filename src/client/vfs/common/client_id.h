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

#ifndef DINGOFS_SRC_CLIENT_VFS_CLIENT_ID_H_
#define DINGOFS_SRC_CLIENT_VFS_CLIENT_ID_H_

#include <cstdint>
#include <string>

#include "common/helper.h"
#include "fmt/format.h"
#include "json/value.h"
#include "utils/time.h"

namespace dingofs {
namespace client {
namespace vfs {

class ClientId {
 public:
  ClientId() = default;
  ClientId(const std::string& uuid, const std::string& hostname, uint32_t port,
           const std::string& mountpoint)
      : uuid_(uuid),
        hostname_(hostname),
        ip_(Helper::HostName2IP(hostname)),
        port_(port),
        mountpoint_(mountpoint) {
    create_time_ms_ = utils::TimestampMs();
  }
  ~ClientId() = default;

  std::string ID() const { return uuid_; }
  std::string Uuid() const { return uuid_; }
  std::string Hostname() const { return hostname_; }
  std::string IP() const { return ip_; }
  uint32_t Port() const { return port_; }
  std::string Mountpoint() const { return mountpoint_; }
  uint64_t CreateTimeMs() const { return create_time_ms_; }

  std::string Description() const {
    return fmt::format("ClientId[{}]: {}:{}{}", uuid_, hostname_, port_,
                       mountpoint_);
  }

  bool Dump(Json::Value& value) const {
    Json::Value item = Json::objectValue;

    item["uuid"] = uuid_;
    item["hostname"] = hostname_;
    item["ip"] = ip_;
    item["port"] = port_;
    item["mountpoint"] = mountpoint_;
    item["create_time_ms"] = create_time_ms_;

    value["client_id"] = item;

    return true;
  }

  bool Load(const Json::Value& value) {
    if (!value.isMember("client_id")) return false;

    const Json::Value& item = value["client_id"];
    if (!item.isObject()) return false;

    uuid_ = item.get("uuid", "").asString();
    hostname_ = item.get("hostname", "").asString();
    ip_ = item.get("ip", "").asString();
    port_ = item.get("port", 0).asUInt();
    mountpoint_ = item.get("mountpoint", "").asString();
    create_time_ms_ = item.get("create_time_ms", 0).asUInt64();

    return true;
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
  uint64_t create_time_ms_{0};
};

}  // namespace vfs
}  // namespace client
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_VFS_CLIENT_ID_H_
