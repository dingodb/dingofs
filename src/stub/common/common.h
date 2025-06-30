/*
 *  Copyright (c) 2020 NetEase Inc.
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
 * Project: dingo
 * File Created: Tuesday, 18th September 2018 3:24:40 pm
 * Author: tongguangxun
 */

#ifndef SRC_STUB_COMMON_COMMON_H_
#define SRC_STUB_COMMON_COMMON_H_

#include <butil/endpoint.h>
#include <butil/status.h>
#include <google/protobuf/stubs/callback.h>

#include <string>

namespace dingofs {
namespace stub {
namespace common {

using ChunkID = uint64_t;
using CopysetID = uint32_t;
using LogicPoolID = uint32_t;
using ChunkServerID = uint32_t;
using ChunkIndex = uint32_t;
using MetaserverID = uint32_t;
using PartitionID = uint32_t;

using EndPoint = butil::EndPoint;
using Status = butil::Status;

using IOManagerID = uint64_t;

constexpr uint64_t KiB = 1024;
constexpr uint64_t MiB = 1024 * KiB;
constexpr uint64_t GiB = 1024 * MiB;

// PeerAddr 代表一个copyset group里的一个chunkserver节点
// 与braft中的PeerID对应
struct PeerAddr {
  // 节点的地址信息
  EndPoint addr_;

  PeerAddr() = default;
  explicit PeerAddr(butil::EndPoint addr) : addr_(addr) {}

  bool IsEmpty() const {
    return (addr_.ip == butil::IP_ANY && addr_.port == 0) &&
           !butil::is_endpoint_extended(addr_);
  }

  // 重置当前地址信息
  void Reset() {
    addr_.ip = butil::IP_ANY;
    addr_.port = 0;
  }

  // 从字符串中将地址信息解析出来
  int Parse(const std::string& str) {
    int idx;
    char ip_str[64];
    if (2 >
        sscanf(str.c_str(), "%[^:]%*[:]%d%*[:]%d", ip_str, &addr_.port, &idx)) {
      Reset();
      return -1;
    }
    int ret = butil::str2ip(ip_str, &addr_.ip);
    if (0 != ret) {
      Reset();
      return -1;
    }
    return 0;
  }

  // 将该节点地址信息转化为字符串形式
  // 在get leader调用中可以将该值直接传入request
  std::string ToString() const {
    char str[128];
    snprintf(str, sizeof(str), "%s:%d", butil::endpoint2str(addr_).c_str(), 0);
    return std::string(str);
  }

  bool operator==(const PeerAddr& other) const { return addr_ == other.addr_; }
};

enum class MetaServerOpType {
  GetDentry,
  ListDentry,
  CreateDentry,
  DeleteDentry,
  PrepareRenameTx,
  GetInode,
  BatchGetInodeAttr,
  BatchGetXAttr,
  UpdateInode,
  CreateInode,
  DeleteInode,
  GetOrModifyS3ChunkInfo,
  GetVolumeExtent,
  UpdateVolumeExtent,
  CreateManageInode,
  GetFsQuota,
  FlushFsUsage,
  LoadDirQutoas,
  FlushDirUsages,
};

std::ostream& operator<<(std::ostream& os, MetaServerOpType optype);

}  // namespace common
}  // namespace stub
}  // namespace dingofs

#endif  // SRC_STUB_COMMON_COMMON_H_
