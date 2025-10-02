/*
 * Copyright (c) 2025 dingodb.com, Inc. All Rights Reserved
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
 * Created Date: 2025-08-18
 * Author: Jingli Chen (Wine93)
 */

#ifndef DINGOFS_SRC_CACHE_COMMON_MDS_CLIENT_H_
#define DINGOFS_SRC_CACHE_COMMON_MDS_CLIENT_H_

#include <cstdint>
#include <string>

#include "client/vfs/meta/v2/mds_discovery.h"
#include "client/vfs/meta/v2/rpc.h"
#include "common/status.h"
#include "dingofs/common.pb.h"
#include "dingofs/mds.pb.h"
#include "mds/mds/mds_meta.h"

namespace dingofs {
namespace cache {

enum class CacheGroupMemberState : uint8_t {
  kUnknown = 0,
  kOnline = 1,
  kUnstable = 2,
  kOffline = 3,
};

inline std::string CacheGroupMemberStateToString(CacheGroupMemberState state) {
  switch (state) {
    case CacheGroupMemberState::kUnknown:
      return "unknown";
    case CacheGroupMemberState::kOnline:
      return "online";
    case CacheGroupMemberState::kUnstable:
      return "unstable";
    case CacheGroupMemberState::kOffline:
      return "offline";
    default:
      CHECK(false) << "unknown cache group member state: "
                   << static_cast<uint8_t>(state);
  }
}

struct CacheGroupMember {
  std::string id;
  std::string ip;
  uint32_t port;
  uint32_t weight;
  CacheGroupMemberState state;

  bool operator==(const CacheGroupMember& other) const {
    return id == other.id && ip == other.ip && port == other.port &&
           weight == other.weight && state == other.state;
  }

  std::string ToString() const {
    return absl::StrFormat(
        "[id = %s, ip = %s, port = %u, weight = %u, state = %s]", id, ip, port,
        weight, CacheGroupMemberStateToString(state));
  }
};

class MDSClient {
 public:
  virtual ~MDSClient() = default;

  virtual Status Start() = 0;
  virtual Status Shutdown() = 0;

  virtual Status GetFSInfo(uint64_t fs_id, pb::mds::FsInfo* fs_info) = 0;

  virtual Status JoinCacheGroup(const std::string& want_id,
                                const std::string& ip, uint32_t port,
                                const std::string& group_name, uint32_t weight,
                                std::string* member_id) = 0;
  virtual Status LeaveCacheGroup(const std::string& member_id,
                                 const std::string& ip, uint32_t port,
                                 const std::string& group_name) = 0;
  virtual Status Heartbeat(const std::string& member_id, const std::string& ip,
                           uint32_t port) = 0;
  virtual Status ListMembers(const std::string& group_name,
                             std::vector<CacheGroupMember>* members) = 0;
};

using MDSClientSPtr = std::shared_ptr<MDSClient>;
using MDSClientUPtr = std::unique_ptr<MDSClient>;

class MDSClientImpl : public MDSClient {
 public:
  explicit MDSClientImpl(const std::string& mds_addr);

  Status Start() override;
  Status Shutdown() override;

  Status GetFSInfo(uint64_t fs_id, pb::mds::FsInfo* fs_info) override;

  Status JoinCacheGroup(const std::string& want_id, const std::string& ip,
                        uint32_t port, const std::string& group_name,
                        uint32_t weight, std::string* member_id) override;
  Status LeaveCacheGroup(const std::string& member_id, const std::string& ip,
                         uint32_t port, const std::string& group_name) override;
  Status Heartbeat(const std::string& member_id, const std::string& ip,
                   uint32_t port) override;
  Status ListMembers(const std::string& group_name,
                     std::vector<CacheGroupMember>* members) override;

 private:
  CacheGroupMemberState ToMemberState(pb::mds::CacheGroupMemberState state);

  mds::MDSMeta GetRandomlyMDS(const mds::MDSMeta& old_mds);
  bool ShouldRetry(Status status);
  bool ShouldSetMDSAbormal(Status status);
  bool ShouldRefreshMDSList(Status status);

  template <typename Request, typename Response>
  Status SendRequest(const std::string& service_name,
                     const std::string& api_name, Request& request,
                     Response& response);

  std::atomic<bool> running_;
  std::shared_ptr<client::vfs::v2::RPC> rpc_;
  std::unique_ptr<client::vfs::v2::MDSDiscovery> mds_discovery_;
};

}  // namespace cache
}  // namespace dingofs

#endif  // DINGOFS_SRC_CACHE_COMMON_MDS_CLIENT_H_
