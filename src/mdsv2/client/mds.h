// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef DINGOFS_MDSV2_CLIENT_MDS_H_
#define DINGOFS_MDSV2_CLIENT_MDS_H_

#include <cstdint>
#include <string>

#include "dingofs/mdsv2.pb.h"
#include "mdsv2/client/interaction.h"
#include "mdsv2/common/type.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

using pb::mdsv2::HeartbeatRequest;
using pb::mdsv2::HeartbeatResponse;

using pb::mdsv2::GetMDSListRequest;
using pb::mdsv2::GetMDSListResponse;

using pb::mdsv2::CreateFsRequest;
using pb::mdsv2::CreateFsResponse;

using pb::mdsv2::MountFsRequest;
using pb::mdsv2::MountFsResponse;

using pb::mdsv2::UmountFsRequest;
using pb::mdsv2::UmountFsResponse;

using pb::mdsv2::DeleteFsRequest;
using pb::mdsv2::DeleteFsResponse;

using pb::mdsv2::UpdateFsInfoRequest;
using pb::mdsv2::UpdateFsInfoResponse;

using pb::mdsv2::GetFsInfoRequest;
using pb::mdsv2::GetFsInfoResponse;

using pb::mdsv2::ListFsInfoRequest;
using pb::mdsv2::ListFsInfoResponse;

using pb::mdsv2::MkDirRequest;
using pb::mdsv2::MkDirResponse;

using pb::mdsv2::RmDirRequest;
using pb::mdsv2::RmDirResponse;

using pb::mdsv2::ReadDirRequest;
using pb::mdsv2::ReadDirResponse;

using pb::mdsv2::MkNodRequest;
using pb::mdsv2::MkNodResponse;

using pb::mdsv2::GetDentryRequest;
using pb::mdsv2::GetDentryResponse;

using pb::mdsv2::ListDentryRequest;
using pb::mdsv2::ListDentryResponse;

using pb::mdsv2::GetInodeRequest;
using pb::mdsv2::GetInodeResponse;

using pb::mdsv2::BatchGetInodeRequest;
using pb::mdsv2::BatchGetInodeResponse;

using pb::mdsv2::BatchGetXAttrRequest;
using pb::mdsv2::BatchGetXAttrResponse;

using pb::mdsv2::LookupRequest;
using pb::mdsv2::LookupResponse;

using pb::mdsv2::OpenRequest;
using pb::mdsv2::OpenResponse;

using pb::mdsv2::ReleaseRequest;
using pb::mdsv2::ReleaseResponse;

using pb::mdsv2::LinkRequest;
using pb::mdsv2::LinkResponse;

using pb::mdsv2::UnLinkRequest;
using pb::mdsv2::UnLinkResponse;

using pb::mdsv2::SymlinkRequest;
using pb::mdsv2::SymlinkResponse;

using pb::mdsv2::ReadLinkRequest;
using pb::mdsv2::ReadLinkResponse;

using pb::mdsv2::AllocSliceIdRequest;
using pb::mdsv2::AllocSliceIdResponse;

using pb::mdsv2::WriteSliceRequest;
using pb::mdsv2::WriteSliceResponse;

using pb::mdsv2::ReadSliceRequest;
using pb::mdsv2::ReadSliceResponse;

using pb::mdsv2::SetFsQuotaRequest;
using pb::mdsv2::SetFsQuotaResponse;

using pb::mdsv2::GetFsQuotaRequest;
using pb::mdsv2::GetFsQuotaResponse;

using pb::mdsv2::SetDirQuotaRequest;
using pb::mdsv2::SetDirQuotaResponse;

using pb::mdsv2::GetDirQuotaRequest;
using pb::mdsv2::GetDirQuotaResponse;

using pb::mdsv2::DeleteDirQuotaRequest;
using pb::mdsv2::DeleteDirQuotaResponse;

using pb::mdsv2::JoinFsRequest;
using pb::mdsv2::JoinFsResponse;

using pb::mdsv2::QuitFsRequest;
using pb::mdsv2::QuitFsResponse;

// cache member
using pb::mdsv2::JoinCacheGroupRequest;
using pb::mdsv2::JoinCacheGroupResponse;

using pb::mdsv2::LeaveCacheGroupRequest;
using pb::mdsv2::LeaveCacheGroupResponse;

using pb::mdsv2::ListGroupsRequest;
using pb::mdsv2::ListGroupsResponse;

using pb::mdsv2::ReweightMemberRequest;
using pb::mdsv2::ReweightMemberResponse;

using pb::mdsv2::ListMembersRequest;
using pb::mdsv2::ListMembersResponse;

using pb::mdsv2::UnLockMemberRequest;
using pb::mdsv2::UnLockMemberResponse;

using pb::mdsv2::DeleteMemberRequest;
using pb::mdsv2::DeleteMemberResponse;

class MDSClient {
 public:
  MDSClient(uint32_t fs_id);
  ~MDSClient();

  bool Init(const std::string& mds_addr);

  void SetFsId(uint32_t fs_id) { fs_id_ = fs_id; }
  void SetEpoch(uint64_t epoch) { epoch_ = epoch; }

  HeartbeatResponse Heartbeat(uint32_t mds_id);

  GetMDSListResponse GetMdsList();

  struct CreateFsParams {
    uint32_t fs_id;
    std::string partition_type;
    uint32_t chunk_size;
    uint32_t block_size;
    uint32_t expect_mds_num;

    S3Info s3_info;
    RadosInfo rados_info;
    std::string owner = "deng";
  };

  CreateFsResponse CreateFs(const std::string& fs_name, const CreateFsParams& params);
  MountFsResponse MountFs(const std::string& fs_name, const std::string& client_id);
  UmountFsResponse UmountFs(const std::string& fs_name, const std::string& client_id);
  DeleteFsResponse DeleteFs(const std::string& fs_name, bool is_force);
  UpdateFsInfoResponse UpdateFs(const std::string& fs_name);
  GetFsInfoResponse GetFs(const std::string& fs_name);
  ListFsInfoResponse ListFs();

  MkDirResponse MkDir(Ino parent, const std::string& name);
  void BatchMkDir(const std::vector<int64_t>& parents, const std::string& prefix, size_t num);
  RmDirResponse RmDir(Ino parent, const std::string& name);
  ReadDirResponse ReadDir(Ino ino, const std::string& last_name, bool with_attr, bool is_refresh);

  MkNodResponse MkNod(Ino parent, const std::string& name);
  void BatchMkNod(const std::vector<int64_t>& parents, const std::string& prefix, size_t num);

  GetDentryResponse GetDentry(Ino parent, const std::string& name);
  ListDentryResponse ListDentry(Ino parent, bool is_only_dir);
  GetInodeResponse GetInode(Ino ino);
  BatchGetInodeResponse BatchGetInode(const std::vector<int64_t>& inos);
  BatchGetXAttrResponse BatchGetXattr(const std::vector<int64_t>& inos);

  void SetFsStats(const std::string& fs_name);
  void ContinueSetFsStats(const std::string& fs_name);
  void GetFsStats(const std::string& fs_name);
  void GetFsPerSecondStats(const std::string& fs_name);

  LookupResponse Lookup(Ino parent, const std::string& name);

  OpenResponse Open(Ino ino);
  ReleaseResponse Release(Ino ino, const std::string& session_id);

  LinkResponse Link(Ino ino, Ino new_parent, const std::string& new_name);
  UnLinkResponse UnLink(Ino parent, const std::string& name);
  SymlinkResponse Symlink(Ino parent, const std::string& name, const std::string& symlink);
  ReadLinkResponse ReadLink(Ino ino);

  AllocSliceIdResponse AllocSliceId(uint32_t alloc_num, uint64_t min_slice_id);
  WriteSliceResponse WriteSlice(Ino ino, int64_t chunk_index);
  ReadSliceResponse ReadSlice(Ino ino, int64_t chunk_index);

  // quota operations
  SetFsQuotaResponse SetFsQuota(const QuotaEntry& quota);
  GetFsQuotaResponse GetFsQuota();
  SetDirQuotaResponse SetDirQuota(Ino ino, const QuotaEntry& quota);
  GetDirQuotaResponse GetDirQuota(Ino ino);
  DeleteDirQuotaResponse DeleteDirQuota(Ino ino);

  JoinFsResponse JoinFs(const std::string& fs_name, uint32_t fs_id, const std::vector<int64_t>& mds_ids);
  QuitFsResponse QuitFs(const std::string& fs_name, uint32_t fs_id, const std::vector<int64_t>& mds_ids);

  // cache member operations
  JoinCacheGroupResponse JoinCacheGroup(const std::string& member_id, const std::string& ip, uint32_t port,
                                        const std::string& group_name, uint32_t weight);
  LeaveCacheGroupResponse LeaveCacheGroup(const std::string& member_id, const std::string& ip, uint32_t port,
                                          const std::string& group_name);
  ListGroupsResponse ListGroups();
  ReweightMemberResponse ReweightMember(const std::string& member_id, const std::string& ip, uint32_t port,
                                        uint32_t weight);
  ListMembersResponse ListMembers(const std::string& group_name);
  UnLockMemberResponse UnlockMember(const std::string& member_id, const std::string& ip, uint32_t port);
  DeleteMemberResponse DeleteMember(const std::string& member_id);

 private:
  uint32_t fs_id_{0};
  uint64_t epoch_{0};
  InteractionPtr interaction_;
};

class MdsCommandRunner {
 public:
  MdsCommandRunner() = default;
  ~MdsCommandRunner() = default;

  struct Options {
    uint32_t fs_id{0};
    Ino ino;
    Ino parent;
    std::string parents;
    std::string name;
    std::string fs_name;
    std::string prefix;
    std::string mds_id_list;
    uint32_t num;
    uint64_t max_bytes;
    uint64_t max_inodes;
    bool is_force{false};

    std::string fs_partition_type;
    uint32_t chunk_size;
    uint32_t block_size;

    // cache member
    std::string member_id;
    std::string ip;
    uint32_t port;
    std::string group_name;
    uint32_t weight;

    S3Info s3_info;
    RadosInfo rados_info;
  };

  static bool Run(const Options& options, const std::string& mds_addr, const std::string& cmd, uint32_t fs_id);
};

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs

#endif  // DINGOFS_MDSV2_CLIENT_MDS_H_