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

#ifndef DINGOFS_MDS_CLIENT_MDS_H_
#define DINGOFS_MDS_CLIENT_MDS_H_

#include <cstdint>
#include <string>

#include "dingofs/mds.pb.h"
#include "mds/client/interaction.h"
#include "mds/common/type.h"

namespace dingofs {
namespace mds {
namespace client {

using pb::mds::HeartbeatRequest;
using pb::mds::HeartbeatResponse;

using pb::mds::GetMDSListRequest;
using pb::mds::GetMDSListResponse;

using pb::mds::CreateFsRequest;
using pb::mds::CreateFsResponse;

using pb::mds::MountFsRequest;
using pb::mds::MountFsResponse;

using pb::mds::UmountFsRequest;
using pb::mds::UmountFsResponse;

using pb::mds::DeleteFsRequest;
using pb::mds::DeleteFsResponse;

using pb::mds::UpdateFsInfoRequest;
using pb::mds::UpdateFsInfoResponse;

using pb::mds::GetFsInfoRequest;
using pb::mds::GetFsInfoResponse;

using pb::mds::ListFsInfoRequest;
using pb::mds::ListFsInfoResponse;

using pb::mds::MkDirRequest;
using pb::mds::MkDirResponse;

using pb::mds::RmDirRequest;
using pb::mds::RmDirResponse;

using pb::mds::ReadDirRequest;
using pb::mds::ReadDirResponse;

using pb::mds::MkNodRequest;
using pb::mds::MkNodResponse;

using pb::mds::GetDentryRequest;
using pb::mds::GetDentryResponse;

using pb::mds::ListDentryRequest;
using pb::mds::ListDentryResponse;

using pb::mds::GetInodeRequest;
using pb::mds::GetInodeResponse;

using pb::mds::BatchGetInodeRequest;
using pb::mds::BatchGetInodeResponse;

using pb::mds::BatchGetXAttrRequest;
using pb::mds::BatchGetXAttrResponse;

using pb::mds::LookupRequest;
using pb::mds::LookupResponse;

using pb::mds::OpenRequest;
using pb::mds::OpenResponse;

using pb::mds::ReleaseRequest;
using pb::mds::ReleaseResponse;

using pb::mds::LinkRequest;
using pb::mds::LinkResponse;

using pb::mds::UnLinkRequest;
using pb::mds::UnLinkResponse;

using pb::mds::SymlinkRequest;
using pb::mds::SymlinkResponse;

using pb::mds::ReadLinkRequest;
using pb::mds::ReadLinkResponse;

using pb::mds::AllocSliceIdRequest;
using pb::mds::AllocSliceIdResponse;

using pb::mds::WriteSliceRequest;
using pb::mds::WriteSliceResponse;

using pb::mds::ReadSliceRequest;
using pb::mds::ReadSliceResponse;

using pb::mds::SetFsQuotaRequest;
using pb::mds::SetFsQuotaResponse;

using pb::mds::GetFsQuotaRequest;
using pb::mds::GetFsQuotaResponse;

using pb::mds::SetDirQuotaRequest;
using pb::mds::SetDirQuotaResponse;

using pb::mds::GetDirQuotaRequest;
using pb::mds::GetDirQuotaResponse;

using pb::mds::DeleteDirQuotaRequest;
using pb::mds::DeleteDirQuotaResponse;

using pb::mds::JoinFsRequest;
using pb::mds::JoinFsResponse;

using pb::mds::QuitFsRequest;
using pb::mds::QuitFsResponse;

// cache member
using pb::mds::JoinCacheGroupRequest;
using pb::mds::JoinCacheGroupResponse;

using pb::mds::LeaveCacheGroupRequest;
using pb::mds::LeaveCacheGroupResponse;

using pb::mds::ListGroupsRequest;
using pb::mds::ListGroupsResponse;

using pb::mds::ReweightMemberRequest;
using pb::mds::ReweightMemberResponse;

using pb::mds::ListMembersRequest;
using pb::mds::ListMembersResponse;

using pb::mds::UnLockMemberRequest;
using pb::mds::UnLockMemberResponse;

using pb::mds::DeleteMemberRequest;
using pb::mds::DeleteMemberResponse;

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
  UpdateFsInfoResponse UpdateFs(const std::string& fs_name, const pb::mds::FsInfo& fs_info);
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

  void UpdateFsS3Info(const std::string& fs_name, const S3Info& s3_info);
  void UpdateFsRadosInfo(const std::string& fs_name, const RadosInfo& rados_info);

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
}  // namespace mds
}  // namespace dingofs

#endif  // DINGOFS_MDS_CLIENT_MDS_H_