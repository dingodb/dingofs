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

#include "mdsv2/client/mds.h"

#include <fcntl.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "fmt/format.h"
#include "mdsv2/common/helper.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

bool MDSClient::Init(const std::string& mds_addr) {
  interaction_ = dingofs::mdsv2::client::Interaction::New();
  return interaction_->Init(mds_addr);
}

HeartbeatResponse MDSClient::Heartbeat(uint32_t mds_id) {
  HeartbeatRequest request;
  HeartbeatResponse response;

  request.set_role(pb::mdsv2::Role::ROLE_MDS);
  auto* mds = request.mutable_mds();
  mds->set_id(mds_id);
  mds->mutable_location()->set_host("127.0.0.1");
  mds->mutable_location()->set_port(10000);
  mds->set_state(MdsEntry::NORMAL);
  mds->set_last_online_time_ms(Helper::TimestampMs());

  interaction_->SendRequest("MDSService", "Heartbeat", request, response);

  return response;
}

GetMDSListResponse MDSClient::GetMdsList() {
  GetMDSListRequest request;
  GetMDSListResponse response;

  interaction_->SendRequest("MDSService", "GetMDSList", request, response);
  for (const auto& mds : response.mdses()) {
    DINGO_LOG(INFO) << "mds: " << mds.ShortDebugString();
  }

  return response;
}

CreateFsResponse MDSClient::CreateFs(const std::string& fs_name, const CreateFsParams& params) {
  CreateFsRequest request;
  CreateFsResponse response;

  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return response;
  }

  if (params.s3_endpoint.empty() || params.s3_ak.empty() || params.s3_sk.empty() || params.s3_bucketname.empty()) {
    DINGO_LOG(ERROR) << "s3 info is empty";
    return response;
  }

  if (params.chunk_size == 0) {
    DINGO_LOG(ERROR) << "chunk_size is 0";
    return response;
  }
  if (params.block_size == 0) {
    DINGO_LOG(ERROR) << "block_size is 0";
    return response;
  }

  request.set_fs_name(fs_name);
  request.set_block_size(params.block_size);
  request.set_chunk_size(params.chunk_size);

  request.set_fs_type(pb::mdsv2::FsType::S3);
  request.set_owner(params.owner);
  request.set_capacity(1024 * 1024 * 1024);
  request.set_recycle_time_hour(24);

  if (params.partition_type == "mono") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION);
  } else if (params.partition_type == "parent_hash") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION);
  }

  pb::mdsv2::S3Info s3_info;
  s3_info.set_ak(params.s3_ak);
  s3_info.set_sk(params.s3_sk);
  s3_info.set_endpoint(params.s3_endpoint);
  s3_info.set_bucketname(params.s3_bucketname);

  s3_info.set_object_prefix(0);

  *request.mutable_fs_extra()->mutable_s3_info() = s3_info;

  DINGO_LOG(INFO) << "CreateFs request: " << request.ShortDebugString();

  auto status = interaction_->SendRequest("MDSService", "CreateFs", request, response);
  if (status.ok()) {
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      DINGO_LOG(INFO) << "CreateFs success, fs_id: " << response.fs_info().fs_id();
    } else {
      DINGO_LOG(ERROR) << "CreateFs fail, error: " << response.ShortDebugString();
    }
  }

  return response;
}

// message MountPoint {
//   string client_id = 1;
//   string hostname = 2;
//   uint32 port = 3;
//   string path = 4;
//   bool cto = 5;
// }

MountFsResponse MDSClient::MountFs(const std::string& fs_name, const std::string& client_id) {
  MountFsRequest request;
  MountFsResponse response;

  request.set_fs_name(fs_name);
  auto* mountpoint = request.mutable_mount_point();
  mountpoint->set_client_id(client_id);
  mountpoint->set_hostname("127.0.0.1");
  mountpoint->set_port(10000);
  mountpoint->set_path("/mnt/dingo");

  interaction_->SendRequest("MDSService", "MountFs", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "MountFs success";
  } else {
    DINGO_LOG(ERROR) << "MountFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

UmountFsResponse MDSClient::UmountFs(const std::string& fs_name, const std::string& client_id) {
  UmountFsRequest request;
  UmountFsResponse response;

  request.set_fs_name(fs_name);
  auto* mountpoint = request.mutable_mount_point();
  mountpoint->set_client_id(client_id);
  mountpoint->set_hostname("127.0.0.1");
  mountpoint->set_port(10000);
  mountpoint->set_path("/mnt/dingo");

  interaction_->SendRequest("MDSService", "UmountFs", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "MountFs success";
  } else {
    DINGO_LOG(ERROR) << "MountFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

DeleteFsResponse MDSClient::DeleteFs(const std::string& fs_name, bool is_force) {
  DeleteFsRequest request;
  DeleteFsResponse response;

  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return response;
  }

  request.set_fs_name(fs_name);
  request.set_is_force(is_force);

  DINGO_LOG(INFO) << "DeleteFs request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "DeleteFs", request, response);

  DINGO_LOG(INFO) << "DeleteFs response: " << response.ShortDebugString();

  return response;
}

UpdateFsInfoResponse MDSClient::UpdateFs(const std::string& fs_name) {
  UpdateFsInfoRequest request;
  UpdateFsInfoResponse response;

  request.set_fs_name(fs_name);

  pb::mdsv2::FsInfo fs_info;
  fs_info.set_owner("deng");
  request.mutable_fs_info()->CopyFrom(fs_info);

  interaction_->SendRequest("MDSService", "UpdateFsInfo", request, response);

  return response;
}

GetFsInfoResponse MDSClient::GetFs(const std::string& fs_name) {
  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return {};
  }

  GetFsInfoRequest request;
  GetFsInfoResponse response;

  request.set_fs_name(fs_name);

  DINGO_LOG(INFO) << "GetFsInfo request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "GetFsInfo", request, response);

  DINGO_LOG(INFO) << "GetFsInfo response: " << response.ShortDebugString();

  return response;
}

ListFsInfoResponse MDSClient::ListFs() {
  ListFsInfoRequest request;
  ListFsInfoResponse response;

  interaction_->SendRequest("MDSService", "ListFsInfo", request, response);

  for (const auto& fs_info : response.fs_infos()) {
    DINGO_LOG(INFO) << "fs_info: " << fs_info.ShortDebugString();
  }

  return response;
}

RefreshFsInfoResponse MDSClient::RefreshFsInfo(const std::string& fs_name) {
  RefreshFsInfoRequest request;
  RefreshFsInfoResponse response;

  request.set_fs_name(fs_name);

  interaction_->SendRequest("MDSService", "RefreshFsInfo", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "RefreshFsInfo success";
  } else {
    DINGO_LOG(ERROR) << "RefreshFsInfo fail, error: " << response.ShortDebugString();
  }

  return response;
}

MkDirResponse MDSClient::MkDir(uint32_t fs_id, Ino parent, const std::string& name) {
  MkDirRequest request;
  MkDirResponse response;

  request.set_fs_id(fs_id);
  request.set_parent(parent);
  request.set_name(name);
  request.set_length(4096);
  request.set_uid(0);
  request.set_gid(0);
  request.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  request.set_rdev(0);

  interaction_->SendRequest("MDSService", "MkDir", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "MkDir success, ino: " << response.inode().ino();
  } else {
    DINGO_LOG(ERROR) << "MkDir fail, error: " << response.ShortDebugString();
  }

  return response;
}

void MDSClient::BatchMkDir(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkDir(fs_id, parent, name);
    }
  }
}

RmDirResponse MDSClient::RmDir(Ino parent, const std::string& name) {
  RmDirRequest request;
  RmDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "RmDir", request, response);

  return response;
}

ReadDirResponse MDSClient::ReadDir(Ino ino, const std::string& last_name, bool with_attr, bool is_refresh) {
  ReadDirRequest request;
  ReadDirResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(100);
  request.set_with_attr(with_attr);
  request.set_is_refresh(is_refresh);

  interaction_->SendRequest("MDSService", "ReadDir", request, response);

  return response;
}

MkNodResponse MDSClient::MkNod(uint32_t fs_id, Ino parent, const std::string& name) {
  MkNodRequest request;
  MkNodResponse response;

  request.set_fs_id(fs_id);
  request.set_parent(parent);
  request.set_name(name);
  request.set_length(0);
  request.set_uid(0);
  request.set_gid(0);
  request.set_mode(S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP | S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  request.set_rdev(0);

  interaction_->SendRequest("MDSService", "MkNod", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "MkNode success, ino: " << response.inode().ino();
  } else {
    DINGO_LOG(ERROR) << "MkNode fail, error: " << response.ShortDebugString();
  }

  return response;
}

void MDSClient::BatchMkNod(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkNod(fs_id, parent, name);
    }
  }
}

GetDentryResponse MDSClient::GetDentry(uint32_t fs_id, Ino parent, const std::string& name) {
  GetDentryRequest request;
  GetDentryResponse response;

  request.set_fs_id(fs_id);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "GetDentry", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "dentry: " << response.dentry().ShortDebugString();
  }

  return response;
}

ListDentryResponse MDSClient::ListDentry(uint32_t fs_id, Ino parent, bool is_only_dir) {
  ListDentryRequest request;
  ListDentryResponse response;

  request.set_fs_id(fs_id);
  request.set_parent(parent);
  request.set_is_only_dir(is_only_dir);

  interaction_->SendRequest("MDSService", "ListDentry", request, response);

  for (const auto& dentry : response.dentries()) {
    DINGO_LOG(INFO) << "dentry: " << dentry.ShortDebugString();
  }

  return response;
}

GetInodeResponse MDSClient::GetInode(uint32_t fs_id, Ino ino) {
  GetInodeRequest request;
  GetInodeResponse response;

  request.set_fs_id(fs_id);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "GetInode", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "inode: " << response.inode().ShortDebugString();
  }

  return response;
}

BatchGetInodeResponse MDSClient::BatchGetInode(uint32_t fs_id, const std::vector<int64_t>& inos) {
  BatchGetInodeRequest request;
  BatchGetInodeResponse response;

  request.set_fs_id(fs_id);
  for (auto ino : inos) {
    request.add_inoes(ino);
  }

  interaction_->SendRequest("MDSService", "BatchGetInode", request, response);

  for (const auto& inode : response.inodes()) {
    DINGO_LOG(INFO) << "inode: " << inode.ShortDebugString();
  }

  return response;
}

BatchGetXAttrResponse MDSClient::BatchGetXattr(uint32_t fs_id, const std::vector<int64_t>& inos) {
  BatchGetXAttrRequest request;
  BatchGetXAttrResponse response;

  request.set_fs_id(fs_id);
  for (auto ino : inos) {
    request.add_inoes(ino);
  }

  interaction_->SendRequest("MDSService", "BatchGetXattr", request, response);

  for (const auto& xattr : response.xattrs()) {
    DINGO_LOG(INFO) << "xattr: " << xattr.ShortDebugString();
  }

  return response;
}

void MDSClient::SetFsStats(const std::string& fs_name) {
  pb::mdsv2::SetFsStatsRequest request;
  pb::mdsv2::SetFsStatsResponse response;

  request.set_fs_name(fs_name);

  using Helper = dingofs::mdsv2::Helper;

  pb::mdsv2::FsStatsData stats;
  stats.set_read_bytes(Helper::GenerateRealRandomInteger(1000, 10000000));
  stats.set_read_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_write_bytes(Helper::GenerateRealRandomInteger(1000, 10000000));
  stats.set_write_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_s3_read_bytes(Helper::GenerateRealRandomInteger(1000, 1000000));
  stats.set_s3_read_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_s3_write_bytes(Helper::GenerateRealRandomInteger(1000, 1000000));
  stats.set_s3_write_qps(Helper::GenerateRealRandomInteger(100, 10000));

  request.mutable_stats()->CopyFrom(stats);

  interaction_->SendRequest("MDSService", "SetFsStats", request, response);
}

void MDSClient::ContinueSetFsStats(const std::string& fs_name) {
  for (;;) {
    SetFsStats(fs_name);
    bthread_usleep(100000);  // 100ms
  }
}

void MDSClient::GetFsStats(const std::string& fs_name) {
  pb::mdsv2::GetFsStatsRequest request;
  pb::mdsv2::GetFsStatsResponse response;

  request.set_fs_name(fs_name);

  interaction_->SendRequest("MDSService", "GetFsStats", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "fs stats: " << response.stats().ShortDebugString();
  }
}

void MDSClient::GetFsPerSecondStats(const std::string& fs_name) {
  pb::mdsv2::GetFsPerSecondStatsRequest request;
  pb::mdsv2::GetFsPerSecondStatsResponse response;

  request.set_fs_name(fs_name);

  interaction_->SendRequest("MDSService", "GetFsPerSecondStats", request, response);

  // sort by time
  std::map<uint64_t, pb::mdsv2::FsStatsData> sorted_stats;
  for (const auto& [time_s, stats] : response.stats()) {
    sorted_stats.insert(std::make_pair(time_s, stats));
  }

  for (const auto& [time_s, stats] : sorted_stats) {
    DINGO_LOG(INFO) << fmt::format("time: {} stats: {}.", Helper::FormatTime(time_s), stats.ShortDebugString());
  }
}

LookupResponse MDSClient::Lookup(Ino parent, const std::string& name) {
  LookupRequest request;
  LookupResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "Lookup", request, response);

  return response;
}

OpenResponse MDSClient::Open(Ino ino) {
  OpenRequest request;
  OpenResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(O_RDWR);

  interaction_->SendRequest("MDSService", "Open", request, response);

  return response;
}

ReleaseResponse MDSClient::Release(Ino ino, const std::string& session_id) {
  ReleaseRequest request;
  ReleaseResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_session_id(session_id);

  interaction_->SendRequest("MDSService", "Release", request, response);

  return response;
}

LinkResponse MDSClient::Link(Ino ino, Ino new_parent, const std::string& new_name) {
  LinkRequest request;
  LinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  interaction_->SendRequest("MDSService", "Link", request, response);

  return response;
}

UnLinkResponse MDSClient::UnLink(Ino parent, const std::string& name) {
  UnLinkRequest request;
  UnLinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "UnLink", request, response);

  return response;
}

SymlinkResponse MDSClient::Symlink(Ino parent, const std::string& name, const std::string& symlink) {
  SymlinkRequest request;
  SymlinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_new_parent(parent);
  request.set_new_name(name);
  request.set_symlink(symlink);
  request.set_uid(0);
  request.set_gid(0);

  interaction_->SendRequest("MDSService", "Symlink", request, response);
  return response;
}

ReadLinkResponse MDSClient::ReadLink(Ino ino) {
  ReadLinkRequest request;
  ReadLinkResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "ReadLink", request, response);

  return response;
}

AllocSliceIdResponse MDSClient::AllocSliceId(uint32_t alloc_num, uint64_t min_slice_id) {
  AllocSliceIdRequest request;
  AllocSliceIdResponse response;

  request.set_alloc_num(alloc_num);
  request.set_min_slice_id(min_slice_id);

  interaction_->SendRequest("MDSService", "AllocSliceId", request, response);

  return response;
}

WriteSliceResponse MDSClient::WriteSlice(Ino ino, int64_t chunk_index) {
  WriteSliceRequest request;
  WriteSliceResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_chunk_index(chunk_index);

  const uint64_t len = 1024;
  for (int i = 0; i < 10; i++) {
    auto* slice = request.add_slices();
    slice->set_id(i + 100000);
    slice->set_offset(i * len);
    slice->set_len(len);
    slice->set_size(len);
  }

  interaction_->SendRequest("MDSService", "WriteSlice", request, response);

  return response;
}

ReadSliceResponse MDSClient::ReadSlice(Ino ino, int64_t chunk_index) {
  ReadSliceRequest request;
  ReadSliceResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_chunk_index(chunk_index);

  interaction_->SendRequest("MDSService", "ReadSlice", request, response);

  return response;
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs