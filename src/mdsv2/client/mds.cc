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
#include <glog/logging.h>
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

MDSClient::MDSClient(uint32_t fs_id) : fs_id_(fs_id) {
  FLAGS_logtostdout = true;
  FLAGS_logtostderr = true;
}

MDSClient::~MDSClient() {
  FLAGS_logtostdout = false;
  FLAGS_logtostderr = false;
}

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

  const auto& s3_info = params.s3_info;
  const auto& rados_info = params.rados_info;

  if (!s3_info.endpoint.empty()) {
    if (s3_info.ak.empty() || s3_info.sk.empty() || s3_info.bucket_name.empty()) {
      DINGO_LOG(ERROR) << "s3 info is empty.";
      return response;
    }

  } else if (!rados_info.mon_host.empty()) {
    if (rados_info.user_name.empty() || rados_info.key.empty() || rados_info.pool_name.empty() ||
        rados_info.cluster_name.empty()) {
      DINGO_LOG(ERROR) << "rados info is empty.";
      return response;
    }

  } else {
    DINGO_LOG(ERROR) << "s3 info and rados info is empty.";
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

  request.set_fs_id(params.fs_id);
  request.set_fs_name(fs_name);
  request.set_block_size(params.block_size);
  request.set_chunk_size(params.chunk_size);

  request.set_owner(params.owner);
  request.set_capacity(1024 * 1024 * 1024);
  request.set_recycle_time_hour(1);

  if (params.partition_type == "mono") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION);
  } else if (params.partition_type == "parent_hash") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION);
  }

  if (!s3_info.endpoint.empty()) {
    request.set_fs_type(pb::mdsv2::FsType::S3);
    auto* mut_s3_info = request.mutable_fs_extra()->mutable_s3_info();
    mut_s3_info->set_ak(s3_info.ak);
    mut_s3_info->set_sk(s3_info.sk);
    mut_s3_info->set_endpoint(s3_info.endpoint);
    mut_s3_info->set_bucketname(s3_info.bucket_name);
    mut_s3_info->set_object_prefix(0);

  } else {
    request.set_fs_type(pb::mdsv2::FsType::RADOS);
    auto* mut_rados_info = request.mutable_fs_extra()->mutable_rados_info();
    mut_rados_info->set_mon_host(rados_info.mon_host);
    mut_rados_info->set_user_name(rados_info.user_name);
    mut_rados_info->set_key(rados_info.key);
    mut_rados_info->set_pool_name(rados_info.pool_name);
    mut_rados_info->set_cluster_name(rados_info.cluster_name);
  }

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
  request.set_client_id(client_id);

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

MkDirResponse MDSClient::MkDir(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  MkDirRequest request;
  MkDirResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
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

void MDSClient::BatchMkDir(const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkDir(parent, name);
    }
  }
}

RmDirResponse MDSClient::RmDir(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  RmDirRequest request;
  RmDirResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "RmDir", request, response);

  return response;
}

ReadDirResponse MDSClient::ReadDir(Ino ino, const std::string& last_name, bool with_attr, bool is_refresh) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  ReadDirRequest request;
  ReadDirResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(100);
  request.set_with_attr(with_attr);
  request.set_is_refresh(is_refresh);

  interaction_->SendRequest("MDSService", "ReadDir", request, response);

  return response;
}

MkNodResponse MDSClient::MkNod(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  MkNodRequest request;
  MkNodResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
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

void MDSClient::BatchMkNod(const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkNod(parent, name);
    }
  }
}

GetDentryResponse MDSClient::GetDentry(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetDentryRequest request;
  GetDentryResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "GetDentry", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "dentry: " << response.dentry().ShortDebugString();
  }

  return response;
}

ListDentryResponse MDSClient::ListDentry(Ino parent, bool is_only_dir) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ListDentryRequest request;
  ListDentryResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_is_only_dir(is_only_dir);

  interaction_->SendRequest("MDSService", "ListDentry", request, response);

  for (const auto& dentry : response.dentries()) {
    DINGO_LOG(INFO) << "dentry: " << dentry.ShortDebugString();
  }

  return response;
}

GetInodeResponse MDSClient::GetInode(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetInodeRequest request;
  GetInodeResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "GetInode", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "inode: " << response.inode().ShortDebugString();
  }

  return response;
}

BatchGetInodeResponse MDSClient::BatchGetInode(const std::vector<int64_t>& inos) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  BatchGetInodeRequest request;
  BatchGetInodeResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  for (auto ino : inos) {
    request.add_inoes(ino);
  }

  interaction_->SendRequest("MDSService", "BatchGetInode", request, response);

  for (const auto& inode : response.inodes()) {
    DINGO_LOG(INFO) << "inode: " << inode.ShortDebugString();
  }

  return response;
}

BatchGetXAttrResponse MDSClient::BatchGetXattr(const std::vector<int64_t>& inos) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  BatchGetXAttrRequest request;
  BatchGetXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
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
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  LookupRequest request;
  LookupResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "Lookup", request, response);

  return response;
}

OpenResponse MDSClient::Open(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  OpenRequest request;
  OpenResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(O_RDWR);

  interaction_->SendRequest("MDSService", "Open", request, response);

  return response;
}

ReleaseResponse MDSClient::Release(Ino ino, const std::string& session_id) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReleaseRequest request;
  ReleaseResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_session_id(session_id);

  interaction_->SendRequest("MDSService", "Release", request, response);

  return response;
}

LinkResponse MDSClient::Link(Ino ino, Ino new_parent, const std::string& new_name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  LinkRequest request;
  LinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  interaction_->SendRequest("MDSService", "Link", request, response);

  return response;
}

UnLinkResponse MDSClient::UnLink(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  UnLinkRequest request;
  UnLinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  interaction_->SendRequest("MDSService", "UnLink", request, response);

  return response;
}

SymlinkResponse MDSClient::Symlink(Ino parent, const std::string& name, const std::string& symlink) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SymlinkRequest request;
  SymlinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

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
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReadLinkRequest request;
  ReadLinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "ReadLink", request, response);

  return response;
}

AllocSliceIdResponse MDSClient::AllocSliceId(uint32_t alloc_num, uint64_t min_slice_id) {
  AllocSliceIdRequest request;
  AllocSliceIdResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_alloc_num(alloc_num);
  request.set_min_slice_id(min_slice_id);

  interaction_->SendRequest("MDSService", "AllocSliceId", request, response);

  return response;
}

WriteSliceResponse MDSClient::WriteSlice(Ino ino, int64_t chunk_index) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  WriteSliceRequest request;
  WriteSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  mdsv2::DeltaSliceEntry delta_slice;
  delta_slice.set_chunk_index(chunk_index);

  const uint64_t len = 1024;
  for (int i = 0; i < 10; i++) {
    auto* slice = delta_slice.add_slices();
    slice->set_id(i + 100000);
    slice->set_offset(i * len);
    slice->set_len(len);
    slice->set_size(len);
  }

  *request.add_delta_slices() = delta_slice;

  interaction_->SendRequest("MDSService", "WriteSlice", request, response);

  return response;
}

ReadSliceResponse MDSClient::ReadSlice(Ino ino, int64_t chunk_index) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReadSliceRequest request;
  ReadSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.add_chunk_indexes(chunk_index);

  interaction_->SendRequest("MDSService", "ReadSlice", request, response);

  return response;
}

SetFsQuotaResponse MDSClient::SetFsQuota(const QuotaEntry& quota) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetFsQuotaRequest request;
  SetFsQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.mutable_quota()->CopyFrom(quota);
  interaction_->SendRequest("MDSService", "SetFsQuota", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "SetFsQuota success";
  } else {
    DINGO_LOG(ERROR) << "SetFsQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

GetFsQuotaResponse MDSClient::GetFsQuota() {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetFsQuotaRequest request;
  GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);

  interaction_->SendRequest("MDSService", "GetFsQuota", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "GetFsQuota success, quota: " << response.quota().ShortDebugString();
  } else {
    DINGO_LOG(ERROR) << "GetFsQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

SetDirQuotaResponse MDSClient::SetDirQuota(Ino ino, const QuotaEntry& quota) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  SetDirQuotaRequest request;
  SetDirQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.mutable_quota()->CopyFrom(quota);

  interaction_->SendRequest("MDSService", "SetDirQuota", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "SetDirQuota success";
  } else {
    DINGO_LOG(ERROR) << "SetDirQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

GetDirQuotaResponse MDSClient::GetDirQuota(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  GetDirQuotaRequest request;
  GetDirQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "GetDirQuota", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "GetDirQuota success, quota: " << response.quota().ShortDebugString();
  } else {
    DINGO_LOG(ERROR) << "GetDirQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

DeleteDirQuotaResponse MDSClient::DeleteDirQuota(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  DeleteDirQuotaRequest request;
  DeleteDirQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  interaction_->SendRequest("MDSService", "DeleteDirQuota", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "DeleteDirQuota success";
  } else {
    DINGO_LOG(ERROR) << "DeleteDirQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

JoinFsResponse MDSClient::JoinFs(const std::string& fs_name, uint32_t fs_id, const std::vector<int64_t>& mds_ids) {
  JoinFsRequest request;
  JoinFsResponse response;

  request.set_fs_id(fs_id);
  request.set_fs_name(fs_name);
  for (const auto& mds_id : mds_ids) {
    request.add_mds_ids(mds_id);
  }

  DINGO_LOG(INFO) << "JoinFs request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "JoinFs", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "JoinFs success";
  } else {
    DINGO_LOG(ERROR) << "JoinFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

QuitFsResponse MDSClient::QuitFs(const std::string& fs_name, uint32_t fs_id, const std::vector<int64_t>& mds_ids) {
  QuitFsRequest request;
  QuitFsResponse response;

  request.set_fs_id(fs_id);
  request.set_fs_name(fs_name);
  for (const auto& mds_id : mds_ids) {
    request.add_mds_ids(mds_id);
  }

  interaction_->SendRequest("MDSService", "QuitFs", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "QuitFs success";
  } else {
    DINGO_LOG(ERROR) << "QuitFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

// cache member operations
JoinCacheGroupResponse MDSClient::JoinCacheGroup(const std::string& member_id, const std::string& ip, uint32_t port,
                                                 const std::string& group_name, uint32_t weight) {
  JoinCacheGroupRequest request;
  JoinCacheGroupResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);
  request.set_weight(weight);

  interaction_->SendRequest("MDSService", "JoinCacheGroup", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "JoinCacheGroup success";
  } else {
    DINGO_LOG(ERROR) << "JoinCacheGroup fail, error: " << response.ShortDebugString();
  }

  return response;
}

LeaveCacheGroupResponse MDSClient::LeaveCacheGroup(const std::string& member_id, const std::string& ip, uint32_t port,
                                                   const std::string& group_name) {
  LeaveCacheGroupRequest request;
  LeaveCacheGroupResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);

  interaction_->SendRequest("MDSService", "LeaveCacheGroup", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "LeaveCacheGroup success";
  } else {
    DINGO_LOG(ERROR) << "LeaveCacheGroup fail, error: " << response.ShortDebugString();
  }

  return response;
}

ListGroupsResponse MDSClient::ListGroups() {
  ListGroupsRequest request;
  ListGroupsResponse response;

  interaction_->SendRequest("MDSService", "ListGroups", request, response);
  if (response.error().errcode() != dingofs::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "ListGroups fail, error: " << response.ShortDebugString();
  }

  for (const auto& group_name : response.group_names()) {
    DINGO_LOG(INFO) << "group_name: " << group_name;
  }

  return response;
}

ReweightMemberResponse MDSClient::ReweightMember(const std::string& member_id, const std::string& ip, uint32_t port,
                                                 uint32_t weight) {
  ReweightMemberRequest request;
  ReweightMemberResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_weight(weight);

  interaction_->SendRequest("MDSService", "ReweightMember", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "ReweightMember success";
  } else {
    DINGO_LOG(ERROR) << "ReweightMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

ListMembersResponse MDSClient::ListMembers(const std::string& group_name) {
  ListMembersRequest request;
  ListMembersResponse response;

  request.set_group_name(group_name);

  interaction_->SendRequest("MDSService", "ListMembers", request, response);
  if (response.error().errcode() != dingofs::pb::error::Errno::OK) {
    DINGO_LOG(ERROR) << "ListMembers fail, error: " << response.ShortDebugString();
  }

  for (const auto& member : response.members()) {
    DINGO_LOG(INFO) << "cache_member: " << member.ShortDebugString();
  }

  return response;
}

UnLockMemberResponse MDSClient::UnlockMember(const std::string& member_id, const std::string& ip, uint32_t port) {
  UnLockMemberRequest request;
  UnLockMemberResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);

  interaction_->SendRequest("MDSService", "UnlockMember", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "UnlockMember success";
  } else {
    DINGO_LOG(ERROR) << "UnlockMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

DeleteMemberResponse MDSClient::DeleteMember(const std::string& member_id) {
  DeleteMemberRequest request;
  DeleteMemberResponse response;

  request.set_member_id(member_id);

  interaction_->SendRequest("MDSService", "DeleteMember", request, response);
  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "DeleteMember success";
  } else {
    DINGO_LOG(ERROR) << "DeleteMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

bool MdsCommandRunner::Run(const Options& options, const std::string& mds_addr, const std::string& cmd,
                           uint32_t fs_id) {
  static std::set<std::string> mds_cmd = {
      "integrationtest", "getmdslist",
      "createfs",        "deletefs",
      "updatefs",        "getfs",
      "listfs",          "mkdir",
      "batchmkdir",      "mknod",
      "batchmknod",      "getdentry",
      "listdentry",      "getinode",
      "batchgetinode",   "batchgetxattr",
      "setfsstats",      "continuesetfsstats",
      "getfsstats",      "getfspersecondstats",
      "setfsquota",      "getfsquota",
      "setdirquota",     "getdirquota",
      "deletedirquota",  "joinfs",
      "quitfs",          "joincachegroup",
      "leavecachegroup", "listgroups",
      "reweightmember",  "listmembers",
      "unlockmember",    "deletemember",
  };

  if (mds_cmd.count(cmd) == 0) return false;

  if (mds_addr.empty()) {
    std::cout << "mds_addr is empty." << '\n';
    return true;
  }

  MDSClient mds_client(fs_id);
  if (!mds_client.Init(mds_addr)) {
    std::cout << "init interaction fail." << '\n';
    return true;
  }

  if (cmd == Helper::ToLowerCase("GetMdsList")) {
    mds_client.GetMdsList();

  } else if (cmd == Helper::ToLowerCase("CreateFs")) {
    dingofs::mdsv2::client::MDSClient::CreateFsParams params;
    params.partition_type = options.fs_partition_type;
    params.chunk_size = options.chunk_size;
    params.block_size = options.block_size;
    params.s3_info = options.s3_info;
    params.rados_info = options.rados_info;
    params.fs_id = options.fs_id;
    params.expect_mds_num = options.num;

    mds_client.CreateFs(options.fs_name, params);

  } else if (cmd == Helper::ToLowerCase("DeleteFs")) {
    mds_client.DeleteFs(options.fs_name, options.is_force);

  } else if (cmd == Helper::ToLowerCase("UpdateFs")) {
    mds_client.UpdateFs(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("GetFs")) {
    mds_client.GetFs(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("ListFs")) {
    mds_client.ListFs();

  } else if (cmd == Helper::ToLowerCase("MkDir")) {
    mds_client.MkDir(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("BatchMkDir")) {
    std::vector<int64_t> parents;
    dingofs::mdsv2::Helper::SplitString(options.parents, ',', parents);
    mds_client.BatchMkDir(parents, options.prefix, options.num);

  } else if (cmd == Helper::ToLowerCase("MkNod")) {
    mds_client.MkNod(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("BatchMkNod")) {
    std::vector<int64_t> parents;
    dingofs::mdsv2::Helper::SplitString(options.parents, ',', parents);
    mds_client.BatchMkNod(parents, options.prefix, options.num);

  } else if (cmd == Helper::ToLowerCase("GetDentry")) {
    mds_client.GetDentry(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("ListDentry")) {
    mds_client.ListDentry(options.parent, false);

  } else if (cmd == Helper::ToLowerCase("GetInode")) {
    mds_client.GetInode(options.parent);

  } else if (cmd == Helper::ToLowerCase("BatchGetInode")) {
    std::vector<int64_t> inos;
    dingofs::mdsv2::Helper::SplitString(options.parents, ',', inos);
    mds_client.BatchGetInode(inos);

  } else if (cmd == Helper::ToLowerCase("BatchGetXattr")) {
    std::vector<int64_t> inos;
    dingofs::mdsv2::Helper::SplitString(options.parents, ',', inos);
    mds_client.BatchGetXattr(inos);

  } else if (cmd == Helper::ToLowerCase("SetFsStats")) {
    mds_client.SetFsStats(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("ContinueSetFsStats")) {
    mds_client.ContinueSetFsStats(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("GetFsStats")) {
    mds_client.GetFsStats(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("GetFsPerSecondStats")) {
    mds_client.GetFsPerSecondStats(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("SetFsQuota")) {
    dingofs::mdsv2::QuotaEntry quota;
    quota.set_max_bytes(options.max_bytes);
    quota.set_max_inodes(options.max_inodes);

    mds_client.SetFsQuota(quota);

  } else if (cmd == Helper::ToLowerCase("GetFsQuota")) {
    auto response = mds_client.GetFsQuota();
    std::cout << "fs quota: " << response.quota().ShortDebugString() << '\n';

  } else if (cmd == Helper::ToLowerCase("SetDirQuota")) {
    if (options.ino == 0) {
      std::cout << "ino is empty." << '\n';
      return true;
    }

    dingofs::mdsv2::QuotaEntry quota;
    quota.set_max_bytes(options.max_bytes);
    quota.set_max_inodes(options.max_inodes);

    mds_client.SetDirQuota(options.ino, quota);

  } else if (cmd == Helper::ToLowerCase("GetDirQuota")) {
    if (options.ino == 0) {
      std::cout << "ino is empty." << '\n';
      return true;
    }

    auto response = mds_client.GetDirQuota(options.ino);
    std::cout << "dir quota: " << response.quota().ShortDebugString() << '\n';

  } else if (cmd == Helper::ToLowerCase("DeleteDirQuota")) {
    if (options.ino == 0) {
      std::cout << "ino is empty." << '\n';
      return true;
    }
    mds_client.DeleteDirQuota(options.ino);
  } else if (cmd == Helper::ToLowerCase("JoinFs")) {
    if (options.fs_name.empty() && options.fs_id == 0) {
      std::cout << "fs_name and fs_id is empty." << '\n';
      return true;
    }

    if (options.mds_id_list.empty()) {
      std::cout << "mds_id_list is empty." << '\n';
      return true;
    }

    std::vector<int64_t> mds_ids;
    dingofs::mdsv2::Helper::SplitString(options.mds_id_list, ',', mds_ids);
    auto response = mds_client.JoinFs(options.fs_name, options.fs_id, mds_ids);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "joinfs success." << '\n';
    } else {
      std::cout << "joinfs fail, error: " << response.ShortDebugString() << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("QuitFs")) {
    if (options.fs_name.empty() && options.fs_id == 0) {
      std::cout << "fs_name and fs_id is empty." << '\n';
      return true;
    }

    if (options.mds_id_list.empty()) {
      std::cout << "mds_id_list is empty." << '\n';
      return true;
    }

    std::vector<int64_t> mds_ids;
    dingofs::mdsv2::Helper::SplitString(options.mds_id_list, ',', mds_ids);
    auto response = mds_client.QuitFs(options.fs_name, options.fs_id, mds_ids);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "quitfs success." << '\n';
    } else {
      std::cout << "quitfs fail, error: " << response.ShortDebugString() << '\n';
    }
  } else if (cmd == Helper::ToLowerCase("JoinCacheGroup")) {
    auto response =
        mds_client.JoinCacheGroup(options.member_id, options.ip, options.port, options.group_name, options.weight);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "joincachegroup success." << '\n';
    } else {
      std::cout << "joincachegroup fail, error: " << response.ShortDebugString() << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("LeaveCacheGroup")) {
    auto response = mds_client.LeaveCacheGroup(options.member_id, options.ip, options.port, options.group_name);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "leavecachegroup success." << '\n';
    } else {
      std::cout << "leavecachegroup fail, error: " << response.ShortDebugString() << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("ReweightMember")) {
    auto response = mds_client.ReweightMember(options.member_id, options.ip, options.port, options.weight);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "reweightmember success." << '\n';
    } else {
      std::cout << "reweightmember fail, error: " << response.ShortDebugString() << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("ListGroups")) {
    auto response = mds_client.ListGroups();

  } else if (cmd == Helper::ToLowerCase("ListMembers")) {
    auto response = mds_client.ListMembers(options.group_name);

  } else if (cmd == Helper::ToLowerCase("UnlockMember")) {
    auto response = mds_client.UnlockMember(options.member_id, options.ip, options.port);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "unlockmember success." << '\n';
    } else {
      std::cout << "unlockmember fail, error: " << response.ShortDebugString() << '\n';
    }
  } else if (cmd == Helper::ToLowerCase("DeleteMember")) {
    auto response = mds_client.DeleteMember(options.member_id);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "deletemember success." << '\n';
    } else {
      std::cout << "deletemember fail, error: " << response.ShortDebugString() << '\n';
    }
  }

  return true;
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs