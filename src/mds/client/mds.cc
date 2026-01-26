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

#include "mds/client/mds.h"

#include <fcntl.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "utils/time.h"
#include "utils/uuid.h"

namespace dingofs {
namespace mds {
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
  interaction_ = dingofs::mds::client::Interaction::New();
  return interaction_->Init(mds_addr);
}

EchoResponse MDSClient::Echo(const std::string& message) {
  EchoRequest request;
  EchoResponse response;

  request.set_message(message);

  interaction_->SendRequest("MDSService", "Echo", request, response);

  return response;
}

HeartbeatResponse MDSClient::Heartbeat(uint32_t mds_id) {
  HeartbeatRequest request;
  HeartbeatResponse response;

  request.set_role(pb::mds::Role::ROLE_MDS);
  auto* mds = request.mutable_mds();
  mds->set_id(mds_id);
  mds->mutable_location()->set_host("127.0.0.1");
  mds->mutable_location()->set_port(10000);
  mds->set_state(MdsEntry::NORMAL);
  mds->set_last_online_time_ms(utils::TimestampMs());

  auto status = interaction_->SendRequest("MDSService", "Heartbeat", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

GetMDSListResponse MDSClient::GetMdsList() {
  GetMDSListRequest request;
  GetMDSListResponse response;

  auto status = interaction_->SendRequest("MDSService", "GetMDSList", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& mds : response.mdses()) {
    LOG(INFO) << "mds: " << mds.ShortDebugString();
  }

  return response;
}

CreateFsResponse MDSClient::CreateFs(const std::string& fs_name, const CreateFsParams& params) {
  CreateFsRequest request;
  CreateFsResponse response;

  if (fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return response;
  }

  const auto& s3_info = params.s3_info;
  const auto& rados_info = params.rados_info;
  const auto& local_file_info = params.local_file_info;

  if (!s3_info.endpoint.empty()) {
    if (s3_info.ak.empty() || s3_info.sk.empty() || s3_info.bucket_name.empty()) {
      LOG(ERROR) << "s3 info is empty.";
      return response;
    }

  } else if (!rados_info.mon_host.empty()) {
    if (rados_info.user_name.empty() || rados_info.key.empty() || rados_info.pool_name.empty() ||
        rados_info.cluster_name.empty()) {
      LOG(ERROR) << "rados info is empty.";
      return response;
    }

  } else {
    LOG(ERROR) << "s3 info and rados info is empty.";
    return response;
  }

  if (params.chunk_size == 0) {
    LOG(ERROR) << "chunk_size is 0";
    return response;
  }
  if (params.block_size == 0) {
    LOG(ERROR) << "block_size is 0";
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
    request.set_partition_type(::dingofs::pb::mds::PartitionType::MONOLITHIC_PARTITION);
  } else if (params.partition_type == "parent_hash") {
    request.set_partition_type(::dingofs::pb::mds::PartitionType::PARENT_ID_HASH_PARTITION);
  }

  if (!s3_info.endpoint.empty()) {
    request.set_fs_type(pb::mds::FsType::S3);
    auto* mut_s3_info = request.mutable_fs_extra()->mutable_s3_info();
    mut_s3_info->set_ak(s3_info.ak);
    mut_s3_info->set_sk(s3_info.sk);
    mut_s3_info->set_endpoint(s3_info.endpoint);
    mut_s3_info->set_bucketname(s3_info.bucket_name);
    mut_s3_info->set_object_prefix(0);

  } else if (!rados_info.mon_host.empty()) {
    request.set_fs_type(pb::mds::FsType::RADOS);
    auto* mut_rados_info = request.mutable_fs_extra()->mutable_rados_info();
    mut_rados_info->set_mon_host(rados_info.mon_host);
    mut_rados_info->set_user_name(rados_info.user_name);
    mut_rados_info->set_key(rados_info.key);
    mut_rados_info->set_pool_name(rados_info.pool_name);
    mut_rados_info->set_cluster_name(rados_info.cluster_name);

  } else if (!local_file_info.path.empty()) {
    request.set_fs_type(pb::mds::FsType::LOCALFILE);
    auto* mut_local_file_info = request.mutable_fs_extra()->mutable_file_info();
    mut_local_file_info->set_path(local_file_info.path);
  }

  for (const auto& mds_id : params.candidate_mds_ids) {
    request.add_candidate_mds_ids(mds_id);
  }

  LOG(INFO) << "CreateFs request: " << request.ShortDebugString();

  auto status = interaction_->SendRequest("MDSService", "CreateFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "CreateFs success, fs_id: " << response.fs_info().fs_id();
  } else {
    LOG(ERROR) << "CreateFs fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "MountFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "MountFs success";
  } else {
    LOG(ERROR) << "MountFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

UmountFsResponse MDSClient::UmountFs(const std::string& fs_name, const std::string& client_id) {
  UmountFsRequest request;
  UmountFsResponse response;

  request.set_fs_name(fs_name);
  request.set_client_id(client_id);

  auto status = interaction_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "MountFs success";
  } else {
    LOG(ERROR) << "MountFs fail, error: " << response.ShortDebugString();
  }

  return response;
}

DeleteFsResponse MDSClient::DeleteFs(const std::string& fs_name, bool is_force) {
  DeleteFsRequest request;
  DeleteFsResponse response;

  if (fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return response;
  }

  request.set_fs_name(fs_name);
  request.set_is_force(is_force);

  LOG(INFO) << "DeleteFs request: " << request.ShortDebugString();

  auto status = interaction_->SendRequest("MDSService", "DeleteFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  LOG(INFO) << "DeleteFs response: " << response.ShortDebugString();

  return response;
}

UpdateFsInfoResponse MDSClient::UpdateFs(const std::string& fs_name, const pb::mds::FsInfo& fs_info) {
  UpdateFsInfoRequest request;
  UpdateFsInfoResponse response;

  request.set_fs_name(fs_name);

  request.mutable_fs_info()->CopyFrom(fs_info);

  auto status = interaction_->SendRequest("MDSService", "UpdateFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

GetFsInfoResponse MDSClient::GetFs(const std::string& fs_name) {
  if (fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return {};
  }

  GetFsInfoRequest request;
  GetFsInfoResponse response;

  request.set_fs_name(fs_name);

  LOG(INFO) << "GetFsInfo request: " << request.ShortDebugString();

  auto status = interaction_->SendRequest("MDSService", "GetFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  LOG(INFO) << "GetFsInfo response: " << response.ShortDebugString();

  return response;
}

ListFsInfoResponse MDSClient::ListFs() {
  ListFsInfoRequest request;
  ListFsInfoResponse response;

  auto status = interaction_->SendRequest("MDSService", "ListFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& fs_info : response.fs_infos()) {
    LOG(INFO) << "fs_info: " << fs_info.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "MkDir", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "MkDir success, ino: " << response.inode().ino();
  } else {
    LOG(ERROR) << "MkDir fail, error: " << response.ShortDebugString();
  }

  return response;
}

void MDSClient::BatchMkDir(const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, utils::TimestampNs());
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

  auto status = interaction_->SendRequest("MDSService", "RmDir", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "ReadDir", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "MkNod", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "MkNode success, ino: " << response.inode().ino();
  } else {
    LOG(ERROR) << "MkNode fail, error: " << response.ShortDebugString();
  }

  return response;
}

void MDSClient::BatchMkNod(const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, utils::TimestampNs());
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

  auto status = interaction_->SendRequest("MDSService", "GetDentry", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "dentry: " << response.dentry().ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "ListDentry", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& dentry : response.dentries()) {
    LOG(INFO) << "dentry: " << dentry.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "GetInode", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "inode: " << response.inode().ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "BatchGetInode", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& inode : response.inodes()) {
    LOG(INFO) << "inode: " << inode.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "BatchGetXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& xattr : response.xattrs()) {
    LOG(INFO) << "xattr: " << xattr.ShortDebugString();
  }

  return response;
}

void MDSClient::SetFsStats(const std::string& fs_name) {
  pb::mds::SetFsStatsRequest request;
  pb::mds::SetFsStatsResponse response;

  request.set_fs_name(fs_name);

  using Helper = dingofs::mds::Helper;

  pb::mds::FsStatsData stats;
  stats.set_read_bytes(Helper::GenerateRealRandomInteger(1000, 10000000));
  stats.set_read_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_write_bytes(Helper::GenerateRealRandomInteger(1000, 10000000));
  stats.set_write_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_s3_read_bytes(Helper::GenerateRealRandomInteger(1000, 1000000));
  stats.set_s3_read_qps(Helper::GenerateRealRandomInteger(100, 1000));
  stats.set_s3_write_bytes(Helper::GenerateRealRandomInteger(1000, 1000000));
  stats.set_s3_write_qps(Helper::GenerateRealRandomInteger(100, 10000));

  request.mutable_stats()->CopyFrom(stats);

  auto status = interaction_->SendRequest("MDSService", "SetFsStats", request, response);
}

void MDSClient::ContinueSetFsStats(const std::string& fs_name) {
  for (;;) {
    SetFsStats(fs_name);
    bthread_usleep(100000);  // 100ms
  }
}

void MDSClient::GetFsStats(const std::string& fs_name) {
  pb::mds::GetFsStatsRequest request;
  pb::mds::GetFsStatsResponse response;

  request.set_fs_name(fs_name);

  auto status = interaction_->SendRequest("MDSService", "GetFsStats", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "fs stats: " << response.stats().ShortDebugString();
  }
}

void MDSClient::GetFsPerSecondStats(const std::string& fs_name) {
  pb::mds::GetFsPerSecondStatsRequest request;
  pb::mds::GetFsPerSecondStatsResponse response;

  request.set_fs_name(fs_name);

  auto status = interaction_->SendRequest("MDSService", "GetFsPerSecondStats", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return;
  }

  // sort by time
  std::map<uint64_t, pb::mds::FsStatsData> sorted_stats;
  for (const auto& [time_s, stats] : response.stats()) {
    sorted_stats.insert(std::make_pair(time_s, stats));
  }

  for (const auto& [time_s, stats] : sorted_stats) {
    LOG(INFO) << fmt::format("time: {} stats: {}.", utils::FormatTime(time_s), stats.ShortDebugString());
  }
}

LookupResponse MDSClient::Lookup(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  LookupRequest request;
  LookupResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status = interaction_->SendRequest("MDSService", "Lookup", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

OpenResponse MDSClient::Open(Ino ino, std::string& session_id) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  OpenRequest request;
  OpenResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_flags(O_RDWR);
  request.set_session_id(utils::GenerateUUID());

  auto status = interaction_->SendRequest("MDSService", "Open", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "Release", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "Link", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "UnLink", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

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

  auto status = interaction_->SendRequest("MDSService", "Symlink", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ReadLinkResponse MDSClient::ReadLink(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReadLinkRequest request;
  ReadLinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = interaction_->SendRequest("MDSService", "ReadLink", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

GetAttrResponse MDSClient::GetAttr(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetAttrRequest request;
  GetAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = interaction_->SendRequest("MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "GetAttr success, inode: " << response.inode().ShortDebugString();
  } else {
    LOG(ERROR) << "GetAttr fail, error: " << response.ShortDebugString();
  }

  return response;
}

SetAttrResponse MDSClient::SetAttr(Ino ino, uint32_t to_set, const pb::mds::Inode& inode) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetAttrRequest request;
  SetAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_to_set(to_set);

  // copy fields from provided inode message into request
  request.set_length(inode.length());
  request.set_ctime(inode.ctime());
  request.set_mtime(inode.mtime());
  request.set_atime(inode.atime());
  request.set_uid(inode.uid());
  request.set_gid(inode.gid());
  request.set_mode(inode.mode());
  request.set_flags(inode.flags());

  auto status = interaction_->SendRequest("MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "SetAttr success";
  } else {
    LOG(ERROR) << "SetAttr fail, error: " << response.ShortDebugString();
  }

  return response;
}

GetXAttrResponse MDSClient::GetXAttr(Ino ino, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetXAttrRequest request;
  GetXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status = interaction_->SendRequest("MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "GetXAttr success, size: " << response.value().size();
  } else {
    LOG(ERROR) << "GetXAttr fail, error: " << response.ShortDebugString();
  }

  return response;
}

SetXAttrResponse MDSClient::SetXAttr(Ino ino, const std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetXAttrRequest request;
  SetXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  for (const auto& [k, v] : xattrs) {
    (*request.mutable_xattrs())[k] = v;
  }

  auto status = interaction_->SendRequest("MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "SetXAttr success";
  } else {
    LOG(ERROR) << "SetXAttr fail, error: " << response.ShortDebugString();
  }

  return response;
}

RemoveXAttrResponse MDSClient::RemoveXAttr(Ino ino, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  RemoveXAttrRequest request;
  RemoveXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_name(name);

  auto status = interaction_->SendRequest("MDSService", "RemoveXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ListXAttrResponse MDSClient::ListXAttr(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ListXAttrRequest request;
  ListXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status = interaction_->SendRequest("MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "ListXAttr success, count: " << response.xattrs_size();
  } else {
    LOG(ERROR) << "ListXAttr fail, error: " << response.ShortDebugString();
  }

  return response;
}

RenameResponse MDSClient::Rename(Ino old_parent, const std::string& old_name, Ino new_parent,
                                 const std::string& new_name, const std::vector<int64_t>& old_ancestors,
                                 const std::vector<int64_t>& new_ancestors) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  RenameRequest request;
  RenameResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_old_parent(old_parent);
  request.set_old_name(old_name);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  for (const auto& a : old_ancestors) request.add_old_ancestors(a);
  for (const auto& a : new_ancestors) request.add_new_ancestors(a);

  auto status = interaction_->SendRequest("MDSService", "Rename", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

AllocSliceIdResponse MDSClient::AllocSliceId(uint32_t alloc_num, uint64_t min_slice_id) {
  AllocSliceIdRequest request;
  AllocSliceIdResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_alloc_num(alloc_num);
  request.set_min_slice_id(min_slice_id);

  auto status = interaction_->SendRequest("MDSService", "AllocSliceId", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

WriteSliceResponse MDSClient::WriteSlice(Ino parent, Ino ino, int64_t chunk_index) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  WriteSliceRequest request;
  WriteSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_ino(ino);

  mds::DeltaSliceEntry delta_slice;
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

  auto status = interaction_->SendRequest("MDSService", "WriteSlice", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ReadSliceResponse MDSClient::ReadSlice(Ino ino, int64_t chunk_index) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReadSliceRequest request;
  ReadSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.add_chunk_descriptors()->set_index(chunk_index);

  auto status = interaction_->SendRequest("MDSService", "ReadSlice", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

SetFsQuotaResponse MDSClient::SetFsQuota(const QuotaEntry& quota) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetFsQuotaRequest request;
  SetFsQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.mutable_quota()->CopyFrom(quota);
  auto status = interaction_->SendRequest("MDSService", "SetFsQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "SetFsQuota success";
  } else {
    LOG(ERROR) << "SetFsQuota fail, error: " << response.ShortDebugString();
  }

  return response;
}

GetFsQuotaResponse MDSClient::GetFsQuota() {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetFsQuotaRequest request;
  GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);

  auto status = interaction_->SendRequest("MDSService", "GetFsQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "GetFsQuota success, quota: " << response.quota().ShortDebugString();
  } else {
    LOG(ERROR) << "GetFsQuota fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "SetDirQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "SetDirQuota success";
  } else {
    LOG(ERROR) << "SetDirQuota fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "GetDirQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "GetDirQuota success, quota: " << response.quota().ShortDebugString();
  } else {
    LOG(ERROR) << "GetDirQuota fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "DeleteDirQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "DeleteDirQuota success";
  } else {
    LOG(ERROR) << "DeleteDirQuota fail, error: " << response.ShortDebugString();
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

  LOG(INFO) << "JoinFs request: " << request.ShortDebugString();

  auto status = interaction_->SendRequest("MDSService", "JoinFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "JoinFs success";
  } else {
    LOG(ERROR) << "JoinFs fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "QuitFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "QuitFs success";
  } else {
    LOG(ERROR) << "QuitFs fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "JoinCacheGroup", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "JoinCacheGroup success";
  } else {
    LOG(ERROR) << "JoinCacheGroup fail, error: " << response.ShortDebugString();
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

  auto status = interaction_->SendRequest("MDSService", "LeaveCacheGroup", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "LeaveCacheGroup success";
  } else {
    LOG(ERROR) << "LeaveCacheGroup fail, error: " << response.ShortDebugString();
  }

  return response;
}

ListGroupsResponse MDSClient::ListGroups() {
  ListGroupsRequest request;
  ListGroupsResponse response;

  auto status = interaction_->SendRequest("MDSService", "ListGroups", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& group_name : response.group_names()) {
    LOG(INFO) << "group_name: " << group_name;
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

  auto status = interaction_->SendRequest("MDSService", "ReweightMember", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "ReweightMember success";
  } else {
    LOG(ERROR) << "ReweightMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

ListMembersResponse MDSClient::ListMembers(const std::string& group_name) {
  ListMembersRequest request;
  ListMembersResponse response;

  request.set_group_name(group_name);

  auto status = interaction_->SendRequest("MDSService", "ListMembers", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() != dingofs::pb::error::Errno::OK) {
    LOG(ERROR) << "ListMembers fail, error: " << response.ShortDebugString();
  }

  for (const auto& member : response.members()) {
    LOG(INFO) << "cache_member: " << member.ShortDebugString();
  }

  return response;
}

UnLockMemberResponse MDSClient::UnlockMember(const std::string& member_id, const std::string& ip, uint32_t port) {
  UnLockMemberRequest request;
  UnLockMemberResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);

  auto status = interaction_->SendRequest("MDSService", "UnlockMember", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "UnlockMember success";
  } else {
    LOG(ERROR) << "UnlockMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

DeleteMemberResponse MDSClient::DeleteMember(const std::string& member_id) {
  DeleteMemberRequest request;
  DeleteMemberResponse response;

  request.set_member_id(member_id);

  auto status = interaction_->SendRequest("MDSService", "DeleteMember", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    LOG(INFO) << "DeleteMember success";
  } else {
    LOG(ERROR) << "DeleteMember fail, error: " << response.ShortDebugString();
  }

  return response;
}

void MDSClient::UpdateFsS3Info(const std::string& fs_name, const S3Info& s3_info) {
  if (fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return;
  }
  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    LOG(ERROR) << "not found fs: " << fs_name;
    return;
  }
  if (fs_info.fs_type() != pb::mds::FsType::S3) {
    LOG(ERROR) << "fs type is not S3, fs_type: " << fs_info.fs_type();
    return;
  }

  pb::mds::S3Info pb_s3_info;
  pb_s3_info.set_ak(s3_info.ak);
  pb_s3_info.set_sk(s3_info.sk);
  pb_s3_info.set_endpoint(s3_info.endpoint);
  pb_s3_info.set_bucketname(s3_info.bucket_name);
  fs_info.mutable_extra()->mutable_s3_info()->CopyFrom(pb_s3_info);

  UpdateFs(fs_name, fs_info);
}

void MDSClient::UpdateFsRadosInfo(const std::string& fs_name, const RadosInfo& rados_info) {
  if (fs_name.empty()) {
    LOG(ERROR) << "fs_name is empty";
    return;
  }

  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    LOG(ERROR) << "not found fs: " << fs_name;
    return;
  }
  if (fs_info.fs_type() != pb::mds::FsType::RADOS) {
    LOG(ERROR) << "fs type is not RADOS, fs_type: " << fs_info.fs_type();
    return;
  }

  pb::mds::RadosInfo pb_rados_info;
  pb_rados_info.set_mon_host(rados_info.mon_host);
  pb_rados_info.set_pool_name(rados_info.pool_name);
  pb_rados_info.set_user_name(rados_info.user_name);
  pb_rados_info.set_key(rados_info.key);
  pb_rados_info.set_cluster_name(rados_info.cluster_name);
  fs_info.mutable_extra()->mutable_rados_info()->CopyFrom(pb_rados_info);

  UpdateFs(fs_name, fs_info);
}

bool MdsCommandRunner::Run(const Options& options, const std::string& mds_addr, const std::string& cmd,
                           uint32_t fs_id) {
  static std::set<std::string> mds_cmd = {
      "integrationtest",
      "getmdslist",
      "createfs",
      "deletefs",
      "updatefs",
      "updatefss3info",
      "updatefsradosinfo",
      "getfs",
      "listfs",
      "mkdir",
      "batchmkdir",
      "mknod",
      "batchmknod",
      "getdentry",
      "listdentry",
      "getinode",
      "batchgetinode",
      "batchgetxattr",
      "setfsstats",
      "continuesetfsstats",
      "getfsstats",
      "getfspersecondstats",
      "setfsquota",
      "getfsquota",
      "setdirquota",
      "getdirquota",
      "deletedirquota",
      "joinfs",
      "quitfs",
      "joincachegroup",
      "leavecachegroup",
      "listgroups",
      "reweightmember",
      "listmembers",
      "unlockmember",
      "deletemember",
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
    dingofs::mds::client::MDSClient::CreateFsParams params;
    params.partition_type = options.fs_partition_type;
    params.chunk_size = options.chunk_size;
    params.block_size = options.block_size;
    params.s3_info = options.s3_info;
    params.rados_info = options.rados_info;
    params.local_file_info.path = options.storage_path;
    params.fs_id = options.fs_id;
    params.expect_mds_num = options.num;

    mds_client.CreateFs(options.fs_name, params);

  } else if (cmd == Helper::ToLowerCase("DeleteFs")) {
    mds_client.DeleteFs(options.fs_name, options.is_force);

  } else if (cmd == Helper::ToLowerCase("UpdateFs")) {
    mds_client.UpdateFs(options.fs_name, {});

  } else if (cmd == Helper::ToLowerCase("UpdateFsS3Info")) {
    mds_client.UpdateFsS3Info(options.fs_name, options.s3_info);

  } else if (cmd == Helper::ToLowerCase("UpdateFsRadosInfo")) {
    mds_client.UpdateFsRadosInfo(options.fs_name, options.rados_info);

  } else if (cmd == Helper::ToLowerCase("GetFs")) {
    mds_client.GetFs(options.fs_name);

  } else if (cmd == Helper::ToLowerCase("ListFs")) {
    mds_client.ListFs();

  } else if (cmd == Helper::ToLowerCase("MkDir")) {
    mds_client.MkDir(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("BatchMkDir")) {
    std::vector<int64_t> parents;
    dingofs::mds::Helper::SplitString(options.parents, ',', parents);
    mds_client.BatchMkDir(parents, options.prefix, options.num);

  } else if (cmd == Helper::ToLowerCase("MkNod")) {
    mds_client.MkNod(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("BatchMkNod")) {
    std::vector<int64_t> parents;
    dingofs::mds::Helper::SplitString(options.parents, ',', parents);
    mds_client.BatchMkNod(parents, options.prefix, options.num);

  } else if (cmd == Helper::ToLowerCase("GetDentry")) {
    mds_client.GetDentry(options.parent, options.name);

  } else if (cmd == Helper::ToLowerCase("ListDentry")) {
    mds_client.ListDentry(options.parent, false);

  } else if (cmd == Helper::ToLowerCase("GetInode")) {
    mds_client.GetInode(options.parent);

  } else if (cmd == Helper::ToLowerCase("BatchGetInode")) {
    std::vector<int64_t> inos;
    dingofs::mds::Helper::SplitString(options.parents, ',', inos);
    mds_client.BatchGetInode(inos);

  } else if (cmd == Helper::ToLowerCase("BatchGetXattr")) {
    std::vector<int64_t> inos;
    dingofs::mds::Helper::SplitString(options.parents, ',', inos);
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
    dingofs::mds::QuotaEntry quota;
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

    dingofs::mds::QuotaEntry quota;
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
    dingofs::mds::Helper::SplitString(options.mds_id_list, ',', mds_ids);
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
    dingofs::mds::Helper::SplitString(options.mds_id_list, ',', mds_ids);
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
}  // namespace mds
}  // namespace dingofs