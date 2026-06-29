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

#include "tools/mds-cli/mds.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <vector>

#include "common/block/block_key.h"
#include "common/block/block_utils.h"
#include "dingofs/error.pb.h"
#include "dingofs/mds.pb.h"
#include "fmt/format.h"
#include "glog/logging.h"
#include "mds/common/helper.h"
#include "tools/mds-cli/dir_tree_walker.h"
#include "tools/mds-cli/owner_router.h"
#include "tools/mds-cli/trash_restore.h"
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

  auto status =
      interaction_->SendRequest("MDSService", "Heartbeat", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "GetMDSList", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

CreateFsResponse MDSClient::CreateFs(const std::string& fs_name,
                                     const CreateFsParams& params) {
  CreateFsRequest request;
  CreateFsResponse response;

  if (fs_name.empty()) {
    std::cerr << "fs_name is empty\n";
    return response;
  }

  const auto& s3_info = params.s3_info;
  const auto& rados_info = params.rados_info;
  const auto& local_file_info = params.local_file_info;

  if (!s3_info.endpoint.empty()) {
    if (s3_info.ak.empty() || s3_info.sk.empty() ||
        s3_info.bucket_name.empty()) {
      std::cerr << "s3 info is empty.\n";
      return response;
    }

  } else if (!rados_info.mon_host.empty()) {
    if (rados_info.user_name.empty() || rados_info.key.empty() ||
        rados_info.pool_name.empty() || rados_info.cluster_name.empty()) {
      std::cerr << "rados info is empty.\n";
      return response;
    }

  } else if (!local_file_info.path.empty()) {
    if (local_file_info.path.empty()) {
      std::cerr << "local file info is empty.\n";
      return response;
    }

  } else {
    std::cerr << "s3 info and rados info is empty.\n";
    return response;
  }

  if (params.chunk_size == 0) {
    std::cerr << "chunk_size is 0\n";
    return response;
  }
  if (params.block_size == 0) {
    std::cerr << "block_size is 0\n";
    return response;
  }

  request.set_fs_id(params.fs_id);
  request.set_fs_name(fs_name);
  request.set_block_size(params.block_size);
  request.set_chunk_size(params.chunk_size);

  request.set_owner(params.owner);
  request.set_capacity(1024 * 1024 * 1024);
  request.set_recycle_time_hour(1);
  request.set_trash_days(params.trash_days);
  request.set_immediate_trash_quota(params.immediate_trash_quota);
  request.set_enable_uid_gid_map(params.enable_uid_gid_map);
  request.set_enable_dir_stats(params.enable_dir_stats);

  if (params.partition_type == "mono") {
    request.set_partition_type(
        ::dingofs::pb::mds::PartitionType::MONOLITHIC_PARTITION);
  } else if (params.partition_type == "parent_hash") {
    request.set_partition_type(
        ::dingofs::pb::mds::PartitionType::PARENT_ID_HASH_PARTITION);
  }

  if (!s3_info.endpoint.empty()) {
    request.set_fs_type(pb::mds::FsType::S3);
    auto* mut_s3_info = request.mutable_fs_extra()->mutable_s3_info();
    mut_s3_info->set_ak(s3_info.ak);
    mut_s3_info->set_sk(s3_info.sk);
    mut_s3_info->set_endpoint(s3_info.endpoint);
    mut_s3_info->set_bucketname(s3_info.bucket_name);

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

  std::cout << "request: " << request.ShortDebugString() << "\n";

  auto status =
      interaction_->SendRequest("MDSService", "CreateFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "success, fs_id: " << response.fs_info().fs_id() << "\n";
  } else {
    std::cerr << "fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

MountFsResponse MDSClient::MountFs(const std::string& fs_name,
                                   const std::string& client_id) {
  MountFsRequest request;
  MountFsResponse response;

  request.set_fs_name(fs_name);
  auto* mountpoint = request.mutable_mount_point();
  mountpoint->set_client_id(client_id);
  mountpoint->set_hostname("127.0.0.1");
  mountpoint->set_port(10000);
  mountpoint->set_path("/mnt/dingo");

  auto status =
      interaction_->SendRequest("MDSService", "MountFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "MountFs success\n";
  } else {
    std::cerr << "MountFs fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

UmountFsResponse MDSClient::UmountFs(const std::string& fs_name,
                                     const std::string& client_id) {
  UmountFsRequest request;
  UmountFsResponse response;

  request.set_fs_name(fs_name);
  request.set_client_id(client_id);

  auto status =
      interaction_->SendRequest("MDSService", "UmountFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "UmountFs success\n";
  } else {
    std::cerr << "UmountFs fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

DeleteFsResponse MDSClient::DeleteFs(const std::string& fs_name,
                                     bool is_force) {
  DeleteFsRequest request;
  DeleteFsResponse response;

  if (fs_name.empty()) {
    std::cerr << "fs_name is empty\n";
    return response;
  }

  request.set_fs_name(fs_name);
  request.set_is_force(is_force);

  std::cout << "request: " << request.ShortDebugString() << "\n";

  auto status =
      interaction_->SendRequest("MDSService", "DeleteFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  std::cout << "response success: " << response.ShortDebugString() << "\n";

  return response;
}

UpdateFsInfoResponse MDSClient::UpdateFs(const std::string& fs_name,
                                         const pb::mds::FsInfo& fs_info) {
  UpdateFsInfoRequest request;
  UpdateFsInfoResponse response;

  request.set_fs_name(fs_name);

  request.mutable_fs_info()->CopyFrom(fs_info);

  auto status = interaction_->SendRequest("MDSService", "UpdateFsInfo", request,
                                          response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

GetFsInfoResponse MDSClient::GetFs(const std::string& fs_name) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty\n";
    return {};
  }

  GetFsInfoRequest request;
  GetFsInfoResponse response;

  request.set_fs_name(fs_name);

  auto status =
      interaction_->SendRequest("MDSService", "GetFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  std::cout << "response success: " << response.ShortDebugString() << '\n';

  return response;
}

GetFsInfoResponse MDSClient::GetFs(uint32_t fs_id) {
  GetFsInfoRequest request;
  GetFsInfoResponse response;

  if (fs_id == 0) {
    response.mutable_error()->set_errcode(
        dingofs::pb::error::Errno::EILLEGAL_PARAMTETER);
    response.mutable_error()->set_errmsg("fs_id is 0");
    return response;
  }

  request.set_fs_id(fs_id);

  auto status =
      interaction_->SendRequest("MDSService", "GetFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ListFsInfoResponse MDSClient::ListFs() {
  ListFsInfoRequest request;
  ListFsInfoResponse response;

  auto status =
      interaction_->SendRequest("MDSService", "ListFsInfo", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& fs_info : response.fs_infos()) {
    std::cout << "fs_info: " << fs_info.ShortDebugString() << "\n";
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
  request.set_mode(S_IFDIR | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                   S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  request.set_rdev(0);

  auto status =
      interaction_->SendRequest("MDSService", "MkDir", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "success, ino: " << response.inode().ino() << "\n";
  } else {
    std::cerr << "fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

void MDSClient::BatchMkDir(const std::vector<int64_t>& parents,
                           const std::string& prefix, size_t num) {
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

  auto status =
      interaction_->SendRequest("MDSService", "RmDir", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ReadDirResponse MDSClient::ReadDir(Ino ino, const std::string& last_name,
                                   bool with_attr, bool is_refresh,
                                   uint32_t limit) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  ReadDirRequest request;
  ReadDirResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_last_name(last_name);
  request.set_limit(limit);
  request.set_with_attr(with_attr);
  request.set_is_refresh(is_refresh);

  auto status =
      interaction_->SendRequest("MDSService", "ReadDir", request, response);
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
  request.set_mode(S_IFREG | S_IRUSR | S_IWUSR | S_IRGRP | S_IXUSR | S_IWGRP |
                   S_IXGRP | S_IROTH | S_IWOTH | S_IXOTH);
  request.set_rdev(0);

  auto status =
      interaction_->SendRequest("MDSService", "MkNod", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "success, ino: " << response.inode().ino() << "\n";
  } else {
    std::cerr << "fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

void MDSClient::BatchMkNod(const std::vector<int64_t>& parents,
                           const std::string& prefix, size_t num) {
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

  auto status =
      interaction_->SendRequest("MDSService", "GetDentry", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "dentry: " << response.dentry().ShortDebugString() << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "ListDentry", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& dentry : response.dentries()) {
    std::cout << "dentry: " << dentry.ShortDebugString() << "\n";
  }

  return response;
}

ListDentryResponse MDSClient::ListDentryPaged(Ino parent,
                                              const std::string& last,
                                              uint32_t limit,
                                              bool is_only_dir) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ListDentryRequest request;
  ListDentryResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_last(last);
  request.set_limit(limit);
  request.set_is_only_dir(is_only_dir);

  auto status =
      interaction_->SendRequest("MDSService", "ListDentry", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
  }

  return response;
}

GetInodeResponse MDSClient::GetInode(Ino ino, bool bypass_cache) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  GetInodeRequest request;
  GetInodeResponse response;

  request.mutable_context()->set_epoch(epoch_);
  request.mutable_context()->set_is_bypass_cache(bypass_cache);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      interaction_->SendRequest("MDSService", "GetInode", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
  }

  return response;
}

BatchGetInodeResponse MDSClient::BatchGetInode(
    const std::vector<int64_t>& inos) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  BatchGetInodeRequest request;
  BatchGetInodeResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  for (auto ino : inos) {
    request.add_inoes(ino);
  }

  auto status = interaction_->SendRequest("MDSService", "BatchGetInode",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& inode : response.inodes()) {
    std::cout << "inode: " << inode.ShortDebugString() << "\n";
  }

  return response;
}

BatchGetXAttrResponse MDSClient::BatchGetXattr(
    const std::vector<int64_t>& inos) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  BatchGetXAttrRequest request;
  BatchGetXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  for (auto ino : inos) {
    request.add_inoes(ino);
  }

  auto status = interaction_->SendRequest("MDSService", "BatchGetXAttr",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& xattr : response.xattrs()) {
    std::cout << "xattr: " << xattr.ShortDebugString() << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "SetFsStats", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "GetFsStats", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "fs stats: " << response.stats().ShortDebugString() << "\n";
  }
}

void MDSClient::GetFsPerSecondStats(const std::string& fs_name) {
  pb::mds::GetFsPerSecondStatsRequest request;
  pb::mds::GetFsPerSecondStatsResponse response;

  request.set_fs_name(fs_name);

  auto status = interaction_->SendRequest("MDSService", "GetFsPerSecondStats",
                                          request, response);
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
    std::cout << fmt::format("time: {} stats: {}.", utils::FormatTime(time_s),
                             stats.ShortDebugString())
              << "\n";
  }
}

LookupResponse MDSClient::Lookup(Ino parent, const std::string& name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  LookupRequest request;
  LookupResponse response;

  request.set_fs_id(fs_id_);
  request.set_parent(parent);
  request.set_name(name);

  auto status =
      interaction_->SendRequest("MDSService", "Lookup", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

bool MDSClient::ResolvePath(const std::string& path, Ino& out_ino) {
  // For an ABSOLUTE path that exists locally (the DingoFS mount path, e.g.
  // /mnt/dingofs/a/b), its st_ino IS the MDS inode -- use it directly. We only
  // stat absolute paths so a mount-relative arg like "a/b" is never
  // accidentally resolved against an unrelated directory in the current working
  // directory.
  // "/" alone is the mount root; never stat it (that would hit the OS root).
  //
  // Trust the OS st_ino only when the path is genuinely INSIDE the DingoFS
  // mount
  // -- i.e. the path itself or some ancestor reports st_ino==1 (the fs root).
  // Otherwise an unrelated OS path like /a/b that merely happens to exist
  // outside the mount would be silently mis-resolved to its OS inode; such a
  // path falls through to the mount-relative walk below instead.
  if (path.size() > 1 && path.front() == '/') {
    // lstat, not stat: a symlink leaf resolves to the link's own inode
    // (aligned with the `stat` command's lstat default); ancestors below are
    // still followed via stat to locate the mount root.
    struct stat st;
    if (::lstat(path.c_str(), &st) == 0) {
      bool in_mount = (st.st_ino == 1);
      std::string cur = path;
      while (cur.size() > 1 && cur.back() == '/') cur.pop_back();
      while (!in_mount) {
        auto pos = cur.find_last_of('/');
        if (pos == std::string::npos || pos == 0) break;
        cur = cur.substr(0, pos);
        struct stat as;
        if (::stat(cur.c_str(), &as) == 0 && as.st_ino == 1) in_mount = true;
      }
      if (in_mount) {
        out_ino = st.st_ino;
        return true;
      }
    }
  }

  // Otherwise treat it as a path relative to the mount point (== fs root), e.g.
  // "a/b/c", "/a/b/c", or "." / "/" for the root itself. Walk components from
  // the root via Lookup, skipping empty and "." segments.
  Ino cur = 1;  // fs root ino is always 1
  const size_t n = path.size();
  size_t i = 0;
  while (i < n) {
    while (i < n && path[i] == '/') ++i;  // skip slashes
    size_t j = i;
    while (j < n && path[j] != '/') ++j;
    if (j > i) {
      std::string name = path.substr(i, j - i);
      if (name != ".") {
        auto resp = Lookup(cur, name);
        if (resp.error().errcode() != dingofs::pb::error::Errno::OK) {
          std::cerr << fmt::format("resolve path '{}' fail at '{}': {}\n", path,
                                   name, resp.error().errmsg());
          return false;
        }
        cur = resp.inode().ino();
      }
    }
    i = j;
  }
  out_ino = cur;
  return true;
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

  auto status =
      interaction_->SendRequest("MDSService", "Open", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "Release", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

LinkResponse MDSClient::Link(Ino ino, Ino new_parent,
                             const std::string& new_name) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  LinkRequest request;
  LinkResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_new_parent(new_parent);
  request.set_new_name(new_name);

  auto status =
      interaction_->SendRequest("MDSService", "Link", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "UnLink", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

SymlinkResponse MDSClient::Symlink(Ino parent, const std::string& name,
                                   const std::string& symlink) {
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

  auto status =
      interaction_->SendRequest("MDSService", "Symlink", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "ReadLink", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "GetAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "GetAttr success, inode: "
              << response.inode().ShortDebugString() << "\n";
  } else {
    std::cerr << "GetAttr fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

SetAttrResponse MDSClient::SetAttr(Ino ino, uint32_t to_set,
                                   const pb::mds::Inode& inode) {
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

  auto status =
      interaction_->SendRequest("MDSService", "SetAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "SetAttr success\n";
  } else {
    std::cerr << "SetAttr fail, error: " << response.ShortDebugString() << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "GetXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "GetXAttr success, size: " << response.value().size() << "\n";
  } else {
    std::cerr << "GetXAttr fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

SetXAttrResponse MDSClient::SetXAttr(
    Ino ino, const std::map<std::string, std::string>& xattrs) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetXAttrRequest request;
  SetXAttrResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  for (const auto& [k, v] : xattrs) {
    (*request.mutable_xattrs())[k] = v;
  }

  auto status =
      interaction_->SendRequest("MDSService", "SetXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "SetXAttr success\n";
  } else {
    std::cerr << "SetXAttr fail, error: " << response.ShortDebugString()
              << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "RemoveXAttr", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "ListXAttr", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "ListXAttr success, count: " << response.xattrs_size() << "\n";
  } else {
    std::cerr << "ListXAttr fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

RenameResponse MDSClient::Rename(Ino old_parent, const std::string& old_name,
                                 Ino new_parent, const std::string& new_name,
                                 const std::vector<int64_t>& old_ancestors,
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

  auto status =
      interaction_->SendRequest("MDSService", "Rename", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

RestoreFromTrashResponse MDSClient::RestoreFromTrash(
    Ino trash_parent, const std::string& trash_name, uint32_t uid,
    bool allow_trash_parent, uint64_t carried_bytes, uint64_t carried_inodes) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  RestoreFromTrashRequest request;
  RestoreFromTrashResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_trash_parent(trash_parent);
  request.set_trash_name(trash_name);
  request.set_uid(uid);
  request.set_allow_trash_parent(allow_trash_parent);
  request.set_carried_bytes(carried_bytes);
  request.set_carried_inodes(carried_inodes);

  auto status = interaction_->SendRequest("MDSService", "RestoreFromTrash",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

AllocSliceIdResponse MDSClient::AllocSliceId(uint32_t alloc_num,
                                             uint64_t min_slice_id) {
  AllocSliceIdRequest request;
  AllocSliceIdResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_alloc_num(alloc_num);
  request.set_min_slice_id(min_slice_id);

  auto status = interaction_->SendRequest("MDSService", "AllocSliceId", request,
                                          response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

WriteSliceResponse MDSClient::WriteSlice(Ino ino, int64_t chunk_index) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  WriteSliceRequest request;
  WriteSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  mds::DeltaSliceEntry delta_slice;
  delta_slice.set_chunk_index(chunk_index);

  const uint64_t len = 1024;
  for (int i = 0; i < 10; i++) {
    auto* slice = delta_slice.add_slices();
    slice->set_id(i + 100000);
    slice->set_pos(0 * len);
    slice->set_size(0);
    slice->set_off(0);
    slice->set_len(len);
  }

  *request.add_delta_slices() = delta_slice;

  auto status =
      interaction_->SendRequest("MDSService", "WriteSlice", request, response);
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

  auto status =
      interaction_->SendRequest("MDSService", "ReadSlice", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  return response;
}

ReadSliceResponse MDSClient::ReadSliceAll(Ino ino, int64_t chunk_num) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  ReadSliceRequest request;
  ReadSliceResponse response;

  request.mutable_context()->set_epoch(epoch_);
  // Read the authoritative store, not a possibly-stale chunk cache snapshot.
  request.mutable_context()->set_is_bypass_cache(true);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  for (int64_t i = 0; i < chunk_num; ++i) {
    request.add_chunk_descriptors()->set_index(i);
  }

  auto status =
      interaction_->SendRequest("MDSService", "ReadSlice", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
  }

  return response;
}

SetFsQuotaResponse MDSClient::SetFsQuota(const QuotaEntry& quota) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  SetFsQuotaRequest request;
  SetFsQuotaResponse response;

  request.set_fs_id(fs_id_);
  request.mutable_quota()->CopyFrom(quota);
  auto status =
      interaction_->SendRequest("MDSService", "SetFsQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "SetFsQuota success\n";
  } else {
    std::cerr << "SetFsQuota fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

GetFsQuotaResponse MDSClient::GetFsQuota() {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";

  GetFsQuotaRequest request;
  GetFsQuotaResponse response;

  request.set_fs_id(fs_id_);

  auto status =
      interaction_->SendRequest("MDSService", "GetFsQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "GetFsQuota success, quota: "
              << response.quota().ShortDebugString() << "\n";
  } else {
    std::cerr << "GetFsQuota fail, error: " << response.ShortDebugString()
              << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "SetDirQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "SetDirQuota success\n";
  } else {
    std::cerr << "SetDirQuota fail, error: " << response.ShortDebugString()
              << "\n";
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

  auto status =
      interaction_->SendRequest("MDSService", "GetDirQuota", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "GetDirQuota success, quota: "
              << response.quota().ShortDebugString() << "\n";
  } else {
    std::cerr << "GetDirQuota fail, error: " << response.ShortDebugString()
              << "\n";
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

  auto status = interaction_->SendRequest("MDSService", "DeleteDirQuota",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "DeleteDirQuota success\n";
  } else {
    std::cerr << "DeleteDirQuota fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

JoinFsResponse MDSClient::JoinFs(const std::string& fs_name, uint32_t fs_id,
                                 const std::vector<int64_t>& mds_ids) {
  JoinFsRequest request;
  JoinFsResponse response;

  request.set_fs_id(fs_id);
  request.set_fs_name(fs_name);
  for (const auto& mds_id : mds_ids) {
    request.add_mds_ids(mds_id);
  }

  std::cout << "JoinFs request: " << request.ShortDebugString();

  auto status =
      interaction_->SendRequest("MDSService", "JoinFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "JoinFs success\n";
  } else {
    std::cerr << "JoinFs fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

QuitFsResponse MDSClient::QuitFs(const std::string& fs_name, uint32_t fs_id,
                                 const std::vector<int64_t>& mds_ids) {
  QuitFsRequest request;
  QuitFsResponse response;

  request.set_fs_id(fs_id);
  request.set_fs_name(fs_name);
  for (const auto& mds_id : mds_ids) {
    request.add_mds_ids(mds_id);
  }

  auto status =
      interaction_->SendRequest("MDSService", "QuitFs", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "QuitFs success\n";
  } else {
    std::cerr << "QuitFs fail, error: " << response.ShortDebugString() << "\n";
  }

  return response;
}

// cache member operations
JoinCacheGroupResponse MDSClient::JoinCacheGroup(const std::string& member_id,
                                                 const std::string& ip,
                                                 uint32_t port,
                                                 const std::string& group_name,
                                                 uint32_t weight) {
  JoinCacheGroupRequest request;
  JoinCacheGroupResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);
  request.set_weight(weight);

  auto status = interaction_->SendRequest("MDSService", "JoinCacheGroup",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "JoinCacheGroup success\n";
  } else {
    std::cerr << "JoinCacheGroup fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

LeaveCacheGroupResponse MDSClient::LeaveCacheGroup(
    const std::string& member_id, const std::string& ip, uint32_t port,
    const std::string& group_name) {
  LeaveCacheGroupRequest request;
  LeaveCacheGroupResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_group_name(group_name);

  auto status = interaction_->SendRequest("MDSService", "LeaveCacheGroup",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "LeaveCacheGroup success\n";
  } else {
    std::cerr << "LeaveCacheGroup fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

ListGroupsResponse MDSClient::ListGroups() {
  ListGroupsRequest request;
  ListGroupsResponse response;

  auto status =
      interaction_->SendRequest("MDSService", "ListGroups", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  for (const auto& group_name : response.group_names()) {
    std::cout << "group_name: " << group_name << "\n";
  }

  return response;
}

ReweightMemberResponse MDSClient::ReweightMember(const std::string& member_id,
                                                 const std::string& ip,
                                                 uint32_t port,
                                                 uint32_t weight) {
  ReweightMemberRequest request;
  ReweightMemberResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);
  request.set_weight(weight);

  auto status = interaction_->SendRequest("MDSService", "ReweightMember",
                                          request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "ReweightMember success\n";
  } else {
    std::cerr << "ReweightMember fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

ListMembersResponse MDSClient::ListMembers(const std::string& group_name) {
  ListMembersRequest request;
  ListMembersResponse response;

  request.set_group_name(group_name);

  auto status =
      interaction_->SendRequest("MDSService", "ListMembers", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() != dingofs::pb::error::Errno::OK) {
    std::cerr << "ListMembers fail, error: " << response.ShortDebugString()
              << "\n";
  }

  for (const auto& member : response.members()) {
    std::cout << "cache_member: " << member.ShortDebugString() << "\n";
  }

  return response;
}

UnLockMemberResponse MDSClient::UnlockMember(const std::string& member_id,
                                             const std::string& ip,
                                             uint32_t port) {
  UnLockMemberRequest request;
  UnLockMemberResponse response;

  request.set_member_id(member_id);
  request.set_ip(ip);
  request.set_port(port);

  auto status = interaction_->SendRequest("MDSService", "UnlockMember", request,
                                          response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "UnlockMember success\n";
  } else {
    std::cerr << "UnlockMember fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

DeleteMemberResponse MDSClient::DeleteMember(const std::string& member_id) {
  DeleteMemberRequest request;
  DeleteMemberResponse response;

  request.set_member_id(member_id);

  auto status = interaction_->SendRequest("MDSService", "DeleteMember", request,
                                          response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    std::cout << "DeleteMember success\n";
  } else {
    std::cerr << "DeleteMember fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

void MDSClient::UpdateFsS3Info(const std::string& fs_name,
                               const S3Info& s3_info) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty"
              << "\n";
    return;
  }
  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    std::cerr << "not found fs: " << fs_name << "\n";
    return;
  }
  if (fs_info.fs_type() != pb::mds::FsType::S3) {
    std::cerr << "fs type is not S3, fs_type: " << fs_info.fs_type() << "\n";
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

void MDSClient::UpdateFsRadosInfo(const std::string& fs_name,
                                  const RadosInfo& rados_info) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty"
              << "\n";
    return;
  }

  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    std::cerr << "not found fs: " << fs_name << "\n";
    return;
  }
  if (fs_info.fs_type() != pb::mds::FsType::RADOS) {
    std::cerr << "fs type is not RADOS, fs_type: " << fs_info.fs_type() << "\n";
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

void MDSClient::UpdateFsTrashDays(const std::string& fs_name,
                                  uint32_t trash_days) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty"
              << "\n";
    return;
  }

  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    std::cerr << "not found fs: " << fs_name << "\n";
    return;
  }

  fs_info.set_trash_days(trash_days);
  UpdateFs(fs_name, fs_info);
}

void MDSClient::UpdateFsEnableUidGidMap(const std::string& fs_name,
                                        bool enable) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty\n";
    return;
  }

  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    std::cerr << "not found fs: " << fs_name << "\n";
    return;
  }

  // Read-modify-write: every UpdateFsInfo call must round-trip the full
  // current FsInfo. Only flip the one field; the server merges all known
  // runtime-mutable fields including this one (store_operation.cc:594).
  fs_info.set_enable_uid_gid_map(enable);
  UpdateFs(fs_name, fs_info);
}

void MDSClient::UpdateFsEnableDirStats(const std::string& fs_name,
                                       bool enable) {
  if (fs_name.empty()) {
    std::cerr << "fs_name is empty\n";
    return;
  }

  auto fs_response = GetFs(fs_name);
  pb::mds::FsInfo fs_info;
  fs_info.CopyFrom(fs_response.fs_info());
  if (fs_info.fs_id() == 0) {
    std::cerr << "not found fs: " << fs_name << "\n";
    return;
  }

  // Read-modify-write: round-trip the full current FsInfo and flip only this
  // one runtime-mutable field; the server merges it (store_operation.cc).
  fs_info.set_enable_dir_stats(enable);
  UpdateFs(fs_name, fs_info);
}

GetDirStatResponse MDSClient::GetDirStat(Ino ino) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  GetDirStatRequest request;
  GetDirStatResponse response;

  request.set_fs_id(fs_id_);
  request.set_ino(ino);

  auto status =
      interaction_->SendRequest("MDSService", "GetDirStat", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
    return response;
  }

  if (response.error().errcode() != dingofs::pb::error::Errno::OK) {
    std::cerr << "GetDirStat fail, error: " << response.ShortDebugString()
              << "\n";
  }

  return response;
}

namespace {

// Client-side upper bounds for `summary` knobs: a deeper/wider tree walk is
// costly server-side and the rendered table is unreadable past these. Values
// above are clamped (with a warning) rather than rejected.
constexpr uint32_t kMaxSummaryDepth = 10;
constexpr uint32_t kMaxSummaryEntries = 100;

// Human-readable size in 1024-based units, matching `juicefs summary`
// (go-humanize IBytes): one decimal below 10 of a unit, none at/above.
std::string HumanizeIBytes(uint64_t s) {
  constexpr double kBase = 1024.0;
  static const char* kSizes[] = {"B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"};
  if (s < 1024) return fmt::format("{} B", s);
  double val = static_cast<double>(s);
  int i = 0;
  while (val >= kBase && i < 6) {
    val /= kBase;
    ++i;
  }
  return val < 10.0 ? fmt::format("{:.1f} {}", val, kSizes[i])
                    : fmt::format("{:.0f} {}", val, kSizes[i]);
}

struct SummaryRow {
  std::string path;
  std::string length;
  std::string dirs;
  std::string files;
};

// Depth-first preorder flatten into table rows. Each row's PATH is the node's
// full path: the queried directory (`path`) for the root, and `path/<name>`
// for descendants (the synthetic "..." aggregate becomes `path/...`).
void FlattenTree(const pb::mds::TreeSummary& node, const std::string& path,
                 std::vector<SummaryRow>& rows) {
  rows.push_back({path, HumanizeIBytes(node.length()),
                  std::to_string(node.dirs()), std::to_string(node.files())});
  for (const auto& c : node.children()) {
    // When this node is the mount root (shown as "/" or "."), children are bare
    // mount-relative names ("dir11"); otherwise append under the parent path.
    std::string child_path =
        (path == "/" || path == ".") ? c.path() : path + "/" + c.path();
    FlattenTree(c, child_path, rows);
  }
}

void PrintSummaryTable(const std::vector<SummaryRow>& rows) {
  size_t wp = 4, ws = 6, wd = 4,
         wf = 5;  // header widths: PATH/LENGTH/DIRS/FILES
  for (const auto& r : rows) {
    wp = std::max(wp, r.path.size());
    ws = std::max(ws, r.length.size());
    wd = std::max(wd, r.dirs.size());
    wf = std::max(wf, r.files.size());
  }
  auto border = [&]() {
    std::cout << fmt::format("+{}+{}+{}+{}+\n", std::string(wp + 2, '-'),
                             std::string(ws + 2, '-'), std::string(wd + 2, '-'),
                             std::string(wf + 2, '-'));
  };
  border();
  std::cout << fmt::format("| {:^{}} | {:^{}} | {:^{}} | {:^{}} |\n", "PATH",
                           wp, "LENGTH", ws, "DIRS", wd, "FILES", wf);
  border();
  for (const auto& r : rows) {
    std::cout << fmt::format("| {:<{}} | {:>{}} | {:>{}} | {:>{}} |\n", r.path,
                             wp, r.length, ws, r.dirs, wd, r.files, wf);
  }
  border();
}

// Render a `summary` tree response as a table. The root row's PATH is the
// queried directory (`base_path`); descendants append under it. Falls back to
// "/" when only --ino was given, and normalizes the mount root.
void PrintTreeTable(const pb::mds::TreeSummary& tree,
                    const std::string& base_path) {
  std::string base = base_path.empty() ? "/" : base_path;
  while (base.size() > 1 && base.back() == '/') base.pop_back();
  if (base == ".") base = "/";

  std::vector<SummaryRow> rows;
  FlattenTree(tree, base, rows);
  PrintSummaryTable(rows);
}

// Canonical whole-system absolute path of the user-supplied --path (e.g.
// /mnt/dingofs/a/b). Empty when --path was not given (--ino mode).
// The FINAL component is never resolved through realpath: a symlink leaf must
// keep its own name so `info` inspects the link itself, matching the lstat
// default of the `stat` command (see ResolvePath).
std::string AbsOsPath(const std::string& input) {
  if (input.empty()) return "";
  std::string p = input;
  while (p.size() > 1 && p.back() == '/') p.pop_back();
  char buf[PATH_MAX];
  auto pos = p.find_last_of('/');
  std::string leaf = (pos == std::string::npos) ? p : p.substr(pos + 1);
  // "." / ".." / "/" leaves have no identity of their own -- resolve fully.
  if (leaf.empty() || leaf == "." || leaf == "..") {
    if (::realpath(p.c_str(), buf) != nullptr) return buf;
    return input;
  }
  const std::string dir =
      (pos == std::string::npos) ? "." : (pos == 0 ? "/" : p.substr(0, pos));
  if (::realpath(dir.c_str(), buf) != nullptr) {
    std::string d = buf;
    return (d == "/") ? d + leaf : d + "/" + leaf;
  }
  return input;  // fall back to the raw arg if realpath fails
}

// Mount-relative absolute fs path (e.g. /a/b) derived from a whole-system
// absolute path: walk ancestors and find the mount root -- the DingoFS root
// inode is always 1 (see ResolvePath) -- then strip that prefix. Empty when
// os_path is empty or no ancestor reports st_ino==1.
std::string MountRelPath(const std::string& os_path) {
  if (os_path.empty()) return "";
  std::string p = os_path;
  while (p.size() > 1 && p.back() == '/') p.pop_back();
  std::string mount;
  for (std::string cur = p;;) {
    struct stat st;
    if (::stat(cur.c_str(), &st) == 0 && st.st_ino == 1) {
      mount = cur;
      break;
    }
    auto pos = cur.find_last_of('/');
    if (pos == std::string::npos || pos == 0) break;
    cur = cur.substr(0, pos);
  }
  if (mount.empty()) return "";
  std::string rel = p.substr(mount.size());
  return rel.empty() ? "/" : rel;
}

// JuiceFS-style `info` header block (size/tier dimensions intentionally
// dropped). Header line shows the whole-system absolute path; `path:` shows the
// mount-relative fs path. Either is omitted when unknown (--ino mode).
void PrintInfoBlock(const std::string& os_path, const std::string& fs_path,
                    Ino ino, int64_t files, int64_t dirs, int64_t length) {
  if (!os_path.empty()) {
    std::cout << fmt::format("{} :\n", os_path);
  } else {
    std::cout << fmt::format("inode {} :\n", ino);
  }
  std::cout << fmt::format("  inode: {}\n", ino);
  std::cout << fmt::format("  files: {}\n", files);
  std::cout << fmt::format("   dirs: {}\n", dirs);
  std::cout << fmt::format(" length: {} ({} Bytes)\n", HumanizeIBytes(length),
                           length);
  if (!fs_path.empty()) {
    std::cout << fmt::format("   path: {}\n", fs_path);
  }
}

// `info` on a single file, raw view: one row per slice (sliceId dimension),
// matching `juicefs info --raw`.
// Render a bordered table with dynamic column widths (header included).
// `aligns` has one char per column: '<' left-justify, '>' right-justify.
void PrintTable(const std::vector<std::string>& headers,
                const std::vector<std::vector<std::string>>& rows,
                const std::string& aligns) {
  const size_t n = headers.size();
  std::vector<size_t> w(n);
  for (size_t i = 0; i < n; ++i) w[i] = headers[i].size();
  for (const auto& r : rows) {
    for (size_t i = 0; i < n; ++i) w[i] = std::max(w[i], r[i].size());
  }
  auto border = [&]() {
    std::string s = "+";
    for (size_t i = 0; i < n; ++i) s += std::string(w[i] + 2, '-') + "+";
    std::cout << s << "\n";
  };
  border();
  std::string h = "|";
  for (size_t i = 0; i < n; ++i)
    h += fmt::format(" {:^{}} |", headers[i], w[i]);
  std::cout << h << "\n";
  border();
  for (const auto& r : rows) {
    std::string line = "|";
    for (size_t i = 0; i < n; ++i) {
      line += (aligns[i] == '<') ? fmt::format(" {:<{}} |", r[i], w[i])
                                 : fmt::format(" {:>{}} |", r[i], w[i]);
    }
    std::cout << line << "\n";
  }
  border();
}

void PrintChunksTable(
    const google::protobuf::RepeatedPtrField<pb::mds::Chunk>& chunks) {
  std::cout << " chunks:\n";
  std::vector<std::vector<std::string>> rows;
  for (const auto& chunk : chunks) {
    for (const auto& slice : chunk.slices()) {
      rows.push_back({std::to_string(chunk.index()), std::to_string(slice.id()),
                      std::to_string(slice.size()), std::to_string(slice.pos()),
                      std::to_string(slice.len())});
    }
  }
  PrintTable({"chunkIndex", "sliceId", "size", "offset", "length"}, rows,
             ">>>>>");
}

// `info` on a single file, default view: one row per object (block) in the
// object store, matching `juicefs info`. objectName is computed client-side via
// BlockKey (the MDS stores only slice ids). pos is the object's start offset
// within the whole file. chunk_size/block_size come from FsInfo so the layout
// is correct even when a stored chunk omits them.
void PrintObjectsTable(
    const google::protobuf::RepeatedPtrField<pb::mds::Chunk>& chunks,
    uint64_t chunk_size, uint32_t block_size) {
  std::cout << " objects:\n";
  std::vector<std::vector<std::string>> rows;
  for (const auto& chunk : chunks) {
    for (const auto& slice : chunk.slices()) {
      uint64_t base = chunk.index() * chunk_size + slice.pos();
      for (const auto& bk :
           EnumerateBlockKeys(slice.id(), slice.size(), block_size)) {
        uint64_t pos = base + static_cast<uint64_t>(bk.index) * block_size;
        rows.push_back({std::to_string(chunk.index()), bk.StoreKey(),
                        std::to_string(bk.size), std::to_string(pos)});
      }
    }
  }
  PrintTable({"chunkIndex", "objectName", "size", "pos"}, rows, "><>>");
}

// Resolve the target inode for a dir-stats subcommand: prefer --path (absolute
// fs path resolved via Lookup) and fall back to --ino. Returns false (after
// printing why) when neither yields a usable inode.
bool ResolveDirStatIno(MDSClient& client,
                       const MdsCommandRunner::Options& options, Ino& ino) {
  if (!options.path.empty()) {
    return client.ResolvePath(options.path, ino);
  }
  if (ino == 0) {
    std::cout << "specify a directory with --path=/a/b/c or --ino=N.\n";
    return false;
  }
  return true;
}

// Build the owner router and walk options shared by the client-side dir-stat
// commands (info -r / summary / syncdirstat). Returns false (and prints) when
// the router cannot be built -- a multi-RPC walk must not silently fall back to
// a single fixed MDS, which would scatter stale, mis-routed reads across the
// tree.
bool SetupWalk(MDSClient& mds_client, uint32_t fs_id,
               const MdsCommandRunner::Options& options, OwnerRouter& router,
               WalkOptions& wopts) {
  auto fs_resp = mds_client.GetFs(fs_id);
  if (fs_resp.error().errcode() != dingofs::pb::error::Errno::OK) {
    std::cerr << "get fs info fail: " << fs_resp.ShortDebugString() << "\n";
    return false;
  }
  if (!router.Init(fs_id, mds_client)) {
    std::cerr << "cannot resolve owner mds routing; aborting walk\n";
    return false;
  }
  wopts.strict = options.strict;
  wopts.dirstats_enabled = fs_resp.fs_info().enable_dir_stats();
  wopts.threads = options.dir_threads;
  return true;
}

// Print the JuiceFS-style break lines from a (whole-tree) syncdirstat walk.
void PrintSyncDirStatResult(
    const std::vector<pb::mds::DirStatMismatch>& mismatches, bool repair) {
  if (mismatches.empty()) {
    std::cout << "all dir-stats are consistent\n";
    return;
  }
  for (const auto& b : mismatches) {
    // Report recomputed (want) vs stored (got); {inodes, length} -- DingoFS has
    // no separate size dimension.
    std::cout << fmt::format(
        "usage stat of inode {} should be {{inodes:{} length:{}}}, but got "
        "{{inodes:{} length:{}}}{}\n",
        b.ino(), b.want_inodes(), b.want_length(), b.got_inodes(),
        b.got_length(), b.found() ? "" : " (no stored record)");
    std::cout << (repair ? fmt::format(
                               "  stat of inode {} is successfully synced\n",
                               b.ino())
                         : fmt::format("  stat of inode {} should be synced, "
                                       "re-run with --repair to fix it\n",
                                       b.ino()));
  }
}

void HandleSummary(MDSClient& mds_client, uint32_t fs_id,
                   const MdsCommandRunner::Options& options) {
  Ino ino = options.ino;
  if (!ResolveDirStatIno(mds_client, options, ino)) return;

  uint32_t depth = options.depth;
  if (depth > kMaxSummaryDepth) {
    std::cerr << fmt::format("warn: depth should be less than {}\n",
                             kMaxSummaryDepth + 1);
    depth = kMaxSummaryDepth;
  }
  uint32_t entries = options.entries;
  if (entries > kMaxSummaryEntries) {
    std::cerr << fmt::format("warn: entries should be less than {}\n",
                             kMaxSummaryEntries + 1);
    entries = kMaxSummaryEntries;
  }

  OwnerRouter router;
  WalkOptions wopts;
  if (!SetupWalk(mds_client, fs_id, options, router, wopts)) return;
  pb::mds::TreeSummary tree;
  if (!WalkTree(router, ino, wopts, depth, entries, tree)) {
    // Fail loud: a summary is only useful if it is complete. Rather than print
    // a silently-undercounted tree behind a warning, abort so a skipped subtree
    // (an unreachable/erroring owner mds) can never be mistaken for the truth.
    std::cerr << "summary: directory walk failed (an owner mds was unreachable "
                 "or errored); result would be incomplete, aborting\n";
    return;
  }
  PrintTreeTable(tree, AbsOsPath(options.path));
}

// Check (and optionally repair) a single directory's dir-stat. Routes to the
// owner and hard-fails on any error: a single level has no partial result to
// salvage, so there is nothing to print but the failure itself.
void SyncDirStatOneLevel(OwnerRouter& router, Ino ino, bool repair) {
  MDSClient* owner = router.ClientForIno(ino);
  if (owner == nullptr) {
    std::cerr << fmt::format("no owner mds for ino({})\n", ino);
    return;
  }
  auto resp = owner->SyncDirStat(ino, repair);
  if (resp.error().errcode() != dingofs::pb::error::Errno::OK) {
    std::cerr << "syncdirstat fail, error: " << resp.ShortDebugString() << "\n";
    return;
  }
  std::vector<pb::mds::DirStatMismatch> mismatches(resp.mismatches().begin(),
                                                   resp.mismatches().end());
  PrintSyncDirStatResult(mismatches, repair);
}

void HandleSyncDirStat(MDSClient& mds_client, uint32_t fs_id,
                       const MdsCommandRunner::Options& options) {
  Ino ino = options.ino;
  if (!ResolveDirStatIno(mds_client, options, ino)) return;
  OwnerRouter router;
  WalkOptions wopts;
  if (!SetupWalk(mds_client, fs_id, options, router, wopts)) return;

  if (!options.recursive) {
    SyncDirStatOneLevel(router, ino, options.repair);
    return;
  }

  // Recursive: tolerate per-directory errors instead of aborting, so a --repair
  // that already fixed some directories still reports them. An error only flags
  // the walk as incomplete (warning), never discards the results gathered so
  // far.
  std::vector<pb::mds::DirStatMismatch> mismatches;
  bool complete =
      WalkSyncDirStat(router, ino, wopts, options.repair, mismatches);
  PrintSyncDirStatResult(mismatches, options.repair);
  if (!complete)
    std::cerr
        << "warn: directory walk incomplete; some directories were skipped\n";
}

void HandleInfo(MDSClient& mds_client, uint32_t fs_id,
                const MdsCommandRunner::Options& options) {
  // `info`: directory -> single-level stat (--strict: authoritative dentry
  // scan; -r: recursive subtree aggregate); file -> data layout (objects, or
  // chunks/slices with --raw).
  Ino ino = options.ino;
  if (!ResolveDirStatIno(mds_client, options, ino)) return;
  const std::string os_path = AbsOsPath(options.path);
  const std::string fs_path = MountRelPath(os_path);

  // Bypass the serving MDS's inode cache: `info` accepts any --mds_addr, and a
  // non-owner MDS would otherwise serve a stale length forever (writes only
  // refresh the owner's cache).
  auto inode_resp = mds_client.GetInode(ino, /*bypass_cache=*/true);
  if (inode_resp.error().errcode() != dingofs::pb::error::Errno::OK) {
    std::cerr << "info: get inode fail, error: "
              << inode_resp.ShortDebugString() << "\n";
    return;
  }
  const auto& inode = inode_resp.inode();

  if (inode.type() == pb::mds::FileType::DIRECTORY) {
    // One owner-routed setup serves all three reads (fail-loud: a dir-stat read
    // never falls back to a fixed --mds_addr, which would scatter stale/mis-
    // routed values under multi-mds).
    OwnerRouter router;
    WalkOptions wopts;
    if (!SetupWalk(mds_client, fs_id, options, router, wopts)) return;

    DirAgg agg;
    bool ok;
    if (options.recursive) {
      ok = WalkAggregate(router, ino, wopts, agg);  // counts root itself
    } else if (options.strict) {
      ok = ReadDirStatStrict(router, ino, agg);  // authoritative dentry scan
    } else {
      ok = ReadDirStatFast(router, ino, agg);  // maintained counter
    }
    if (!ok) {
      // Fail loud: an incomplete read (a skipped subtree under -r, or a failed
      // single-level stat) would print a silently-wrong number. Abort instead.
      std::cerr
          << "info: dir-stat read failed (an owner mds was unreachable or "
             "errored); result would be incomplete, aborting\n";
      return;
    }
    PrintInfoBlock(os_path, fs_path, ino, agg.files, agg.dirs, agg.length);
  } else {
    // file / symlink: header block, then (for regular files) the data layout.
    PrintInfoBlock(os_path, fs_path, ino, 1, 0,
                   static_cast<int64_t>(inode.length()));
    if (inode.type() == pb::mds::FileType::FILE && inode.length() > 0) {
      auto fs_resp = mds_client.GetFs(fs_id);
      if (fs_resp.error().errcode() != dingofs::pb::error::Errno::OK) {
        std::cerr << "info: get fs info fail, error: "
                  << fs_resp.ShortDebugString() << "\n";
        return;
      }
      const uint64_t chunk_size = fs_resp.fs_info().chunk_size();
      const uint32_t block_size = fs_resp.fs_info().block_size();
      if (chunk_size == 0 || block_size == 0) {
        std::cerr << "info: fs chunk_size/block_size is 0\n";
        return;
      }
      const int64_t chunk_num = (inode.length() + chunk_size - 1) / chunk_size;
      // ReadSlice is epoch-validated on the MDS; without a fresh partition
      // epoch the default 0 is treated as 1 and rejected once the fs has been
      // rebalanced (JoinFs/QuitFs bump the epoch). Seed it from the fs_info we
      // just fetched so `info <file>` works after any partition change.
      mds_client.SetEpoch(fs_resp.fs_info().partition_policy().epoch());
      auto rs = mds_client.ReadSliceAll(ino, chunk_num);
      if (rs.error().errcode() != dingofs::pb::error::Errno::OK) {
        std::cerr << "info: read slice fail, error: " << rs.ShortDebugString()
                  << "\n";
        return;
      }
      if (options.raw) {
        PrintChunksTable(rs.chunks());
      } else {
        PrintObjectsTable(rs.chunks(), chunk_size, block_size);
      }
    }
  }
}

}  // namespace

SyncDirStatResponse MDSClient::SyncDirStat(Ino ino, bool repair) {
  CHECK(fs_id_ > 0) << "fs_id_ is zero";
  CHECK(ino > 0) << "ino is zero";

  SyncDirStatRequest request;
  SyncDirStatResponse response;

  request.mutable_context()->set_epoch(epoch_);

  request.set_fs_id(fs_id_);
  request.set_ino(ino);
  request.set_repair(repair);
  // Single-level check/repair: the server reads the authoritative store
  // (critical for repair=true, so it never repairs off a stale cache). The
  // recursive walk lives in the CLI (dir_tree_walker), which calls this per
  // directory routed to the owning MDS.
  request.mutable_context()->set_is_bypass_cache(true);

  auto status =
      interaction_->SendRequest("MDSService", "SyncDirStat", request, response);
  if (!status.ok()) {
    response.mutable_error()->set_errcode(dingofs::pb::error::Errno::EINTERNAL);
    response.mutable_error()->set_errmsg(status.error_str());
  }

  return response;
}

bool MdsCommandRunner::Run(const Options& options, const std::string& mds_addr,
                           const std::string& cmd, uint32_t fs_id) {
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
      "restoretrash",
      "updatefstrashdays",
      "updatefsenableuidgidmap",
      "updatefsenabledirstats",
      "info",
      "summary",
      "syncdirstat",
  };

  if (mds_cmd.count(cmd) == 0) return false;

  if (mds_addr.empty()) {
    std::cout << "mds_addr is empty.\n";
    return true;
  }

  MDSClient mds_client(fs_id);
  if (!mds_client.Init(mds_addr)) {
    std::cout << "init interaction fail.\n";
    return true;
  }

  if (cmd == Helper::ToLowerCase("GetMdsList")) {
    auto response = mds_client.GetMdsList();
    for (const auto& mds : response.mdses()) {
      std::cout << "mds: " << mds.ShortDebugString() << "\n";
    }

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
    params.trash_days = options.trash_days;
    params.immediate_trash_quota = options.immediate_trash_quota;
    params.enable_uid_gid_map = options.enable_uid_gid_map;
    params.enable_dir_stats = options.enable_dir_stats;

    mds_client.CreateFs(options.fs_name, params);

  } else if (cmd == Helper::ToLowerCase("DeleteFs")) {
    mds_client.DeleteFs(options.fs_name, options.is_force);

  } else if (cmd == Helper::ToLowerCase("UpdateFs")) {
    mds_client.UpdateFs(options.fs_name, {});

  } else if (cmd == Helper::ToLowerCase("UpdateFsS3Info")) {
    mds_client.UpdateFsS3Info(options.fs_name, options.s3_info);

  } else if (cmd == Helper::ToLowerCase("UpdateFsRadosInfo")) {
    mds_client.UpdateFsRadosInfo(options.fs_name, options.rados_info);

  } else if (cmd == Helper::ToLowerCase("UpdateFsTrashDays")) {
    mds_client.UpdateFsTrashDays(options.fs_name, options.trash_days);

  } else if (cmd == Helper::ToLowerCase("UpdateFsEnableUidGidMap")) {
    mds_client.UpdateFsEnableUidGidMap(options.fs_name,
                                       options.enable_uid_gid_map);

  } else if (cmd == Helper::ToLowerCase("UpdateFsEnableDirStats")) {
    mds_client.UpdateFsEnableDirStats(options.fs_name,
                                      options.enable_dir_stats);

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
    if (options.ino == 0) {
      std::cout << "ino is empty.\n";
      return true;
    }
    auto resp = mds_client.GetInode(options.ino);
    if (resp.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "inode: " << resp.inode().ShortDebugString() << "\n";
    } else {
      std::cerr << "GetInode fail, error: " << resp.ShortDebugString() << "\n";
    }

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
      std::cout << "ino is empty.\n";
      return true;
    }

    dingofs::mds::QuotaEntry quota;
    quota.set_max_bytes(options.max_bytes);
    quota.set_max_inodes(options.max_inodes);

    mds_client.SetDirQuota(options.ino, quota);

  } else if (cmd == Helper::ToLowerCase("GetDirQuota")) {
    if (options.ino == 0) {
      std::cout << "ino is empty.\n";
      return true;
    }

    auto response = mds_client.GetDirQuota(options.ino);
    std::cout << "dir quota: " << response.quota().ShortDebugString() << '\n';

  } else if (cmd == Helper::ToLowerCase("DeleteDirQuota")) {
    if (options.ino == 0) {
      std::cout << "ino is empty.\n";
      return true;
    }
    mds_client.DeleteDirQuota(options.ino);
  } else if (cmd == Helper::ToLowerCase("JoinFs")) {
    if (options.fs_name.empty() && options.fs_id == 0) {
      std::cout << "fs_name and fs_id is empty.\n";
      return true;
    }

    if (options.mds_id_list.empty()) {
      std::cout << "mds_id_list is empty.\n";
      return true;
    }

    std::vector<int64_t> mds_ids;
    dingofs::mds::Helper::SplitString(options.mds_id_list, ',', mds_ids);
    auto response = mds_client.JoinFs(options.fs_name, options.fs_id, mds_ids);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "joinfs success." << '\n';
    } else {
      std::cout << "joinfs fail, error: " << response.ShortDebugString()
                << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("QuitFs")) {
    if (options.fs_name.empty() && options.fs_id == 0) {
      std::cout << "fs_name and fs_id is empty.\n";
      return true;
    }

    if (options.mds_id_list.empty()) {
      std::cout << "mds_id_list is empty.\n";
      return true;
    }

    std::vector<int64_t> mds_ids;
    dingofs::mds::Helper::SplitString(options.mds_id_list, ',', mds_ids);
    auto response = mds_client.QuitFs(options.fs_name, options.fs_id, mds_ids);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "quitfs success." << '\n';
    } else {
      std::cout << "quitfs fail, error: " << response.ShortDebugString()
                << '\n';
    }
  } else if (cmd == Helper::ToLowerCase("JoinCacheGroup")) {
    auto response =
        mds_client.JoinCacheGroup(options.member_id, options.ip, options.port,
                                  options.group_name, options.weight);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "joincachegroup success." << '\n';
    } else {
      std::cout << "joincachegroup fail, error: " << response.ShortDebugString()
                << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("LeaveCacheGroup")) {
    auto response = mds_client.LeaveCacheGroup(
        options.member_id, options.ip, options.port, options.group_name);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "leavecachegroup success.\n";
    } else {
      std::cout << "leavecachegroup fail, error: "
                << response.ShortDebugString() << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("ReweightMember")) {
    auto response = mds_client.ReweightMember(options.member_id, options.ip,
                                              options.port, options.weight);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "reweightmember success.\n";
    } else {
      std::cout << "reweightmember fail, error: " << response.ShortDebugString()
                << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("ListGroups")) {
    auto response = mds_client.ListGroups();

  } else if (cmd == Helper::ToLowerCase("ListMembers")) {
    auto response = mds_client.ListMembers(options.group_name);

  } else if (cmd == Helper::ToLowerCase("UnlockMember")) {
    auto response =
        mds_client.UnlockMember(options.member_id, options.ip, options.port);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "unlockmember success.\n";
    } else {
      std::cout << "unlockmember fail, error: " << response.ShortDebugString()
                << '\n';
    }
  } else if (cmd == Helper::ToLowerCase("DeleteMember")) {
    auto response = mds_client.DeleteMember(options.member_id);
    if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
      std::cout << "deletemember success.\n";
    } else {
      std::cout << "deletemember fail, error: " << response.ShortDebugString()
                << '\n';
    }

  } else if (cmd == Helper::ToLowerCase("RestoreTrash")) {
    TrashRestore::Options restore_options;
    restore_options.fs_id = options.fs_id;
    restore_options.hours = options.trash_hours;
    restore_options.put_back = options.trash_put_back;
    restore_options.threads = options.trash_threads;

    TrashRestore runner;
    if (!runner.Init(mds_addr, restore_options)) {
      return true;
    }
    runner.Run();

  } else if (cmd == Helper::ToLowerCase("Info")) {
    HandleInfo(mds_client, fs_id, options);

  } else if (cmd == Helper::ToLowerCase("Summary")) {
    HandleSummary(mds_client, fs_id, options);

  } else if (cmd == Helper::ToLowerCase("SyncDirStat")) {
    HandleSyncDirStat(mds_client, fs_id, options);
  }

  return true;
}

}  // namespace client
}  // namespace mds
}  // namespace dingofs