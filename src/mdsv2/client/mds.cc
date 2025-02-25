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

#include <cstddef>
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

void MDSClient::CreateFs(const std::string& fs_name, const std::string& partition_type) {
  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return;
  }

  pb::mdsv2::CreateFsRequest request;
  pb::mdsv2::CreateFsResponse response;

  request.set_fs_name(fs_name);
  request.set_block_size(4 * 1024 * 1024);
  request.set_fs_type(pb::mdsv2::FsType::S3);
  request.set_owner("deng");
  request.set_capacity(1024 * 1024 * 1024);
  request.set_recycle_time_hour(24);

  if (partition_type == "mono") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::MONOLITHIC_PARTITION);
  } else if (partition_type == "parent_hash") {
    request.set_partition_type(::dingofs::pb::mdsv2::PartitionType::PARENT_ID_HASH_PARTITION);
  }

  pb::mdsv2::S3Info s3_info;
  s3_info.set_ak("1111111111111111111111111");
  s3_info.set_sk("2222222222222222222222222");
  s3_info.set_endpoint("http://s3.dingodb.com");
  s3_info.set_bucketname("dingo");
  s3_info.set_block_size(4 * 1024 * 1024);
  s3_info.set_chunk_size(4 * 1024 * 1024);
  s3_info.set_object_prefix(0);

  *request.mutable_fs_extra()->mutable_s3_info() = s3_info;

  DINGO_LOG(INFO) << "CreateFs request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "CreateFs", request, response);

  if (response.error().errcode() == dingofs::pb::error::Errno::OK) {
    DINGO_LOG(INFO) << "CreateFs success, fs_id: " << response.fs_info().fs_id();
  } else {
    DINGO_LOG(ERROR) << "CreateFs fail, error: " << response.ShortDebugString();
  }
}

void MDSClient::DeleteFs(const std::string& fs_name) {
  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return;
  }

  pb::mdsv2::DeleteFsRequest request;
  pb::mdsv2::DeleteFsResponse response;

  request.set_fs_name(fs_name);

  DINGO_LOG(INFO) << "DeleteFs request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "DeleteFs", request, response);

  DINGO_LOG(INFO) << "DeleteFs response: " << response.ShortDebugString();
}

void MDSClient::GetFs(const std::string& fs_name) {
  if (fs_name.empty()) {
    DINGO_LOG(ERROR) << "fs_name is empty";
    return;
  }

  pb::mdsv2::GetFsInfoRequest request;
  pb::mdsv2::GetFsInfoResponse response;

  request.set_fs_name(fs_name);

  DINGO_LOG(INFO) << "GetFsInfo request: " << request.ShortDebugString();

  interaction_->SendRequest("MDSService", "GetFsInfo", request, response);

  DINGO_LOG(INFO) << "GetFsInfo response: " << response.ShortDebugString();
}

void MDSClient::MkDir(uint32_t fs_id, uint64_t parent, const std::string& name) {
  pb::mdsv2::MkDirRequest request;
  pb::mdsv2::MkDirResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_ino(parent);
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
}

void MDSClient::BatchMkDir(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkDir(fs_id, parent, name);
    }
  }
}

void MDSClient::MkNod(uint32_t fs_id, uint64_t parent_ino, const std::string& name) {
  pb::mdsv2::MkNodRequest request;
  pb::mdsv2::MkNodResponse response;

  request.set_fs_id(fs_id);
  request.set_parent_ino(parent_ino);
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
}

void MDSClient::BatchMkNod(uint32_t fs_id, const std::vector<int64_t>& parents, const std::string& prefix, size_t num) {
  for (size_t i = 0; i < num; i++) {
    for (auto parent : parents) {
      std::string name = fmt::format("{}_{}", prefix, Helper::TimestampNs());
      MkNod(fs_id, parent, name);
    }
  }
}

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs