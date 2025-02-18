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

#include "dingofs/error.pb.h"
#include "dingofs/mdsv2.pb.h"
#include "mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

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

  DINGO_LOG(INFO) << "CreateFs response: " << response.ShortDebugString();
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

}  // namespace client
}  // namespace mdsv2
}  // namespace dingofs