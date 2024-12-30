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

#include "dingofs/src/mdsv2/client/mds.h"

#include "dingofs/proto/error.pb.h"
#include "dingofs/proto/mdsv2.pb.h"
#include "dingofs/src/mdsv2/common/logging.h"

namespace dingofs {
namespace mdsv2 {
namespace client {

// message S3Info {
//   string ak = 1;
//   string sk = 2;
//   string endpoint = 3;
//   string bucketname = 4;
//   uint64 block_size = 5;
//   uint64 chunk_size = 6;
//   uint32 object_prefix = 7;
// }

void MDSClient::CreateFs(Interaction& interaction) {
  pb::mdsv2::CreateFsRequest request;
  pb::mdsv2::CreateFsResponse response;

  request.set_fs_name("dengzh");
  request.set_block_size(4 * 1024 * 1024);
  request.set_fs_type(pb::mdsv2::FsType::S3);
  request.set_owner("deng");
  request.set_capacity(1024 * 1024 * 1024);
  request.set_recycle_time_hour(24);

  pb::mdsv2::S3Info s3_info;
  s3_info.set_ak("1111111111111111111111111");
  s3_info.set_sk("2222222222222222222222222");
  s3_info.set_endpoint("http://s3.dingodb.com");
  s3_info.set_bucketname("dingo");
  s3_info.set_block_size(4 * 1024 * 1024);
  s3_info.set_chunk_size(4 * 1024 * 1024);
  s3_info.set_object_prefix(0);

  *request.mutable_fs_detail()->mutable_s3_info() = s3_info;

  DINGO_LOG(INFO) << "CreateFs request: " << request.ShortDebugString();

  interaction.SendRequest("MDSService", "CreateFs", request, response);

  DINGO_LOG(INFO) << "CreateFs response: " << response.ShortDebugString();
}

}  // namespace client

}  // namespace mdsv2

}  // namespace dingofs