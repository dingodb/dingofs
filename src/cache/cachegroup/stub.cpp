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
 * Created Date: 2025-07-24
 * Author: Jingli Chen (Wine93)
 */

#include "cache/common/macro.h"
#include "cache/common/proto.h"
#include "cache/storage/storage_pool.h"
#include "client/vfs/meta/v2/mds_client.h"
#include "client/vfs/meta/v2/rpc.h"
#include "common/status.h"
#include "dingofs/common.pb.h"

namespace dingofs {
namespace cache {

namespace {

inline PBS3Info ToS3Info(PBS3InfoV2 s3_info_v2) {
  PBS3Info s3_info_v1;
  s3_info_v1.set_ak(s3_info_v2.ak());
  s3_info_v1.set_sk(s3_info_v2.sk());
  s3_info_v1.set_endpoint(s3_info_v2.endpoint());
  s3_info_v1.set_bucketname(s3_info_v2.bucketname());
  return s3_info_v1;
}

inline PBRadosInfo ToRadosInfo(PBRadosInfoV2 rados_info_v2) {
  PBRadosInfo rados_info_v1;
  rados_info_v1.set_user_name(rados_info_v2.user_name());
  rados_info_v1.set_key(rados_info_v2.key());
  rados_info_v1.set_mon_host(rados_info_v2.mon_host());
  rados_info_v1.set_pool_name(rados_info_v2.pool_name());
  rados_info_v1.set_cluster_name(rados_info_v2.cluster_name());
  return rados_info_v1;
}

}  // namespace

GetStorageInfoFunc NewV1GetStorageInfoFunc(
    std::shared_ptr<stub::rpcclient::MdsClient> mds_client) {
  return [mds_client](uint32_t fs_id, PBStorageInfo* storage_info) {
    PBFsInfo fs_info;
    PBFSStatusCode code = mds_client->GetFsInfo(fs_id, &fs_info);
    if (code != PBFSStatusCode::OK) {
      LOG_ERROR("Get filesystem information failed: fs_id = %d, rc = %s", fs_id,
                FSStatusCode_Name(code));
      return Status::Internal("get filesystem information failed");
    } else if (!fs_info.has_storage_info()) {
      LOG_ERROR("The filesystem missing storage_info: fs_id = %d", fs_id);
      return Status::Internal("filesystem missing storage info");
    }

    *storage_info = fs_info.storage_info();
    return Status::OK();
  };
}

GetStorageInfoFunc NewV2GetStorageInfoFunc(const std::string& mds_addr) {
  return [mds_addr](uint32_t fs_id, PBStorageInfo* storage_info) {
    auto rpc = client::vfs::v2::RPC::New(mds_addr);

    PBFsInfoV2 fs_info;
    auto status = client::vfs::v2::MDSClient::GetFsInfo(rpc, fs_id, fs_info);
    if (!status.ok()) {
      LOG_ERROR("Get filesystem information failed: fs_id = %d, status = %s",
                fs_id, status.ToString());
      return status;
    }

    if (fs_info.fs_type() == pb::mdsv2::FsType::S3) {
      storage_info->set_type(pb::common::StorageType::TYPE_S3);
      *storage_info->mutable_s3_info() = ToS3Info(fs_info.extra().s3_info());
    } else if (fs_info.fs_type() == pb::mdsv2::FsType::RADOS) {
      storage_info->set_type(pb::common::StorageType::TYPE_RADOS);
      *storage_info->mutable_rados_info() =
          ToRadosInfo(fs_info.extra().rados_info());
    } else {
      CHECK(false) << "Unsupported fs type: "
                   << pb::mdsv2::FsType_Name(fs_info.fs_type());
    }

    return Status::OK();
  };
}

}  // namespace cache
}  // namespace dingofs
