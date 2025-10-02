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

#ifndef DINGOFS_COMMON_CONFIG_MAPPER_H_
#define DINGOFS_COMMON_CONFIG_MAPPER_H_

#include "blockaccess/accesser_common.h"
#include "dingofs/mds.pb.h"

namespace dingofs {

static inline void FillBlockAccessOption(
    const pb::mds::FsInfo& fs_info,
    blockaccess::BlockAccessOptions* block_access_opt) {
  if (fs_info.fs_type() == pb::mds::FsType::S3) {
    CHECK(fs_info.extra().has_s3_info()) << "ilegall fs_info, S3 info not set";

    const auto& s3_info = fs_info.extra().s3_info();

    block_access_opt->type = blockaccess::AccesserType::kS3;
    block_access_opt->s3_options.s3_info.ak = s3_info.ak();
    block_access_opt->s3_options.s3_info.sk = s3_info.sk();
    block_access_opt->s3_options.s3_info.endpoint = s3_info.endpoint();
    block_access_opt->s3_options.s3_info.bucket_name = s3_info.bucketname();
  } else if (fs_info.fs_type() == pb::mds::FsType::RADOS) {
    CHECK(fs_info.extra().has_rados_info())
        << "ilegall storage_info, RADOS info not set";

    const auto& rados_info = fs_info.extra().rados_info();

    block_access_opt->type = blockaccess::AccesserType::kRados;
    block_access_opt->rados_options.mon_host = rados_info.mon_host();
    block_access_opt->rados_options.user_name = rados_info.user_name();
    block_access_opt->rados_options.key = rados_info.key();
    block_access_opt->rados_options.pool_name = rados_info.pool_name();
    if (!rados_info.cluster_name().empty()) {
      block_access_opt->rados_options.cluster_name = rados_info.cluster_name();
    }
  } else {
    CHECK(false) << "Unsupported fs type: "
                 << pb::mds::FsType_Name(fs_info.fs_type());
  }
}

}  // namespace dingofs

#endif  // DINGOFS_COMMON_CONFIG_MAPPER_H_