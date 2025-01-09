// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "mdsv2/service/service_helper.h"

#include "brpc/reloadable_flags.h"
#include "gflags/gflags.h"

namespace dingofs {
namespace mdsv2 {

DEFINE_int64(service_log_threshold_time_ns, 1000000000L, "service log threshold time ns");
BRPC_VALIDATE_GFLAG(service_log_threshold_time_ns, brpc::PositiveInteger);

DEFINE_int32(log_print_max_length, 512, "log print max length");
BRPC_VALIDATE_GFLAG(log_print_max_length, brpc::PositiveInteger);

void ServiceHelper::SetError(pb::error::Error* error, int errcode, const std::string& errmsg) {
  error->set_errcode(static_cast<pb::error::Errno>(errcode));
  error->set_errmsg(errmsg);
}

void ServiceHelper::Inode2PBInode(InodePtr inode, pb::mdsv2::Inode* pb_inode) {
  pb_inode->set_fs_id(inode->GetFsId());
  pb_inode->set_inode_id(inode->GetIno());
  pb_inode->set_length(inode->GetLength());
  pb_inode->set_ctime(inode->GetCtime());
  pb_inode->set_mtime(inode->GetMtime());
  pb_inode->set_atime(inode->GetAtime());
  pb_inode->set_uid(inode->GetUid());
  pb_inode->set_gid(inode->GetGid());
  pb_inode->set_mode(inode->GetMode());
  pb_inode->set_nlink(inode->GetNlink());
  pb_inode->set_type(inode->GetType());
  pb_inode->set_symlink(inode->GetSymlink());
  pb_inode->set_rdev(inode->GetRdev());

  for (const auto& [key, value] : inode->GetS3ChunkMap()) {
    pb_inode->mutable_s3_chunk_map()->insert({key, value});
  }

  pb_inode->set_dtime(inode->GetDtime());
  pb_inode->set_openmpcount(inode->GetOpenmpcount());
  for (const auto& [key, value] : inode->GetXAttrMap()) {
    pb_inode->mutable_xattr()->insert({key, value});
  }
}

}  // namespace mdsv2
}  // namespace dingofs
