/*
 *  Copyright (c) 2021 NetEase Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/*
 * @Project: dingo
 * @Date: 2021-12-15 10:54:37
 * @Author: chenwei
 */
#include "metaserver/partition_cleaner.h"

#include <list>

#include "proto/metaserver.pb.h"
#include "metaserver/copyset/meta_operator.h"

namespace dingofs {
namespace metaserver {

using pb::mds::FsInfo;
using pb::mds::FSStatusCode;
using pb::mds::FSStatusCode_Name;

using pb::metaserver::Inode;
using pb::metaserver::MetaStatusCode;

bool PartitionCleaner::ScanPartition() {
  if (!copysetNode_->IsLeaderTerm()) {
    return false;
  }

  std::list<uint64_t> inode_id_list;
  if (!partition_->GetInodeIdList(&inode_id_list)) {
    return false;
  }

  for (auto inode_id : inode_id_list) {
    if (isStop_ || !copysetNode_->IsLeaderTerm()) {
      return false;
    }
    Inode inode;
    MetaStatusCode ret = partition_->GetInodeWithChunkInfo(
        partition_->GetFsId(), inode_id, &inode);
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "ScanPartition get inode fail, fsId = "
                   << partition_->GetFsId() << ", inodeId = " << inode_id;
      continue;
    }

    ret = CleanDataAndDeleteInode(inode);
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "ScanPartition clean inode fail, inode = "
                   << inode.ShortDebugString();
      continue;
    }
    usleep(inodeDeletePeriodMs_);
  }

  uint32_t partition_id = partition_->GetPartitionId();
  if (partition_->EmptyInodeStorage()) {
    LOG(INFO) << "Inode num is 0, delete partition from metastore"
              << ", partitonId = " << partition_id;
    MetaStatusCode ret = DeletePartition();
    if (ret == MetaStatusCode::OK) {
      VLOG(3) << "DeletePartition success, partitionId = " << partition_id;
      return true;
    } else {
      LOG(WARNING) << "delete partition from copyset fail, partitionId = "
                   << partition_id << ", ret = " << MetaStatusCode_Name(ret);
      return false;
    }
  }

  return false;
}

MetaStatusCode PartitionCleaner::CleanDataAndDeleteInode(const Inode& inode) {
  // TODO(cw123) : consider FsFileType::TYPE_FILE
  if (pb::metaserver::FsFileType::TYPE_S3 == inode.type()) {
    // get s3info from mds
    FsInfo fs_info;
    if (fsInfoMap_.find(inode.fsid()) == fsInfoMap_.end()) {
      auto ret = mdsClient_->GetFsInfo(inode.fsid(), &fs_info);
      if (ret != FSStatusCode::OK) {
        if (FSStatusCode::NOT_FOUND == ret) {
          LOG(ERROR) << "The fsName not exist, fsId = " << inode.fsid();
          return MetaStatusCode::S3_DELETE_ERR;
        } else {
          LOG(ERROR) << "GetFsInfo failed, FSStatusCode = " << ret
                     << ", FSStatusCode_Name = " << FSStatusCode_Name(ret)
                     << ", fsId = " << inode.fsid();
          return MetaStatusCode::S3_DELETE_ERR;
        }
      }
      fsInfoMap_.insert({inode.fsid(), fs_info});
    } else {
      fs_info = fsInfoMap_.find(inode.fsid())->second;
    }
    const auto& s3_info = fs_info.detail().s3info();
    // reinit s3 adaptor
    S3ClientAdaptorOption client_adaptor_option;
    s3Adaptor_->GetS3ClientAdaptorOption(&client_adaptor_option);
    client_adaptor_option.blockSize = s3_info.blocksize();
    client_adaptor_option.chunkSize = s3_info.chunksize();
    client_adaptor_option.objectPrefix = s3_info.objectprefix();
    s3Adaptor_->Reinit(client_adaptor_option, s3_info.ak(), s3_info.sk(),
                       s3_info.endpoint(), s3_info.bucketname());
    int ret_val = s3Adaptor_->Delete(inode);
    if (ret_val != 0) {
      LOG(ERROR) << "S3ClientAdaptor delete s3 data failed"
                 << ", ret = " << ret_val << ", fsId = " << inode.fsid()
                 << ", inodeId = " << inode.inodeid();
      return MetaStatusCode::S3_DELETE_ERR;
    }
  }

  // send request to copyset to delete inode
  MetaStatusCode ret = DeleteInode(inode);
  if (ret != MetaStatusCode::OK && ret != MetaStatusCode::NOT_FOUND) {
    LOG(ERROR) << "Delete Inode fail, fsId = " << inode.fsid()
               << ", inodeId = " << inode.inodeid()
               << ", ret = " << MetaStatusCode_Name(ret);
    return ret;
  }
  return MetaStatusCode::OK;
}

MetaStatusCode PartitionCleaner::DeleteInode(const Inode& inode) {
  pb::metaserver::DeleteInodeRequest request;
  request.set_poolid(partition_->GetPoolId());
  request.set_copysetid(partition_->GetCopySetId());
  request.set_partitionid(partition_->GetPartitionId());
  request.set_fsid(inode.fsid());
  request.set_inodeid(inode.inodeid());
  pb::metaserver::DeleteInodeResponse response;
  PartitionCleanerClosure done;
  auto* delete_inode_op = new copyset::DeleteInodeOperator(
      copysetNode_, nullptr, &request, &response, &done);
  delete_inode_op->Propose();
  done.WaitRunned();
  return response.statuscode();
}

MetaStatusCode PartitionCleaner::DeletePartition() {
  pb::metaserver::DeletePartitionRequest request;
  request.set_poolid(partition_->GetPoolId());
  request.set_copysetid(partition_->GetCopySetId());
  request.set_partitionid(partition_->GetPartitionId());
  pb::metaserver::DeletePartitionResponse response;
  PartitionCleanerClosure done;
  auto* delete_partition_op = new copyset::DeletePartitionOperator(
      copysetNode_, nullptr, &request, &response, &done);
  delete_partition_op->Propose();
  done.WaitRunned();
  return response.statuscode();
}
}  // namespace metaserver
}  // namespace dingofs
