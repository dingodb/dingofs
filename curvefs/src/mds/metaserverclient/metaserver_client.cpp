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
 * @Project: curve
 * @Date: 2021-06-24 11:21:36
 * @Author: chenwei
 */

#include "curvefs/src/mds/metaserverclient/metaserver_client.h"

#include <bthread/bthread.h>

#include <ctime>

#include "curvefs/src/mds/topology/deal_peerid.h"

namespace curvefs {
namespace mds {

using curvefs::mds::topology::BuildPeerIdWithAddr;
using curvefs::mds::topology::SplitPeerId;
using curvefs::metaserver::CreateDentryRequest;
using curvefs::metaserver::CreateDentryResponse;
using curvefs::metaserver::CreateManageInodeRequest;
using curvefs::metaserver::CreateManageInodeResponse;
using curvefs::metaserver::CreateRootInodeRequest;
using curvefs::metaserver::CreateRootInodeResponse;
using curvefs::metaserver::DeleteDentryRequest;
using curvefs::metaserver::DeleteDentryResponse;
using curvefs::metaserver::DeleteInodeRequest;
using curvefs::metaserver::DeleteInodeResponse;
using curvefs::metaserver::Dentry;
using curvefs::metaserver::MetaServerService_Stub;
using curvefs::metaserver::MetaStatusCode;
using curvefs::metaserver::Time;
using curvefs::metaserver::copyset::COPYSET_OP_STATUS;
using curvefs::metaserver::copyset::CopysetService_Stub;

template <typename T, typename Request, typename Response>
FSStatusCode MetaserverClient::SendRpc2MetaServer(
    Request* request, Response* response, const LeaderCtx& ctx,
    void (T::*func)(google::protobuf::RpcController*, const Request*, Response*,
                    google::protobuf::Closure*)) {
  bool refresh_leader = true;
  uint32_t max_retry = options_.rpcRetryTimes;

  std::string leader;
  auto pool_id = ctx.poolId;
  auto copyset_id = ctx.copysetId;
  brpc::Controller cntl;
  do {
    if (refresh_leader) {
      auto ret = GetLeader(ctx, &leader);
      if (ret != FSStatusCode::OK) {
        LOG(ERROR) << "Get leader fail" << ", poolId = " << pool_id
                   << ", copysetId = " << copyset_id;
        return ret;
      }
      if (channel_.Init(leader.c_str(), nullptr) != 0) {
        LOG(ERROR) << "Init channel to metaserver: " << leader << " failed!";
        return FSStatusCode::RPC_ERROR;
      }
    }

    cntl.Reset();
    cntl.set_timeout_ms(options_.rpcTimeoutMs);
    MetaServerService_Stub stub(&channel_);
    (stub.*func)(&cntl, request, response, nullptr);
    if (cntl.Failed()) {
      LOG(WARNING) << "rpc error: " << cntl.ErrorText();
      refresh_leader =
          (cntl.ErrorCode() == EHOSTDOWN || cntl.ErrorCode() == brpc::ELOGOFF);
    } else {
      switch (response->statuscode()) {
        case MetaStatusCode::OVERLOAD:
          refresh_leader = false;
          break;
        case MetaStatusCode::REDIRECTED:
          refresh_leader = true;
          break;
        default:
          return FSStatusCode::UNKNOWN_ERROR;
      }
    }
    max_retry--;
  } while (max_retry > 0);

  if (cntl.Failed()) {
    LOG(ERROR) << "rpc error: " << cntl.ErrorText();
    return FSStatusCode::RPC_ERROR;
  } else {
    return FSStatusCode::UNKNOWN_ERROR;
  }
}

FSStatusCode MetaserverClient::GetLeader(const LeaderCtx& ctx,
                                         std::string* leader) {
  GetLeaderRequest2 request;
  GetLeaderResponse2 response;
  request.set_poolid(ctx.poolId);
  request.set_copysetid(ctx.copysetId);

  for (const std::string& item : ctx.addrs) {
    LOG(INFO) << "GetLeader from " << item;
    if (channel_.Init(item.c_str(), nullptr) != 0) {
      LOG(ERROR) << "Init channel to metaserver: " << item << " failed!";
      continue;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);
    CliService2_Stub stub(&channel_);
    stub.GetLeader(&cntl, &request, &response, nullptr);

    uint32_t max_retry = options_.rpcRetryTimes;
    while (cntl.Failed() && (max_retry > 0)) {
      int32_t ret_code = cntl.ErrorCode();
      LOG(WARNING) << "GetLeader failed" << ", poolid = " << ctx.poolId
                   << ", copysetId = " << ctx.copysetId
                   << ", errorCode = " << ret_code
                   << ", Rpc error = " << cntl.ErrorText();
      if (ret_code == EHOSTDOWN || ret_code == ECONNRESET ||
          ret_code == ECONNREFUSED || ret_code == brpc::ELOGOFF) {
        break;
      }
      max_retry--;
      bthread_usleep(options_.rpcRetryIntervalUs);
      cntl.Reset();
      cntl.set_timeout_ms(options_.rpcTimeoutMs);
      stub.GetLeader(&cntl, &request, &response, nullptr);
    }

    if (response.has_leader()) {
      std::string ip;
      uint32_t port = 0;
      SplitPeerId(response.leader().address(), &ip, &port);
      *leader = ip + ":" + std::to_string(port);
      return FSStatusCode::OK;
    }
  }
  return FSStatusCode::NOT_FOUND;
}

FSStatusCode MetaserverClient::CreateRootInode(
    uint32_t fs_id, uint32_t pool_id, uint32_t copyset_id,
    uint32_t partition_id, uint32_t uid, uint32_t gid, uint32_t mode,
    const std::set<std::string>& addrs) {
  CreateRootInodeRequest request;
  CreateRootInodeResponse response;
  request.set_poolid(pool_id);
  request.set_copysetid(copyset_id);
  request.set_partitionid(partition_id);
  request.set_fsid(fs_id);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_mode(mode);
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  Time* tm = new Time();
  tm->set_sec(now.tv_sec);
  tm->set_nsec(now.tv_nsec);
  request.set_allocated_create(tm);
  auto fp = &MetaServerService_Stub::CreateRootInode;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = request.poolid();
  ctx.copysetId = request.copysetid();
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "CreateInode failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "CreateInode failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
        return FSStatusCode::OK;
      case MetaStatusCode::INODE_EXIST:
        LOG(WARNING) << "CreateInode failed, Inode exist.";
        return FSStatusCode::INODE_EXIST;
      default:
        LOG(ERROR) << "CreateInode failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::INSERT_ROOT_INODE_ERROR;
    }
  }
}

FSStatusCode MetaserverClient::CreateManageInode(
    uint32_t fs_id, uint32_t pool_id, uint32_t copyset_id,
    uint32_t partition_id, uint32_t uid, uint32_t gid, uint32_t mode,
    ManageInodeType manage_type, const std::set<std::string>& addrs) {
  CreateManageInodeRequest request;
  CreateManageInodeResponse response;
  request.set_poolid(pool_id);
  request.set_copysetid(copyset_id);
  request.set_partitionid(partition_id);
  request.set_fsid(fs_id);
  request.set_uid(uid);
  request.set_gid(gid);
  request.set_mode(mode);
  request.set_managetype(manage_type);

  auto fp = &MetaServerService_Stub::CreateManageInode;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = request.poolid();
  ctx.copysetId = request.copysetid();
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "CreateManageInode failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "CreateManageInode failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
        return FSStatusCode::OK;
      case MetaStatusCode::INODE_EXIST:
        LOG(WARNING) << "CreateManageInode failed, Inode exist.";
        return FSStatusCode::INODE_EXIST;
      default:
        LOG(ERROR) << "CreateManageInode failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::INSERT_MANAGE_INODE_FAIL;
    }
  }
}

FSStatusCode MetaserverClient::CreateDentry(
    uint32_t fs_id, uint32_t pool_id, uint32_t copyset_id,
    uint32_t partition_id, uint64_t parent_inode_id, const std::string& name,
    uint64_t inode_id, const std::set<std::string>& addrs) {
  CreateDentryRequest request;
  CreateDentryResponse response;
  request.set_poolid(pool_id);
  request.set_copysetid(copyset_id);
  request.set_partitionid(partition_id);
  Dentry* d = new Dentry;
  d->set_fsid(fs_id);
  d->set_inodeid(inode_id);
  d->set_parentinodeid(parent_inode_id);
  d->set_name(name);
  d->set_txid(0);
  d->set_type(FsFileType::TYPE_DIRECTORY);
  request.set_allocated_dentry(d);

  auto fp = &MetaServerService_Stub::CreateDentry;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = request.poolid();
  ctx.copysetId = request.copysetid();
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "CreateDentry failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "CreateDentry failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
        return FSStatusCode::OK;
      default:
        LOG(ERROR) << "CreateDentry failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::INSERT_DENTRY_FAIL;
    }
  }
}

FSStatusCode MetaserverClient::DeleteDentry(
    uint32_t pool_id, uint32_t copyset_id, uint32_t partition_id,
    uint32_t fs_id, uint64_t parent_inode_id, const std::string& name,
    const std::set<std::string>& addrs) {
  DeleteDentryRequest request;
  DeleteDentryResponse response;
  request.set_poolid(pool_id);
  request.set_copysetid(copyset_id);
  request.set_partitionid(partition_id);
  request.set_fsid(fs_id);
  request.set_txid(0);
  request.set_parentinodeid(parent_inode_id);
  request.set_name(name);
  request.set_type(FsFileType::TYPE_DIRECTORY);

  auto fp = &MetaServerService_Stub::DeleteDentry;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = request.poolid();
  ctx.copysetId = request.copysetid();
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "DeleteDentry failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "DeleteDentry failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
        return FSStatusCode::OK;
      default:
        LOG(ERROR) << "DeleteDentry failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::DELETE_DENTRY_FAIL;
    }
  }
}

// FIXME: NOT UNDERSTAND
FSStatusCode MetaserverClient::DeleteInode(uint32_t fs_id, uint64_t inode_id) {
  DeleteInodeRequest request;
  DeleteInodeResponse response;

  brpc::Controller cntl;
  cntl.set_timeout_ms(options_.rpcTimeoutMs);

  if (channel_.Init(options_.metaserverAddr.c_str(), nullptr) != 0) {
    LOG(ERROR) << "Init channel to metaserver: " << options_.metaserverAddr
               << " failed!";
    return FSStatusCode::RPC_ERROR;
  }
  MetaServerService_Stub stub(&channel_);
  // TODO(cw123): add partiton
  request.set_poolid(0);
  request.set_copysetid(0);
  request.set_partitionid(0);
  request.set_fsid(fs_id);
  request.set_inodeid(inode_id);
  // TODO(@威姐): 适配新的proto
  request.set_copysetid(1);
  request.set_poolid(1);
  request.set_partitionid(1);

  stub.DeleteInode(&cntl, &request, &response, nullptr);

  if (cntl.Failed()) {
    LOG(ERROR) << "DeleteInode failed" << ", fsId = " << fs_id
               << ", inodeId = " << inode_id
               << ", Rpc error = " << cntl.ErrorText();
    return FSStatusCode::RPC_ERROR;
  }

  if (response.statuscode() != MetaStatusCode::OK) {
    LOG(ERROR) << "DeleteInode failed, fsId = " << fs_id
               << ", inodeId = " << inode_id << ", ret = "
               << FSStatusCode_Name(FSStatusCode::DELETE_INODE_ERROR);
    return FSStatusCode::DELETE_INODE_ERROR;
  }

  return FSStatusCode::OK;
}

FSStatusCode MetaserverClient::CreatePartition(
    uint32_t fs_id, uint32_t pool_id, uint32_t copyset_id,
    uint32_t partition_id, uint64_t id_start, uint64_t id_end,
    const std::set<std::string>& addrs) {
  curvefs::metaserver::CreatePartitionRequest request;
  curvefs::metaserver::CreatePartitionResponse response;
  PartitionInfo* partition = request.mutable_partition();
  partition->set_fsid(fs_id);
  partition->set_poolid(pool_id);
  partition->set_copysetid(copyset_id);
  partition->set_partitionid(partition_id);
  partition->set_start(id_start);
  partition->set_end(id_end);
  partition->set_txid(0);
  partition->set_status(PartitionStatus::READWRITE);
  LOG(INFO) << "CreatePartition request: " << request.ShortDebugString();

  auto fp = &MetaServerService_Stub::CreatePartition;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = request.partition().poolid();
  ctx.copysetId = request.partition().copysetid();
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "CreatePartition failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "CreatePartition failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
        LOG(INFO) << "CreatePartition success, partitionId = " << partition_id;
        return FSStatusCode::OK;
      case MetaStatusCode::PARTITION_EXIST:
        LOG(ERROR) << "CreatePartition failed, partition exist.";
        return FSStatusCode::PARTITION_EXIST;
      default:
        LOG(ERROR) << "CreatePartition failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::CREATE_PARTITION_ERROR;
    }
  }
}

FSStatusCode MetaserverClient::DeletePartition(
    uint32_t pool_id, uint32_t copyset_id, uint32_t partition_id,
    const std::set<std::string>& addrs) {
  curvefs::metaserver::DeletePartitionRequest request;
  curvefs::metaserver::DeletePartitionResponse response;
  request.set_poolid(pool_id);
  request.set_copysetid(copyset_id);
  request.set_partitionid(partition_id);

  auto fp = &MetaServerService_Stub::DeletePartition;
  LeaderCtx ctx;
  ctx.addrs = addrs;
  ctx.poolId = pool_id;
  ctx.copysetId = copyset_id;
  auto ret = SendRpc2MetaServer(&request, &response, ctx, fp);

  if (FSStatusCode::RPC_ERROR == ret) {
    LOG(ERROR) << "DeletePartition failed, rpc error. request = "
               << request.ShortDebugString();
    return ret;
  } else if (FSStatusCode::NOT_FOUND == ret) {
    LOG(ERROR) << "DeletePartition failed, get leader failed. request = "
               << request.ShortDebugString();
    return ret;
  } else {
    switch (response.statuscode()) {
      case MetaStatusCode::OK:
      case MetaStatusCode::PARTITION_NOT_FOUND:
        return FSStatusCode::OK;
      case MetaStatusCode::PARTITION_DELETING:
        LOG(INFO) << "DeletePartition partition deleting, id = "
                  << partition_id;
        return FSStatusCode::UNDER_DELETING;
      default:
        LOG(ERROR) << "DeletePartition failed, request = "
                   << request.ShortDebugString()
                   << ", response statuscode = " << response.statuscode();
        return FSStatusCode::DELETE_PARTITION_ERROR;
    }
  }
}

FSStatusCode MetaserverClient::CreateCopySet(
    uint32_t pool_id, uint32_t copyset_id, const std::set<std::string>& addrs) {
  CreateCopysetRequest request;
  CreateCopysetResponse response;
  auto* copyset = request.add_copysets();
  copyset->set_poolid(pool_id);
  copyset->set_copysetid(copyset_id);
  for (const auto& item : addrs) {
    copyset->add_peers()->set_address(BuildPeerIdWithAddr(item));
  }

  for (const std::string& item : addrs) {
    if (channel_.Init(item.c_str(), nullptr) != 0) {
      LOG(ERROR) << "Init channel to metaserver: " << item << " failed!";
      return FSStatusCode::RPC_ERROR;
    }

    brpc::Controller cntl;
    cntl.set_timeout_ms(options_.rpcTimeoutMs);
    CopysetService_Stub stub(&channel_);
    stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

    uint32_t max_retry = options_.rpcRetryTimes;
    while (cntl.Failed() && max_retry > 0) {
      LOG(WARNING) << "Create copyset failed" << " from " << cntl.remote_side()
                   << " to " << cntl.local_side()
                   << " errCode = " << cntl.ErrorCode()
                   << " errorText = " << cntl.ErrorText()
                   << ", then will retry " << max_retry << " times.";
      max_retry--;
      bthread_usleep(options_.rpcRetryIntervalUs);
      cntl.Reset();
      cntl.set_timeout_ms(options_.rpcTimeoutMs);
      stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
    }
    if (cntl.Failed()) {
      LOG(ERROR) << "Create copyset failed"
                 << ", Rpc error = " << cntl.ErrorText();
      return FSStatusCode::RPC_ERROR;
    }

    if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
      LOG(ERROR) << "Create copyset failed." << " from " << cntl.remote_side()
                 << " to " << cntl.local_side()
                 << " request = " << request.ShortDebugString()
                 << " error code = " << response.status();
      return FSStatusCode::CREATE_COPYSET_ERROR;
    }
  }
  return FSStatusCode::OK;
}

FSStatusCode MetaserverClient::CreateCopySetOnOneMetaserver(
    uint32_t pool_id, uint32_t copyset_id, const std::string& addr) {
  CreateCopysetRequest request;
  CreateCopysetResponse response;

  auto* copyset = request.add_copysets();
  copyset->set_poolid(pool_id);
  copyset->set_copysetid(copyset_id);

  if (channel_.Init(addr.c_str(), nullptr) != 0) {
    LOG(ERROR) << "Init channel to metaserver: " << addr << " failed!";
    return FSStatusCode::RPC_ERROR;
  }

  brpc::Controller cntl;
  cntl.set_timeout_ms(options_.rpcTimeoutMs);
  CopysetService_Stub stub(&channel_);
  stub.CreateCopysetNode(&cntl, &request, &response, nullptr);

  uint32_t max_retry = options_.rpcRetryTimes;
  while (cntl.Failed() && max_retry > 0) {
    max_retry--;
    bthread_usleep(options_.rpcRetryIntervalUs);
    cntl.Reset();
    cntl.set_timeout_ms(options_.rpcTimeoutMs);
    stub.CreateCopysetNode(&cntl, &request, &response, nullptr);
  }
  if (cntl.Failed()) {
    LOG(ERROR) << "Create copyset failed"
               << ", Rpc error = " << cntl.ErrorText();
    return FSStatusCode::RPC_ERROR;
  }

  if (response.status() != COPYSET_OP_STATUS::COPYSET_OP_STATUS_SUCCESS) {
    LOG(ERROR) << "Create copyset failed." << " from " << cntl.remote_side()
               << " to " << cntl.local_side()
               << " request = " << request.ShortDebugString()
               << " error code = " << response.status();
    return FSStatusCode::CREATE_COPYSET_ERROR;
  }

  return FSStatusCode::OK;
}
}  // namespace mds
}  // namespace curvefs
