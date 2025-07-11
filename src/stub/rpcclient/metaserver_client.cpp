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
 * Project: dingo
 * Created Date: Mon Sept 1 2021
 * Author: lixiaocui
 */

#include "stub/rpcclient/metaserver_client.h"

#include <brpc/closure_guard.h>
#include <butil/iobuf.h>
#include <glog/logging.h>
#include <time.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <utility>

#include "absl/cleanup/cleanup.h"
#include "butil/time.h"
#include "common/define.h"
#include "common/rpc_stream.h"
#include "dingofs/metaserver.pb.h"
#include "fmt/core.h"
#include "metrics/metaserver/metaserver_client.h"
#include "metrics/metric_guard.h"
#include "stub/common/common.h"
#include "stub/rpcclient/meta_access_log.h"
#include "stub/rpcclient/metacache.h"
#include "stub/rpcclient/task_excutor.h"
#include "utils/string_util.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

using pb::metaserver::BatchGetInodeAttrRequest;
using pb::metaserver::BatchGetInodeAttrResponse;
using pb::metaserver::BatchGetXAttrRequest;
using pb::metaserver::BatchGetXAttrResponse;
using pb::metaserver::Dentry;
using pb::metaserver::FlushDirUsagesRequest;
using pb::metaserver::FlushDirUsagesResponse;
using pb::metaserver::FlushFsUsageRequest;
using pb::metaserver::FlushFsUsageResponse;
using pb::metaserver::FsFileType;
using pb::metaserver::GetFsQuotaRequest;
using pb::metaserver::GetFsQuotaResponse;
using pb::metaserver::GetOrModifyS3ChunkInfoRequest;
using pb::metaserver::GetOrModifyS3ChunkInfoResponse;
using pb::metaserver::Inode;
using pb::metaserver::InodeAttr;
using pb::metaserver::LoadDirQuotasRequest;
using pb::metaserver::LoadDirQuotasResponse;
using pb::metaserver::MetaServerService_Stub;
using pb::metaserver::MetaStatusCode;
using pb::metaserver::MetaStatusCode_Name;
using pb::metaserver::Quota;
using pb::metaserver::S3ChunkInfoList;
using pb::metaserver::Time;
using pb::metaserver::UpdateInodeRequest;
using pb::metaserver::UpdateInodeResponse;
using pb::metaserver::Usage;
using pb::metaserver::XAttr;

using common::CopysetID;
using common::ExcutorOpt;
using common::LogicPoolID;
using common::MetaserverID;
using common::MetaServerOpType;
using common::PartitionID;
using metrics::MetricListGuard;
using metrics::metaserver::MetaServerClientMetric;
using rpcclient::ConvertToMetaStatusCode;
using utils::StringToUll;

using dingofs::common::StreamConnection;
using dingofs::common::StreamOptions;
using dingofs::common::StreamStatus;

using CreateDentryExcutor = TaskExecutor;
using GetDentryExcutor = TaskExecutor;
using ListDentryExcutor = TaskExecutor;
using DeleteDentryExcutor = TaskExecutor;
using PrepareRenameTxExcutor = TaskExecutor;
using DeleteInodeExcutor = TaskExecutor;
using UpdateInodeExcutor = TaskExecutor;
using GetInodeExcutor = TaskExecutor;
using BatchGetInodeAttrExcutor = TaskExecutor;
using BatchGetXAttrExcutor = TaskExecutor;
using GetOrModifyS3ChunkInfoExcutor = TaskExecutor;
using UpdateVolumeExtentExecutor = TaskExecutor;
using GetVolumeExtentExecutor = TaskExecutor;

MetaStatusCode MetaServerClientImpl::Init(
    const ExcutorOpt& excutor_opt, const ExcutorOpt& excutor_internal_opt,
    std::shared_ptr<MetaCache> meta_cache,
    std::shared_ptr<ChannelManager<MetaserverID>> channel_manager) {
  opt_ = excutor_opt;
  optInternal_ = excutor_internal_opt;
  metaCache_ = meta_cache;
  channelManager_ = channel_manager;
  return MetaStatusCode::OK;
}

#define RPCTask                                                         \
  [&](LogicPoolID poolID, CopysetID copysetID, PartitionID partitionID, \
      uint64_t txId, uint64_t applyIndex, brpc::Channel* channel,       \
      brpc::Controller* cntl, TaskExecutorDone* taskExecutorDone) -> int

#define AsyncRPCTask                                                    \
  [=](LogicPoolID poolID, CopysetID copysetID, PartitionID partitionID, \
      uint64_t txId, uint64_t applyIndex, brpc::Channel* channel,       \
      brpc::Controller* cntl, TaskExecutorDone* taskExecutorDone) -> int

class MetaServerClientRpcDoneBase : public google::protobuf::Closure {
 public:
  MetaServerClientRpcDoneBase(TaskExecutorDone* done,
                              MetaServerClientMetric* metric)
      : done_(done), metric_(metric), start_time_us(butil::cpuwide_time_us()) {}

  ~MetaServerClientRpcDoneBase() override = default;

 protected:
  TaskExecutorDone* done_;
  MetaServerClientMetric* metric_;
  int64_t start_time_us;
};

MetaStatusCode MetaServerClientImpl::GetTxId(uint32_t fs_id, uint64_t inode_id,
                                             uint32_t* partition_id,
                                             uint64_t* tx_id) {
  if (!metaCache_->GetTxId(fs_id, inode_id, partition_id, tx_id)) {
    return MetaStatusCode::NOT_FOUND;
  }
  return MetaStatusCode::OK;
}

void MetaServerClientImpl::SetTxId(uint32_t partition_id, uint64_t tx_id) {
  metaCache_->SetTxId(partition_id, tx_id);
}

MetaStatusCode MetaServerClientImpl::GetDentry(uint32_t fs_id, uint64_t inodeid,
                                               const std::string& name,
                                               Dentry* out) {
  auto task = RPCTask {
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("get_dentry (%d,%d,%d,%d) (%d,%s) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid, name,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(
        &is_ok, {&metric_.getDentry, &metric_.getAllOperation}, start);

    pb::metaserver::GetDentryResponse response;
    pb::metaserver::GetDentryRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_parentinodeid(inodeid);
    request.set_name(name);
    request.set_txid(txId);
    request.set_appliedindex(applyIndex);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.GetDentry(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "Fail GetDentry ino: " << inodeid << ", fs_id: " << fs_id
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }
    MetaStatusCode ret = response.statuscode();

    if (ret != MetaStatusCode::OK) {
      LOG_IF(WARNING, ret != MetaStatusCode::NOT_FOUND)
          << "GetDentry: fsId = " << fs_id << ", inodeId=" << inodeid
          << ", name = " << name << ", errcode = " << ret
          << ", errmsg = " << MetaStatusCode_Name(ret);

    } else if (response.has_dentry() && response.has_appliedindex()) {
      *out = response.dentry();

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "GetDentry: fsId = " << fs_id << ", inodeId=" << inodeid
                   << ", name = " << name
                   << " ok, but dentry or applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "GetDentry done, request: " << request.ShortDebugString()
             << ", response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx =
      std::make_shared<TaskContext>(MetaServerOpType::GetDentry, task, fs_id,
                                    inodeid, false, opt_.enableRenameParallel);
  GetDentryExcutor excutor(opt_, metaCache_, channelManager_,
                           std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::ListDentry(
    uint32_t fs_id, uint64_t inodeid, const std::string& last, uint32_t count,
    bool only_dir, std::list<Dentry>* dentry_list) {
  auto task = RPCTask {
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("list_dentry (%d,%d,%d,%d) (%d,%d,%s) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid, count,
                             last, is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(
        &is_ok, {&metric_.listDentry, &metric_.getAllOperation}, start);

    pb::metaserver::ListDentryRequest request;
    pb::metaserver::ListDentryResponse response;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_dirinodeid(inodeid);
    request.set_txid(txId);
    request.set_last(last);
    request.set_count(count);
    request.set_onlydir(only_dir);
    request.set_appliedindex(applyIndex);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.ListDentry(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "ListDentry Failed, ino: " << inodeid
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "ListDentry: fsId = " << fs_id << ", inodeId=" << inodeid
                   << ", last = " << last << ", count = " << count
                   << ", onlyDir = " << only_dir << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());

      auto dentrys = response.dentrys();
      std::for_each(dentrys.begin(), dentrys.end(),
                    [&](Dentry& d) { dentry_list->push_back(d); });
    } else {
      LOG(WARNING) << "ListDentry: fsId = " << fs_id << ", inodeId=" << inodeid
                   << ", last = " << last << ", count = " << count
                   << ", onlyDir = " << only_dir
                   << " ok, but applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "ListDentry done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx =
      std::make_shared<TaskContext>(MetaServerOpType::ListDentry, task, fs_id,
                                    inodeid, false, opt_.enableRenameParallel);
  ListDentryExcutor excutor(opt_, metaCache_, channelManager_,
                            std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::CreateDentry(const Dentry& dentry) {
  auto task = RPCTask {
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    auto fs_id = dentry.fsid();

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("create_dentry (%d,%d,%d,%d) (%d,%d,%s) %s", fs_id,
                             poolID, copysetID, partitionID,
                             dentry.parentinodeid(), dentry.inodeid(),
                             dentry.name(), is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.createDentry, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    pb::metaserver::CreateDentryResponse response;
    pb::metaserver::CreateDentryRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    Dentry* d = new Dentry;
    d->set_fsid(fs_id);
    d->set_inodeid(dentry.inodeid());
    d->set_parentinodeid(dentry.parentinodeid());
    d->set_name(dentry.name());
    d->set_txid(txId);
    d->set_type(dentry.type());
    request.set_allocated_dentry(d);
    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.CreateDentry(cntl, &request, &response, nullptr);

    std::ostringstream oss;
    channel->Describe(oss, {});

    VLOG(6) << "CreateDentry " << request.ShortDebugString() << " to "
            << oss.str();

    if (cntl->Failed()) {
      LOG(WARNING) << "CreateDentry Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "CreateDentry:  dentry = " << dentry.ShortDebugString()
                   << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "CreateDentry:  dentry = " << dentry.ShortDebugString()
                   << " ok, but applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "CreateDentry "
             << (ret == MetaStatusCode::OK ? "success" : "failure")
             << ", request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::CreateDentry, task, dentry.fsid(),
      // TODO(@lixiaocui): may be taskContext need diffrent according to
      // different operatrion
      dentry.parentinodeid(), false, opt_.enableRenameParallel);
  CreateDentryExcutor excutor(opt_, metaCache_, channelManager_,
                              std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteDentry(uint32_t fs_id,
                                                  uint64_t inodeid,
                                                  const std::string& name,
                                                  FsFileType type) {
  auto task = RPCTask {
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("delete_dentry (%d,%d,%d,%d) (%d,%s) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid, name,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.deleteDentry, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    pb::metaserver::DeleteDentryResponse response;
    pb::metaserver::DeleteDentryRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_parentinodeid(inodeid);
    request.set_name(name);
    request.set_txid(txId);
    request.set_type(type);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.DeleteDentry(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "DeleteDentry Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "DeleteDentry:  fsid = " << fs_id
                   << ", inodeId=" << inodeid << ", name = " << name
                   << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "DeleteDentry:  fsid = " << fs_id
                   << ", inodeId=" << inodeid << ", name = " << name
                   << " ok, but applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "DeleteDentry done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx =
      std::make_shared<TaskContext>(MetaServerOpType::DeleteDentry, task, fs_id,
                                    inodeid, false, opt_.enableRenameParallel);
  DeleteDentryExcutor excutor(opt_, metaCache_, channelManager_,
                              std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::PrepareRenameTx(
    const std::vector<Dentry>& dentrys) {
  auto fs_id = dentrys[0].fsid();
  auto inodeid = dentrys[0].parentinodeid();

  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("prepare_rename_tx (%d,%d,%d,%d) (%d) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(
        &is_ok,
        {&metric_.prepareRenameTx, &metric_.getTxnOperation,
         &metric_.getAllOperation},
        start);

    pb::metaserver::PrepareRenameTxRequest request;
    pb::metaserver::PrepareRenameTxResponse response;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    *request.mutable_dentrys() = {dentrys.begin(), dentrys.end()};

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.PrepareRenameTx(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "PrepareRenameTx failed"
                   << ", errorCode = " << cntl->ErrorCode()
                   << ", errorText = " << cntl->ErrorText()
                   << ", logId = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    auto rc = response.statuscode();
    if (rc != MetaStatusCode::OK) {
      LOG(WARNING) << "PrepareRenameTx: retCode = " << rc
                   << ", message = " << MetaStatusCode_Name(rc)
                   << ", request: " << request.ShortDebugString();
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "PrepareRenameTx OK"
                   << ", but applyIndex not set in response:"
                   << response.ShortDebugString()
                   << ", request: " << request.ShortDebugString();
      return -1;
    }

    VLOG(12) << "PrepareRenameTx done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return rc;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::PrepareRenameTx, task, fs_id, inodeid);
  PrepareRenameTxExcutor excutor(opt_, metaCache_, channelManager_,
                                 std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::GetInode(uint32_t fs_id, uint64_t inodeid,
                                              Inode* out, bool* streaming) {
  auto task = RPCTask {
    (void)txId;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("get_inode (%d,%d,%d,%d) (%d) %s", fs_id, poolID,
                             copysetID, partitionID, inodeid,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(
        &is_ok, {&metric_.getInode, &metric_.getAllOperation}, start);

    pb::metaserver::GetInodeRequest request;
    pb::metaserver::GetInodeResponse response;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_inodeid(inodeid);
    request.set_appliedindex(applyIndex);
    request.set_supportstreaming(true);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.GetInode(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "GetInode Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG_IF(WARNING, ret != MetaStatusCode::NOT_FOUND)
          << "inodeId=" << inodeid << ", errcode = " << ret
          << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_inode() && response.has_appliedindex()) {
      out->CopyFrom(response.inode());

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "inodeId=" << inodeid
                   << " ok, but applyIndex or inode not set in response: "
                   << response.ShortDebugString();
      return -1;
    }

    *streaming = response.has_streaming() ? response.streaming() : false;
    const auto& s3chunkinfo_map = response.inode().s3chunkinfomap();
    for (const auto& item : s3chunkinfo_map) {
      VLOG(12) << "inodeInfo, inodeId=" << inodeid
               << ", s3chunkinfo item key:" << item.first
               << ", value:" << item.second.ShortDebugString();
    }
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(MetaServerOpType::GetInode,
                                                task, fs_id, inodeid);
  GetInodeExcutor excutor(opt_, metaCache_, channelManager_,
                          std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

bool GroupInodeIdByPartition(
    uint32_t fs_id, std::shared_ptr<MetaCache> meta_cache,
    const std::set<uint64_t>& inode_ids,
    std::unordered_map<uint32_t, std::vector<uint64_t>>* inode_groups) {
  for (const auto& it : inode_ids) {
    uint32_t p_id = 0;
    if (meta_cache->GetPartitionIdByInodeId(fs_id, it, &p_id)) {
      auto iter = inode_groups->find(p_id);
      if (iter == inode_groups->end()) {
        inode_groups->emplace(p_id, std::vector<uint64_t>({it}));
      } else {
        iter->second.push_back(it);
      }
    } else {
      LOG(ERROR) << "Group inodeId fialed when get partitionId by"
                 << "inodeId, fsId = " << fs_id << ", inodeId=" << it;
      return false;
    }
  }
  return true;
}

bool MetaServerClientImpl::SplitRequestInodes(
    uint32_t fs_id, const std::set<uint64_t>& inode_ids,
    std::vector<std::vector<uint64_t>>* inode_groups) {
  std::unordered_map<uint32_t, std::vector<uint64_t>> groups;
  bool ret = GroupInodeIdByPartition(fs_id, metaCache_, inode_ids, &groups);
  if (!ret) {
    return false;
  }
  for (const auto& it : groups) {
    auto iter = it.second.begin();
    while (iter != it.second.end()) {
      std::vector<uint64_t> tmp;
      uint32_t batch_limit = opt_.batchInodeAttrLimit;
      while (iter != it.second.end() && batch_limit > 0) {
        tmp.emplace_back(*iter);
        iter++;
        batch_limit--;
      }
      inode_groups->emplace_back(std::move(tmp));
    }
  }
  return true;
}

class BatchGetInodeAttrRpcDone : public MetaServerClientRpcDoneBase {
 public:
  using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

  void Run() override;

  BatchGetInodeAttrRequest request;
  BatchGetInodeAttrResponse response;
};

void BatchGetInodeAttrRpcDone::Run() {
  // update metaserver operation metrics stats
  bool is_ok = true;

  MetaAccessLogGuard log(start_time_us, [&]() {
    return absl::StrFormat("async_batch_get_inode_attr (%d,%d,%d,%d) %d %s",
                           request.fsid(), request.poolid(),
                           request.copysetid(), request.partitionid(),
                           request.inodeid_size(), is_ok ? "ok" : "failed");
  });

  MetricListGuard meta_guard(
      &is_ok, {&(metric_->batchGetInodeAttr), &(metric_->getAllOperation)},
      start_time_us);

  std::unique_ptr<BatchGetInodeAttrRpcDone> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  auto task_ctx = done_->GetTaskExcutor()->GetTaskCxt();
  auto& cntl = task_ctx->cntl_;
  if (cntl.Failed()) {
    LOG(WARNING) << "batchGetInodeAttr Failed, errorcode = " << cntl.ErrorCode()
                 << ", error content: " << cntl.ErrorText()
                 << ", log id: " << cntl.log_id()
                 << ", request: " << request.ShortDebugString();
    is_ok = false;
    done_->SetRetCode(-cntl.ErrorCode());
    return;
  }

  MetaStatusCode ret = response.statuscode();
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "batchGetInodeAttr failed" << ", errcode = " << ret
                 << ", errmsg = " << MetaStatusCode_Name(ret);
  } else if (response.has_appliedindex()) {
    auto meta_cache = done_->GetTaskExcutor()->GetMetaCache();
    meta_cache->UpdateApplyIndex(task_ctx->target.groupID,
                                 response.appliedindex());
  } else {
    LOG(WARNING) << "batchGetInodeAttr ok,"
                 << " but applyIndex not set in response:"
                 << response.ShortDebugString();
    done_->SetRetCode(-1);
    return;
  }

  VLOG(12) << "batchGetInodeAttr done, "
           << "response: " << response.ShortDebugString();
  done_->SetRetCode(ret);
  dynamic_cast<BatchGetInodeAttrTaskExecutorDone*>(done_)->SetInodeAttrs(
      response.attr());
}

MetaStatusCode MetaServerClientImpl::BatchGetInodeAttr(
    uint32_t fs_id, const std::set<uint64_t>& inode_ids,
    std::list<InodeAttr>* attr) {
  // group inodeid by partition and batchlimit
  std::vector<std::vector<uint64_t>> inode_groups;
  if (!SplitRequestInodes(fs_id, inode_ids, &inode_groups)) {
    return MetaStatusCode::NOT_FOUND;
  }

  // TDOD(wanghai): send rpc parallelly
  for (const auto& it : inode_groups) {
    if (it.empty()) {
      LOG(WARNING) << "BatchGetInodeAttr request empty.";
      return MetaStatusCode::PARAM_ERROR;
    }
    uint64_t inode_id = *it.begin();

    auto task = RPCTask {
      (void)txId;
      (void)taskExecutorDone;

      // update metaserver operation metrics stats
      auto start = butil::cpuwide_time_us();
      bool is_ok = true;

      MetaAccessLogGuard log(start, [&]() {
        return absl::StrFormat("batch_get_inode_attr (%d,%d,%d,%d) %d %s",
                               fs_id, poolID, copysetID, partitionID,
                               inode_ids.size(), is_ok ? "ok" : "failed");
      });

      MetricListGuard meta_guard(
          &is_ok, {&metric_.batchGetInodeAttr, &metric_.getAllOperation},
          start);

      BatchGetInodeAttrRequest request;
      BatchGetInodeAttrResponse response;
      request.set_poolid(poolID);
      request.set_copysetid(copysetID);
      request.set_partitionid(partitionID);
      request.set_fsid(fs_id);
      request.set_appliedindex(applyIndex);
      *request.mutable_inodeid() = {it.begin(), it.end()};

      dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
      stub.BatchGetInodeAttr(cntl, &request, &response, nullptr);

      if (cntl->Failed()) {
        LOG(WARNING) << "BatchGetInodeAttr Failed, errorcode = "
                     << cntl->ErrorCode()
                     << ", error content:" << cntl->ErrorText()
                     << ", log id = " << cntl->log_id()
                     << ", request: " << request.ShortDebugString();
        is_ok = false;
        return -cntl->ErrorCode();
      }

      MetaStatusCode ret = response.statuscode();
      if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "BatchGetInodeAttr failed, errcode = " << ret
                     << ", errmsg = " << MetaStatusCode_Name(ret);
      } else if (response.attr_size() > 0 && response.has_appliedindex()) {
        auto* attrs = response.mutable_attr();
        attr->insert(attr->end(), std::make_move_iterator(attrs->begin()),
                     std::make_move_iterator(attrs->end()));
        metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                     response.appliedindex());
      } else {
        LOG(WARNING) << "BatchGetInodeAttr ok, but"
                     << " applyIndex or attr not set in response: "
                     << response.ShortDebugString();
        return -1;
      }
      return ret;
    };
    auto task_ctx = std::make_shared<TaskContext>(
        MetaServerOpType::BatchGetInodeAttr, task, fs_id, inode_id);
    BatchGetInodeAttrExcutor excutor(opt_, metaCache_, channelManager_,
                                     std::move(task_ctx));
    auto ret = ConvertToMetaStatusCode(excutor.DoRPCTask());
    if (ret != MetaStatusCode::OK) {
      attr->clear();
      return ret;
    }
  }
  return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::BatchGetInodeAttrAsync(
    uint32_t fs_id, const std::vector<uint64_t>& inode_ids,
    MetaServerClientDone* done) {
  if (inode_ids.empty()) {
    done->Run();
    return MetaStatusCode::OK;
  }

  auto task = AsyncRPCTask {
    (void)txId;

    auto* rpc_done = new BatchGetInodeAttrRpcDone(taskExecutorDone, &metric_);
    BatchGetInodeAttrRequest& request = rpc_done->request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_appliedindex(applyIndex);
    *request.mutable_inodeid() = {inode_ids.begin(), inode_ids.end()};

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.BatchGetInodeAttr(cntl, &request, &rpc_done->response, rpc_done);
    return MetaStatusCode::OK;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::BatchGetInodeAttr, task, fs_id, *inode_ids.begin());
  auto excutor = std::make_shared<BatchGetInodeAttrExcutor>(
      opt_, metaCache_, channelManager_, std::move(task_ctx));
  TaskExecutorDone* task_done =
      new BatchGetInodeAttrTaskExecutorDone(excutor, done);
  excutor->DoAsyncRPCTask(task_done);
  return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::BatchGetXAttr(
    uint32_t fs_id, const std::set<uint64_t>& inode_ids,
    std::list<XAttr>* xattr) {
  // group inodeid by partition and batchlimit
  std::vector<std::vector<uint64_t>> inode_groups;
  if (!SplitRequestInodes(fs_id, inode_ids, &inode_groups)) {
    return MetaStatusCode::NOT_FOUND;
  }

  // TDOD(wanghai): send rpc parallelly
  for (const auto& it : inode_groups) {
    if (it.empty()) {
      LOG(WARNING) << "BatchGetInodeXAttr request empty.";
      return MetaStatusCode::PARAM_ERROR;
    }

    uint64_t inode_id = *it.begin();
    auto task = RPCTask {
      (void)txId;
      (void)taskExecutorDone;

      // update metaserver operation metrics stats
      auto start = butil::cpuwide_time_us();
      bool is_ok = true;

      MetaAccessLogGuard log(start, [&]() {
        return absl::StrFormat("batch_get_xattr (%d,%d,%d,%d) %d %s", fs_id,
                               poolID, copysetID, partitionID, inode_ids.size(),
                               is_ok ? "ok" : "failed");
      });

      MetricListGuard metric_guard(
          &is_ok, {&metric_.batchGetXattr, &metric_.getAllOperation}, start);

      BatchGetXAttrRequest request;
      BatchGetXAttrResponse response;
      request.set_poolid(poolID);
      request.set_copysetid(copysetID);
      request.set_partitionid(partitionID);
      request.set_fsid(fs_id);
      request.set_appliedindex(applyIndex);
      *request.mutable_inodeid() = {it.begin(), it.end()};

      dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
      stub.BatchGetXAttr(cntl, &request, &response, nullptr);

      if (cntl->Failed()) {
        LOG(WARNING) << "BatchGetXAttr Failed, errorcode = "
                     << cntl->ErrorCode()
                     << ", error content:" << cntl->ErrorText()
                     << ", log id = " << cntl->log_id();
        is_ok = false;
        return -cntl->ErrorCode();
      }

      MetaStatusCode ret = response.statuscode();
      if (ret != MetaStatusCode::OK) {
        LOG(WARNING) << "BatchGetXAttr failed, errcode = " << ret
                     << ", errmsg = " << MetaStatusCode_Name(ret);
      } else if (response.xattr_size() > 0 && response.has_appliedindex()) {
        auto* xattrs = response.mutable_xattr();
        xattr->insert(xattr->end(), std::make_move_iterator(xattrs->begin()),
                      std::make_move_iterator(xattrs->end()));
        metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                     response.appliedindex());
      } else {
        LOG(WARNING) << "BatchGetXAttr ok, but"
                     << " applyIndex or attr not set in response: "
                     << response.ShortDebugString();
        return -1;
      }
      return ret;
    };
    auto task_ctx = std::make_shared<TaskContext>(
        MetaServerOpType::BatchGetInodeAttr, task, fs_id, inode_id);
    BatchGetInodeAttrExcutor excutor(opt_, metaCache_, channelManager_,
                                     std::move(task_ctx));
    auto ret = ConvertToMetaStatusCode(excutor.DoRPCTask());
    if (ret != MetaStatusCode::OK) {
      xattr->clear();
      return ret;
    }
  }
  return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::UpdateInode(
    const UpdateInodeRequest& request, bool internal) {
  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    auto fs_id = request.fsid();
    auto inodeid = request.inodeid();

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("update_inode (%d,%d,%d,%d) (%d) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.updateInode, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    UpdateInodeRequest req = request;
    req.set_poolid(poolID);
    req.set_copysetid(copysetID);
    req.set_partitionid(partitionID);

    UpdateInodeResponse response;
    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.UpdateInode(cntl, &req, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "inodeId=" << request.inodeid()
                   << " UpdateInode Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "inodeId=" << request.inodeid()
                   << " UpdateInode:  request: " << request.ShortDebugString()
                   << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "inodeId=" << request.inodeid()
                   << " UpdateInode:  request: " << request.ShortDebugString()
                   << "ok, but applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "UpdateInode done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::UpdateInode, task, request.fsid(), request.inodeid());
  ExcutorOpt opt;
  if (internal) {
    opt = optInternal_;
  } else {
    opt = opt_;
  }
  UpdateInodeExcutor excutor(opt, metaCache_, channelManager_,
                             std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

namespace {

#define SET_REQUEST_FIELD_IF_HAS(request, attr, field) \
  do {                                                 \
    if ((attr).has_##field()) {                        \
      (request)->set_##field((attr).field());          \
    }                                                  \
  } while (false)

void FillInodeAttr(uint32_t fs_id, uint64_t inode_id, const InodeAttr& attr,
                   bool nlink, UpdateInodeRequest* request) {
  request->set_fsid(fs_id);
  request->set_inodeid(inode_id);

  SET_REQUEST_FIELD_IF_HAS(request, attr, length);
  SET_REQUEST_FIELD_IF_HAS(request, attr, atime);
  SET_REQUEST_FIELD_IF_HAS(request, attr, atime_ns);
  SET_REQUEST_FIELD_IF_HAS(request, attr, ctime);
  SET_REQUEST_FIELD_IF_HAS(request, attr, ctime_ns);
  SET_REQUEST_FIELD_IF_HAS(request, attr, mtime);
  SET_REQUEST_FIELD_IF_HAS(request, attr, mtime_ns);
  SET_REQUEST_FIELD_IF_HAS(request, attr, uid);
  SET_REQUEST_FIELD_IF_HAS(request, attr, gid);
  SET_REQUEST_FIELD_IF_HAS(request, attr, mode);

  *request->mutable_parent() = attr.parent();
  if (attr.xattr_size() > 0) {
    *request->mutable_xattr() = attr.xattr();
  }

  if (nlink) {
    request->set_nlink(attr.nlink());
  }
}

#undef SET_REQUEST_FIELD_IF_HAS

void FillDataIndices(DataIndices&& indices, UpdateInodeRequest* request) {
  if (indices.s3ChunkInfoMap && !indices.s3ChunkInfoMap->empty()) {
    *request->mutable_s3chunkinfoadd() =
        std::move(indices.s3ChunkInfoMap.value());
  }

  if (indices.volumeExtents && indices.volumeExtents->slices_size() > 0) {
    *request->mutable_volumeextents() =
        std::move(indices.volumeExtents.value());
  }
}

}  // namespace

MetaStatusCode MetaServerClientImpl::UpdateInodeAttr(uint32_t fs_id,
                                                     uint64_t inode_id,
                                                     const InodeAttr& attr) {
  UpdateInodeRequest request;
  FillInodeAttr(fs_id, inode_id, attr, /*nlink=*/true, &request);
  return UpdateInode(request);
}

MetaStatusCode MetaServerClientImpl::UpdateInodeAttrWithOutNlink(
    uint32_t fs_id, uint64_t inode_id, const InodeAttr& attr,
    S3ChunkInfoMap* s3_chunk_info_add, bool internal) {
  UpdateInodeRequest request;
  FillInodeAttr(fs_id, inode_id, attr, /*nlink=*/false, &request);
  if (s3_chunk_info_add != nullptr) {
    DataIndices indices;
    indices.s3ChunkInfoMap = *s3_chunk_info_add;
    FillDataIndices(std::move(indices), &request);
  }
  return UpdateInode(request, internal);
}

class UpdateInodeRpcDone : public MetaServerClientRpcDoneBase {
 public:
  using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

  void Run() override;

  UpdateInodeRequest request;
  UpdateInodeResponse response;
};

void UpdateInodeRpcDone::Run() {
  // update metaserver operation metrics stats
  bool is_ok = true;

  MetaAccessLogGuard log(start_time_us, [&]() {
    return absl::StrFormat("async_update_inode (%d,%d,%d,%d) (%d) %s",
                           request.fsid(), request.poolid(),
                           request.copysetid(), request.partitionid(),
                           request.inodeid(), is_ok ? "ok" : "failed");
  });

  MetricListGuard meta_guard(
      &is_ok,
      {&(metric_->updateInode), &(metric_->getTxnOperation),
       &(metric_->getAllOperation)},
      start_time_us);

  std::unique_ptr<UpdateInodeRpcDone> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  auto task_ctx = done_->GetTaskExcutor()->GetTaskCxt();
  auto& cntl = task_ctx->cntl_;
  auto meta_cache = done_->GetTaskExcutor()->GetMetaCache();
  if (cntl.Failed()) {
    LOG(WARNING) << "inodeId=" << task_ctx->inodeID
                 << " UpdateInode Failed, errorcode = " << cntl.ErrorCode()
                 << ", error content: " << cntl.ErrorText()
                 << ", log id: " << cntl.log_id();
    is_ok = false;
    done_->SetRetCode(-cntl.ErrorCode());
    return;
  }

  MetaStatusCode ret = response.statuscode();
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "inodeId=" << task_ctx->inodeID
                 << " UpdateInode fail errcode = " << ret
                 << ", errmsg = " << MetaStatusCode_Name(ret);
  } else if (response.has_appliedindex()) {
    meta_cache->UpdateApplyIndex(task_ctx->target.groupID,
                                 response.appliedindex());
  } else {
    LOG(WARNING) << "inodeId=" << task_ctx->inodeID
                 << "UpdateInode ok, but applyIndex not set in response:"
                 << response.ShortDebugString();
    done_->SetRetCode(-1);
    return;
  }

  VLOG(12) << "inodeId=" << task_ctx->inodeID << " UpdateInode done, "
           << "response: " << response.ShortDebugString();
  done_->SetRetCode(ret);
}

void MetaServerClientImpl::UpdateInodeAsync(const UpdateInodeRequest& request,
                                            MetaServerClientDone* done) {
  auto task = AsyncRPCTask {
    (void)txId;
    (void)applyIndex;

    auto* rpc_done = new UpdateInodeRpcDone(taskExecutorDone, &metric_);

    UpdateInodeRequest& req = rpc_done->request;
    req = request;
    req.set_poolid(poolID);
    req.set_copysetid(copysetID);
    req.set_partitionid(partitionID);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.UpdateInode(cntl, &req, &rpc_done->response, rpc_done);
    return MetaStatusCode::OK;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::UpdateInode, task, request.fsid(), request.inodeid());
  auto excutor = std::make_shared<UpdateInodeExcutor>(
      opt_, metaCache_, channelManager_, std::move(task_ctx));
  TaskExecutorDone* task_done = new TaskExecutorDone(excutor, done);
  excutor->DoAsyncRPCTask(task_done);
}

void MetaServerClientImpl::UpdateInodeWithOutNlinkAsync(
    uint32_t fs_id, uint64_t inode_id, const InodeAttr& attr,
    MetaServerClientDone* done, DataIndices&& indices) {
  UpdateInodeRequest request;
  FillInodeAttr(fs_id, inode_id, attr, /*nlink=*/false, &request);
  FillDataIndices(std::move(indices), &request);
  UpdateInodeAsync(request, done);
}

bool MetaServerClientImpl::ParseS3MetaStreamBuffer(butil::IOBuf* buffer,
                                                   uint64_t* chunk_index,
                                                   S3ChunkInfoList* list) {
  butil::IOBuf out;
  std::string delim = ":";
  if (buffer->cut_until(&out, delim) != 0) {
    LOG(ERROR) << "invalid stream buffer: no delimiter";
    return false;
  } else if (!StringToUll(out.to_string(), chunk_index)) {
    LOG(ERROR) << "invalid stream buffer: invalid chunkIndex";
    return false;
  } else if (!brpc::ParsePbFromIOBuf(list, *buffer)) {
    LOG(ERROR) << "invalid stream buffer: invalid s3chunkinfo list";
    return false;
  }

  return true;
}

bool MetaServerClientImpl::HandleS3MetaStreamBuffer(butil::IOBuf* buffer,
                                                    S3ChunkInfoMap* out) {
  uint64_t chunk_index;
  S3ChunkInfoList list;
  if (!ParseS3MetaStreamBuffer(buffer, &chunk_index, &list)) {
    return false;
  }

  auto merge = [](S3ChunkInfoList* from, S3ChunkInfoList* to) {
    for (int i = 0; i < from->s3chunks_size(); i++) {
      auto* chunkinfo = to->add_s3chunks();
      *chunkinfo = std::move(*from->mutable_s3chunks(i));
    }
  };

  auto iter = out->find(chunk_index);
  if (iter == out->end()) {
    out->insert({chunk_index, std::move(list)});
  } else {
    merge(&list, &iter->second);
  }
  return true;
}

MetaStatusCode MetaServerClientImpl::GetOrModifyS3ChunkInfo(
    uint32_t fs_id, uint64_t inode_id,
    const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3_chunk_infos,
    bool return_s3_chunk_info_map,
    google::protobuf::Map<uint64_t, S3ChunkInfoList>* out, bool internal) {
  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat(
          "get_or_modify_s3_chunkinfo (%d,%d,%d,%d) (%d) %d %s", fs_id, poolID,
          copysetID, partitionID, inode_id, s3_chunk_infos.size(),
          is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(
        &is_ok,
        {&metric_.appendS3ChunkInfo, &metric_.getTxnOperation,
         &metric_.getAllOperation},
        start);

    GetOrModifyS3ChunkInfoRequest request;
    GetOrModifyS3ChunkInfoResponse response;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_inodeid(inode_id);
    request.set_returns3chunkinfomap(return_s3_chunk_info_map);
    *(request.mutable_s3chunkinfoadd()) = s3_chunk_infos;
    request.set_supportstreaming(true);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);

    // stream connection for s3chunkinfo list
    std::shared_ptr<StreamConnection> connection;
    auto defer = absl::MakeCleanup([&]() {
      if (connection != nullptr) {
        streamClient_.Close(connection);
      }
    });
    auto receive_callback = [&](butil::IOBuf* buffer) {
      return HandleS3MetaStreamBuffer(buffer, out);
    };
    if (return_s3_chunk_info_map) {
      StreamOptions options(opt_.rpcStreamIdleTimeoutMS);
      connection = streamClient_.Connect(cntl, receive_callback, options);
      if (nullptr == connection) {
        LOG(ERROR) << "Stream connect failed in client-side";
        return MetaStatusCode::RPC_STREAM_ERROR;
      }
    }

    stub.GetOrModifyS3ChunkInfo(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "inodeId=" << inode_id
                   << " GetOrModifyS3ChunkInfo Failed, errorcode: "
                   << cntl->ErrorCode()
                   << ", error content: " << cntl->ErrorText()
                   << ", log id: " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "inodeId=" << inode_id
                   << " GetOrModifyS3ChunkInfo fail fsId: " << fs_id
                   << ", errorcode: " << ret
                   << ", errmsg: " << MetaStatusCode_Name(ret);
      return ret;
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
      if (return_s3_chunk_info_map) {
        CHECK(out != nullptr) << "out ptr should be set.";
        auto status = connection->WaitAllDataReceived();
        if (status != StreamStatus::STREAM_OK) {
          LOG(ERROR) << "Receive stream data failed" << ", status=" << status;
          return MetaStatusCode::RPC_STREAM_ERROR;
        }
      }
    } else {
      LOG(WARNING) << "inodeId=" << inode_id
                   << " GetOrModifyS3ChunkInfo fsId: " << fs_id
                   << " ok, but applyIndex or inode not set in response: "
                   << response.ShortDebugString();
      return -1;
    }
    VLOG(6) << "GetOrModifyS3ChunkInfo done, request: "
            << request.ShortDebugString()
            << "response: " << response.ShortDebugString();
    return ret;
  };

  bool streaming = return_s3_chunk_info_map;
  auto task_ctx =
      std::make_shared<TaskContext>(MetaServerOpType::GetOrModifyS3ChunkInfo,
                                    task, fs_id, inode_id, streaming);
  ExcutorOpt opt;
  if (internal) {
    opt = optInternal_;
  } else {
    opt = opt_;
  }
  GetOrModifyS3ChunkInfoExcutor excutor(opt, metaCache_, channelManager_,
                                        std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

class GetOrModifyS3ChunkInfoRpcDone : public MetaServerClientRpcDoneBase {
 public:
  using MetaServerClientRpcDoneBase::MetaServerClientRpcDoneBase;

  void Run() override;

  GetOrModifyS3ChunkInfoRequest request;
  GetOrModifyS3ChunkInfoResponse response;
};

void GetOrModifyS3ChunkInfoRpcDone::Run() {
  // update metaserver operation metrics stats async
  bool is_ok = true;

  MetaAccessLogGuard log(start_time_us, [&]() {
    return absl::StrFormat(
        "async_get_or_modify_s3_chunkinfo (%d,%d,%d,%d) (%d) %d %s",
        request.fsid(), request.poolid(), request.copysetid(),
        request.partitionid(), request.inodeid(), request.s3chunkinfoadd_size(),
        is_ok ? "ok" : "failed");
  });

  MetricListGuard meta_guard(
      &is_ok,
      {&(metric_->appendS3ChunkInfo), &(metric_->getTxnOperation),
       &(metric_->getAllOperation)},
      start_time_us);

  std::unique_ptr<GetOrModifyS3ChunkInfoRpcDone> self_guard(this);
  brpc::ClosureGuard done_guard(done_);
  auto task_ctx = done_->GetTaskExcutor()->GetTaskCxt();
  auto& cntl = task_ctx->cntl_;
  auto meta_cache = done_->GetTaskExcutor()->GetMetaCache();
  if (cntl.Failed()) {
    LOG(WARNING) << "GetOrModifyS3ChunkInfo Failed, errorcode: "
                 << cntl.ErrorCode() << ", error content: " << cntl.ErrorText()
                 << ", log id: " << cntl.log_id();
    is_ok = false;
    done_->SetRetCode(-cntl.ErrorCode());
    return;
  }

  MetaStatusCode ret = response.statuscode();
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "GetOrModifyS3ChunkInfo, inodeId=" << task_ctx->inodeID
                 << ", fsId: " << task_ctx->fsID << ", errorcode: " << ret
                 << ", errmsg: " << MetaStatusCode_Name(ret);
    done_->SetRetCode(ret);
    return;
  } else if (response.has_appliedindex()) {
    meta_cache->UpdateApplyIndex(task_ctx->target.groupID,
                                 response.appliedindex());
  } else {
    LOG(WARNING) << "GetOrModifyS3ChunkInfo,  inodeId=" << task_ctx->inodeID
                 << ", fsId: " << task_ctx->fsID
                 << "ok, but applyIndex or inode not set in response: "
                 << response.ShortDebugString();
    done_->SetRetCode(-1);
    return;
  }

  VLOG(12) << "GetOrModifyS3ChunkInfo done, response: "
           << response.ShortDebugString();
  done_->SetRetCode(ret);
}

void MetaServerClientImpl::GetOrModifyS3ChunkInfoAsync(
    uint32_t fs_id, uint64_t inode_id,
    const google::protobuf::Map<uint64_t, S3ChunkInfoList>& s3_chunk_infos,
    MetaServerClientDone* done) {
  auto task = AsyncRPCTask {
    (void)txId;
    (void)applyIndex;

    auto* rpc_done =
        new GetOrModifyS3ChunkInfoRpcDone(taskExecutorDone, &metric_);

    GetOrModifyS3ChunkInfoRequest& request = rpc_done->request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_inodeid(inode_id);
    request.set_returns3chunkinfomap(false);
    *(request.mutable_s3chunkinfoadd()) = s3_chunk_infos;

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.GetOrModifyS3ChunkInfo(cntl, &request, &rpc_done->response, rpc_done);
    return MetaStatusCode::OK;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::GetOrModifyS3ChunkInfo, task, fs_id, inode_id);
  auto excutor = std::make_shared<GetOrModifyS3ChunkInfoExcutor>(
      opt_, metaCache_, channelManager_, std::move(task_ctx));
  TaskExecutorDone* task_done = new TaskExecutorDone(excutor, done);
  excutor->DoAsyncRPCTask(task_done);
}

MetaStatusCode MetaServerClientImpl::CreateInode(const InodeParam& param,
                                                 Inode* out) {
  auto fs_id = param.fsId;

  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("create_inode (%d,%d,%d,%d) (%d) %s", fs_id,
                             poolID, copysetID, partitionID, param.parent,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.createInode, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    pb::metaserver::CreateInodeResponse response;
    pb::metaserver::CreateInodeRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(param.fsId);
    request.set_length(param.length);
    request.set_uid(param.uid);
    request.set_gid(param.gid);
    request.set_mode(param.mode);
    request.set_type(param.type);
    request.set_rdev(param.rdev);
    request.set_symlink(param.symlink);
    request.set_parent(param.parent);
    struct timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    Time* tm = new Time();
    tm->set_sec(now.tv_sec);
    tm->set_nsec(now.tv_nsec);
    request.set_allocated_create(tm);
    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.CreateInode(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "CreateInode Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "CreateInode= param = " << param << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret)
                   << ", remote side = "
                   << butil::endpoint2str(cntl->remote_side()).c_str()
                   << ", request: " << request.ShortDebugString()
                   << ", pool: " << poolID << ", copyset: " << copysetID
                   << ", partition: " << partitionID;
    } else if (response.has_inode() && response.has_appliedindex()) {
      *out = response.inode();

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "CreateInode= param = " << param
                   << " ok, but applyIndex or inode not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "CreateInode done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(MetaServerOpType::CreateInode,
                                                task, fs_id, 0);
  CreateInodeExcutor excutor(opt_, metaCache_, channelManager_, task_ctx);
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::CreateManageInode(const InodeParam& param,
                                                       Inode* out) {
  auto fs_id = param.fsId;

  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("create_manage_inode (%d,%d,%d,%d)  %s", fs_id,
                             poolID, copysetID, partitionID,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.createInode, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    pb::metaserver::CreateManageInodeResponse response;
    pb::metaserver::CreateManageInodeRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(param.fsId);
    request.set_uid(param.uid);
    request.set_gid(param.gid);
    request.set_mode(param.mode);
    assert(param.manageType !=
           pb::metaserver::ManageInodeType::TYPE_NOT_MANAGE);
    request.set_managetype(param.manageType);

    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.CreateManageInode(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "CreateManageInode Failed, errorcode = "
                   << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "CreateManageInode= param = " << param
                   << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret)
                   << ", remote side = "
                   << butil::endpoint2str(cntl->remote_side()).c_str()
                   << ", request: " << request.ShortDebugString()
                   << ", pool: " << poolID << ", copyset: " << copysetID
                   << ", partition: " << partitionID;
    } else if (response.has_inode() && response.has_appliedindex()) {
      *out = response.inode();

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "CreateManageInode= param = " << param
                   << " ok, but applyIndex or inode not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "CreateManageInode done, request: "
             << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(
      MetaServerOpType::CreateManageInode, task, fs_id, 0);
  CreateInodeExcutor excutor(opt_, metaCache_, channelManager_, task_ctx);
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::DeleteInode(uint32_t fs_id,
                                                 uint64_t inodeid) {
  auto task = RPCTask {
    (void)txId;
    (void)applyIndex;
    (void)taskExecutorDone;

    // update metaserver operation metrics stats
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;

    MetaAccessLogGuard log(start, [&]() {
      return absl::StrFormat("create_inode (%d,%d,%d,%d) (%d) %s", fs_id,
                             poolID, copysetID, partitionID, inodeid,
                             is_ok ? "ok" : "failed");
    });

    MetricListGuard meta_guard(&is_ok,
                               {&metric_.deleteInode, &metric_.getTxnOperation,
                                &metric_.getAllOperation},
                               start);

    pb::metaserver::DeleteInodeResponse response;
    pb::metaserver::DeleteInodeRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_partitionid(partitionID);
    request.set_fsid(fs_id);
    request.set_inodeid(inodeid);
    dingofs::pb::metaserver::MetaServerService_Stub stub(channel);
    stub.DeleteInode(cntl, &request, &response, nullptr);

    if (cntl->Failed()) {
      LOG(WARNING) << "DeleteInode Failed, errorcode = " << cntl->ErrorCode()
                   << ", error content:" << cntl->ErrorText()
                   << ", log id = " << cntl->log_id();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "DeleteInode= fsid = " << fs_id << ", inodeId=" << inodeid
                   << ", errcode = " << ret
                   << ", errmsg = " << MetaStatusCode_Name(ret);
    } else if (response.has_appliedindex()) {
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      LOG(WARNING) << "DeleteInode= fsid = " << fs_id << ", inodeId=" << inodeid
                   << " ok, but applyIndex not set in response:"
                   << response.ShortDebugString();
      return -1;
    }

    VLOG(12) << "DeleteInode done, request: " << request.ShortDebugString()
             << "response: " << response.ShortDebugString();
    return ret;
  };

  auto task_ctx = std::make_shared<TaskContext>(MetaServerOpType::DeleteInode,
                                                task, fs_id, inodeid);
  DeleteInodeExcutor excutor(opt_, metaCache_, channelManager_,
                             std::move(task_ctx));
  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

#define SET_COMMON_FIELDS                 \
  do {                                    \
    request.set_poolid(poolID);           \
    request.set_copysetid(copysetID);     \
    request.set_partitionid(partitionID); \
    request.set_fsid(fsId);               \
    request.set_inodeid(inodeId);         \
  } while (0)

MetaStatusCode MetaServerClientImpl::GetInodeAttr(uint32_t fs_id,
                                                  uint64_t inodeid,
                                                  InodeAttr* attr) {
  std::set<uint64_t> inode_ids;
  inode_ids.insert(inodeid);
  std::list<InodeAttr> attrs;
  MetaStatusCode ret = BatchGetInodeAttr(fs_id, inode_ids, &attrs);
  if (ret != MetaStatusCode::OK) {
    LOG(WARNING) << "inodeId=" << inodeid
                 << " GetInodeAttr failed, fsid: " << fs_id;
    return ret;
  }

  if (attrs.size() != 1) {
    LOG(ERROR) << "inodeId=" << inodeid
               << " GetInodeAttr return attrs.size() != 1, which is "
               << attrs.size();
    return MetaStatusCode::UNKNOWN_ERROR;
  }

  *attr = attrs.front();
  return MetaStatusCode::OK;
}

MetaStatusCode MetaServerClientImpl::GetFsQuota(uint32_t fs_id, Quota& quota) {
  auto task = RPCTask {
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;
    MetricListGuard metric_guard(
        &is_ok, {&metric_.get_fs_quota, &metric_.getAllOperation}, start);

    GetFsQuotaRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_fsid(fs_id);

    GetFsQuotaResponse response;
    MetaServerService_Stub stub(channel);
    stub.GetFsQuota(cntl, &request, &response, nullptr);

    std::string log_prefix =
        fmt::format("GetFsQuota remote side: {}",
                    butil::endpoint2str(cntl->remote_side()).c_str());

    if (cntl->Failed()) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content: " << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    VLOG(12) << log_prefix << ", request: " << request.ShortDebugString()
             << ", response: " << response.ShortDebugString();

    MetaStatusCode ret = response.statuscode();
    if (ret == MetaStatusCode::OK) {
      CHECK(response.has_appliedindex())
          << "applied index not set in response:" << response.ShortDebugString()
          << ", requst:" << request.ShortDebugString();
      quota = response.quota();
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else {
      if (ret == MetaStatusCode::NOT_FOUND) {
        LOG(FATAL) << "Failed " << log_prefix
                   << ", request: " << request.ShortDebugString()
                   << ", response: " << response.ShortDebugString();
      } else {
        LOG(WARNING) << "Failed " << log_prefix
                     << ", errmsg = " << MetaStatusCode_Name(ret)
                     << ", request: " << request.ShortDebugString()
                     << ", response: " << response.ShortDebugString();
      }
    }

    return ret;
  };

  auto task_context = std::make_shared<TaskContext>(
      MetaServerOpType::GetFsQuota, task, fs_id, ROOTINODEID);

  TaskExecutor excutor(opt_, metaCache_, channelManager_,
                       std::move(task_context));

  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::FlushFsUsage(uint32_t fs_id,
                                                  const Usage& usage,
                                                  Quota& new_quota) {
  auto task = RPCTask {
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;
    MetricListGuard metric_guard(
        &is_ok, {&metric_.flush_fs_usage, &metric_.getAllOperation}, start);

    FlushFsUsageRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_fsid(fs_id);
    request.mutable_usage()->CopyFrom(usage);

    FlushFsUsageResponse response;
    MetaServerService_Stub stub(channel);
    stub.FlushFsUsage(cntl, &request, &response, nullptr);

    std::string log_prefix =
        fmt::format("FlushFsUsage remote side: {}",
                    butil::endpoint2str(cntl->remote_side()).c_str());

    if (cntl->Failed()) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content: " << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    VLOG(12) << log_prefix << ", request: " << request.ShortDebugString()
             << ", response: " << response.ShortDebugString();

    MetaStatusCode ret = response.statuscode();
    if (ret == MetaStatusCode::OK) {
      CHECK(response.has_appliedindex())
          << "applied index not set in response:" << response.ShortDebugString()
          << ", requst:" << request.ShortDebugString();

      new_quota = response.quota();
      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    } else if (ret == MetaStatusCode::NOT_FOUND) {
      LOG(INFO) << "Failed " << log_prefix
                << ", errmsg = " << MetaStatusCode_Name(ret)
                << ", request: " << request.ShortDebugString();
    } else {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errmsg = " << MetaStatusCode_Name(ret)
                   << ", request: " << request.ShortDebugString()
                   << ", response: " << response.ShortDebugString();
    }

    return ret;
  };

  auto task_context = std::make_shared<TaskContext>(
      MetaServerOpType::FlushFsUsage, task, fs_id, ROOTINODEID);

  TaskExecutor excutor(opt_, metaCache_, channelManager_,
                       std::move(task_context));

  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::LoadDirQuotas(
    uint32_t fs_id, std::unordered_map<uint64_t, Quota>& dir_quotas) {
  auto task = RPCTask {
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;
    MetricListGuard metric_guard(
        &is_ok, {&metric_.load_dir_quotas, &metric_.getAllOperation}, start);

    LoadDirQuotasRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_appliedindex(applyIndex);
    request.set_fsid(fs_id);

    LoadDirQuotasResponse response;
    MetaServerService_Stub stub(channel);
    stub.LoadDirQuotas(cntl, &request, &response, nullptr);

    std::string log_prefix =
        fmt::format("LoadDirQuotas remote side: {}",
                    butil::endpoint2str(cntl->remote_side()).c_str());

    if (cntl->Failed()) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content: " << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request: " << request.ShortDebugString();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    VLOG(12) << log_prefix << ", request: " << request.ShortDebugString()
             << ", response quota size: " << response.quotas_size();

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errmsg = " << MetaStatusCode_Name(ret)
                   << ", request: " << request.ShortDebugString()
                   << ", response: " << response.ShortDebugString();
    } else {
      CHECK(response.has_appliedindex())
          << "applied index not set in response" << response.ShortDebugString()
          << ", requst:" << request.ShortDebugString();

      for (const auto& dir_quota_iter : response.quotas()) {
        VLOG(12) << log_prefix << " response, inodeId=" << dir_quota_iter.first
                 << ", quota: " << dir_quota_iter.second.ShortDebugString();

        uint64_t ino = dir_quota_iter.first;
        const auto& dir_quota = dir_quota_iter.second;
        if (dir_quota.maxbytes() == 0 && dir_quota.maxinodes() == 0) {
          LOG(INFO) << log_prefix << " invalid quota, inodeId=" << ino
                    << ", quota: " << dir_quota.ShortDebugString();
        } else {
          dir_quotas[ino] = dir_quota;
        }
      }

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    }

    return ret;
  };

  auto task_context = std::make_shared<TaskContext>(
      MetaServerOpType::FlushFsUsage, task, fs_id, ROOTINODEID);

  TaskExecutor excutor(opt_, metaCache_, channelManager_,
                       std::move(task_context));

  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

MetaStatusCode MetaServerClientImpl::FlushDirUsages(
    uint32_t fs_id, std::unordered_map<uint64_t, Usage>& dir_usages) {
  CHECK_GT(dir_usages.size(), 0);

  auto task = RPCTask {
    auto start = butil::cpuwide_time_us();
    bool is_ok = true;
    MetricListGuard metric_guard(
        &is_ok, {&metric_.flush_dir_usages, &metric_.getAllOperation}, start);

    FlushDirUsagesRequest request;
    request.set_poolid(poolID);
    request.set_copysetid(copysetID);
    request.set_fsid(fs_id);
    auto* mutable_usages = request.mutable_usages();
    for (const auto& dir_usage_iter : dir_usages) {
      VLOG(12) << "FlushDirUsages inodeId=" << dir_usage_iter.first
               << ", usage: " << dir_usage_iter.second.ShortDebugString();
      CHECK(mutable_usages->emplace(dir_usage_iter.first, dir_usage_iter.second)
                .second)
          << "duplicate quota inodeId=" << dir_usage_iter.first;
    }

    CHECK_GT(request.usages_size(), 0);

    FlushDirUsagesResponse response;
    MetaServerService_Stub stub(channel);
    stub.FlushDirUsages(cntl, &request, &response, nullptr);

    std::string log_prefix =
        fmt::format("FlushDirUsages remote side: {}",
                    butil::endpoint2str(cntl->remote_side()).c_str());

    if (cntl->Failed()) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errorcode = " << cntl->ErrorCode()
                   << ", error content: " << cntl->ErrorText()
                   << ", log id = " << cntl->log_id()
                   << ", request usage size: " << request.usages_size();
      is_ok = false;
      return -cntl->ErrorCode();
    }

    VLOG(12) << log_prefix << ", request usage size:" << request.usages_size()
             << ", response: " << response.ShortDebugString();

    MetaStatusCode ret = response.statuscode();
    if (ret != MetaStatusCode::OK) {
      LOG(WARNING) << "Failed " << log_prefix
                   << ", errmsg = " << MetaStatusCode_Name(ret)
                   << ", request usage size: " << request.usages_size()
                   << ", response: " << response.ShortDebugString();
    } else {
      CHECK(response.has_appliedindex())
          << "applied index not set in response" << response.ShortDebugString()
          << ", request usage size: " << request.usages_size();

      metaCache_->UpdateApplyIndex(CopysetGroupID(poolID, copysetID),
                                   response.appliedindex());
    }

    return ret;
  };

  auto task_context = std::make_shared<TaskContext>(
      MetaServerOpType::FlushFsUsage, task, fs_id, ROOTINODEID);

  TaskExecutor excutor(opt_, metaCache_, channelManager_,
                       std::move(task_context));

  return ConvertToMetaStatusCode(excutor.DoRPCTask());
}

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs
