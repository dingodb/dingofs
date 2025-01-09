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
 * Created Date: Thur Sept 2 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_

#include <brpc/channel.h>
#include <brpc/controller.h>

#include <memory>
#include <string>
#include <utility>

#include "stub/common/common.h"
#include "stub/rpcclient/channel_manager.h"
#include "stub/rpcclient/metacache.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

class TaskExecutorDone;

pb::metaserver::MetaStatusCode ConvertToMetaStatusCode(int retcode);

class TaskContext {
 public:
  using RpcFunc = std::function<int(
      common::LogicPoolID poolID, common::CopysetID copysetID,
      common::PartitionID partitionID, uint64_t txId, uint64_t applyIndex,
      brpc::Channel* channel, brpc::Controller* cntl, TaskExecutorDone* done)>;

  TaskContext() = default;
  TaskContext(common::MetaServerOpType type, RpcFunc func, uint32_t fsid = 0,
              uint64_t inodeid = 0, bool streaming = false,
              bool refreshTxId = false)
      : optype(type),
        rpctask(std::move(func)),
        fsID(fsid),
        inodeID(inodeid),
        streaming(streaming),
        refreshTxId(refreshTxId) {}

  std::string TaskContextStr() {
    std::ostringstream oss;
    oss << "{" << optype << ",fsid=" << fsID << ",inodeid=" << inodeID
        << ",streaming=" << streaming << ",refreshTxId=" << refreshTxId << "}";
    return oss.str();
  }

  uint64_t rpcTimeoutMs;
  common::MetaServerOpType optype;
  RpcFunc rpctask = nullptr;
  uint32_t fsID = 0;
  // inode used to locate replacement of dentry or inode. for CreateDentry
  // inodeid,`task_->inodeID` is parentinodeID
  uint64_t inodeID = 0;

  CopysetTarget target;
  uint64_t applyIndex = 0;

  uint64_t retryTimes = 0;
  bool suspend = false;
  bool retryDirectly = false;

  // whether need stream
  bool streaming = false;

  bool refreshTxId = false;

  brpc::Controller cntl_;
};

class TaskExecutor {
 public:
  TaskExecutor() {}
  TaskExecutor(const common::ExcutorOpt& opt,
               const std::shared_ptr<MetaCache>& metaCache,
               const std::shared_ptr<ChannelManager<common::MetaserverID>>&
                   channelManager,
               std::shared_ptr<TaskContext> task)
      : metaCache_(metaCache),
        channelManager_(channelManager),
        task_(std::move(task)),
        opt_(opt) {
    SetRetryParam();
  }

  int DoRPCTask();
  void DoAsyncRPCTask(TaskExecutorDone* done);
  int DoRPCTaskInner(TaskExecutorDone* done);

  bool OnReturn(int retCode);
  void PreProcessBeforeRetry(int retCode);

  std::shared_ptr<TaskContext> GetTaskCxt() const { return task_; }

  std::shared_ptr<MetaCache> GetMetaCache() const { return metaCache_; }

 protected:
  // prepare resource and excute task
  bool NeedRetry();
  int ExcuteTask(brpc::Channel* channel, TaskExecutorDone* done);
  virtual bool GetTarget();
  void UpdateApplyIndex(const common::LogicPoolID& poolID,
                        const common::CopysetID& copysetId,
                        uint64_t applyIndex);

  // handle a returned rpc
  void OnSuccess();
  void OnReDirected();
  void OnCopysetNotExist();
  bool OnPartitionNotExist();
  void OnPartitionAllocIDFail();

  // retry policy
  void RefreshLeader();
  uint64_t OverLoadBackOff();
  uint64_t TimeoutBackOff();
  void SetRetryParam();

 private:
  bool HasValidTarget() const;

  void ResetChannelIfNotHealth();

 protected:
  std::shared_ptr<MetaCache> metaCache_;
  std::shared_ptr<ChannelManager<common::MetaserverID>> channelManager_;
  std::shared_ptr<TaskContext> task_;

  common::ExcutorOpt opt_;
  uint64_t maxOverloadPow_;
  uint64_t maxTimeoutPow_;
};

class MetaServerClientDone : public google::protobuf::Closure {
 public:
  MetaServerClientDone() {}
  ~MetaServerClientDone() {}

  void SetMetaStatusCode(pb::metaserver::MetaStatusCode code) { code_ = code; }

  pb::metaserver::MetaStatusCode GetStatusCode() const { return code_; }

 private:
  pb::metaserver::MetaStatusCode code_;
};

class BatchGetInodeAttrDone : public MetaServerClientDone {
 public:
  BatchGetInodeAttrDone() {}
  ~BatchGetInodeAttrDone() {}

  void SetInodeAttrs(
      const google::protobuf::RepeatedPtrField<pb::metaserver::InodeAttr>&
          inodeAttrs) {
    inodeAttrs_ = inodeAttrs;
  }

  const google::protobuf::RepeatedPtrField<pb::metaserver::InodeAttr>&
  GetInodeAttrs() const {
    return inodeAttrs_;
  }

 private:
  google::protobuf::RepeatedPtrField<pb::metaserver::InodeAttr> inodeAttrs_;
};

class TaskExecutorDone : public google::protobuf::Closure {
 public:
  TaskExecutorDone(const std::shared_ptr<TaskExecutor>& excutor,
                   MetaServerClientDone* done)
      : excutor_(excutor), done_(done) {}

  ~TaskExecutorDone() override = default;

  void Run() override;

  void SetRetCode(int code) { code_ = code; }

  int GetRetCode() const { return code_; }

  std::shared_ptr<TaskExecutor> GetTaskExcutor() const { return excutor_; }

  MetaServerClientDone* GetDone() { return done_; }

 private:
  friend class TaskExecutor;

  std::shared_ptr<TaskExecutor> excutor_;
  MetaServerClientDone* done_;
  int code_;
};

class BatchGetInodeAttrTaskExecutorDone : public TaskExecutorDone {
 public:
  using TaskExecutorDone::TaskExecutorDone;

  void SetInodeAttrs(
      const google::protobuf::RepeatedPtrField<pb::metaserver::InodeAttr>&
          inodeAttrs) {
    dynamic_cast<BatchGetInodeAttrDone*>(GetDone())->SetInodeAttrs(inodeAttrs);
  }

  const google::protobuf::RepeatedPtrField<pb::metaserver::InodeAttr>&
  GetInodeAttrs() {
    return dynamic_cast<BatchGetInodeAttrDone*>(GetDone())->GetInodeAttrs();
  }
};

class CreateInodeExcutor : public TaskExecutor {
 public:
  explicit CreateInodeExcutor(
      const common::ExcutorOpt& opt,
      const std::shared_ptr<MetaCache>& metaCache,
      const std::shared_ptr<ChannelManager<common::MetaserverID>>&
          channelManager,
      const std::shared_ptr<TaskContext>& task)
      : TaskExecutor(opt, metaCache, channelManager, task) {}

 protected:
  bool GetTarget() override;
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs

#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_TASK_EXCUTOR_H_
