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
 * Created Date: Mon Sept 4 2021
 * Author: lixiaocui
 */

#ifndef DINGOFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_
#define DINGOFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <bthread/condition_variable.h>

#include <memory>
#include <string>
#include <vector>

#include "dingofs/cli2.pb.h"
#include "stub/common/common.h"
#include "stub/common/metacache_struct.h"

namespace dingofs {
namespace stub {
namespace rpcclient {

using PeerInfoList = std::vector<common::CopysetPeerInfo<common::MetaserverID>>;
using Task2 = std::function<void(brpc::Channel* channel)>;

class GetLeaderTaskExecutor;
struct Cli2TaskContext {
  common::LogicPoolID poolID;
  common::CopysetID copysetID;
  std::string peerAddr;

  Cli2TaskContext(const common::LogicPoolID& poolid,
                  const common::CopysetID& copysetid,
                  const std::string& peeraddr)
      : poolID(poolid), copysetID(copysetid), peerAddr(peeraddr) {}
};

class Cli2Closure : public google::protobuf::Closure {
 public:
  Cli2Closure() = default;
  explicit Cli2Closure(const Cli2TaskContext& context,
                       std::shared_ptr<GetLeaderTaskExecutor> taskexcutor)
      : taskContext(context), excutor(taskexcutor) {}

  void Run() override;

  Cli2TaskContext taskContext;
  std::shared_ptr<GetLeaderTaskExecutor> excutor;

  dingofs::pb::metaserver::copyset::GetLeaderResponse2 response;
  brpc::Controller cntl;
};

struct Cli2ClientImplOption {
  uint32_t rpcTimeoutMs;

  explicit Cli2ClientImplOption(uint32_t rpcTimeoutMs = 500)
      : rpcTimeoutMs(rpcTimeoutMs) {}
};

class Cli2Client {
 public:
  Cli2Client() {}
  virtual ~Cli2Client() {}

  virtual bool GetLeader(const common::LogicPoolID& poolID,
                         const common::CopysetID& copysetID,
                         const PeerInfoList& peerInfoList,
                         int16_t currentLeaderIndex, common::PeerAddr* peerAddr,
                         common::MetaserverID* metaServerID) = 0;
};

class GetLeaderTaskExecutor {
 public:
  GetLeaderTaskExecutor() : finish_(false), success_(false) {}

  bool DoRPCTaskAndWait(const Task2& task, const std::string& peerAddr);

  void NotifyRpcFinish(bool success);

 private:
  bthread::ConditionVariable finishCv_;
  bthread::Mutex finishMtx_;

  bool finish_;
  bool success_;
};

class Cli2ClientImpl : public Cli2Client {
 public:
  Cli2ClientImpl() = default;
  explicit Cli2ClientImpl(const Cli2ClientImplOption& opt) : opt_(opt) {}

  bool GetLeader(const common::LogicPoolID& pooID,
                 const common::CopysetID& copysetID,
                 const PeerInfoList& peerInfoList, int16_t currentLeaderIndex,
                 common::PeerAddr* peerAddr,
                 common::MetaserverID* metaServerID) override;

 private:
  bool DoGetLeader(Cli2Closure* done, common::PeerAddr* peerAddr,
                   common::MetaserverID* metaServerID);

  Cli2ClientImplOption opt_;
};

}  // namespace rpcclient
}  // namespace stub
}  // namespace dingofs
#endif  // DINGOFS_SRC_CLIENT_RPCCLIENT_CLI2_CLIENT_H_
