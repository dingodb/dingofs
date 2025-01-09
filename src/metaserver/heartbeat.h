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
 * Created Date: 2021-09-12
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_HEARTBEAT_H_
#define DINGOFS_SRC_METASERVER_HEARTBEAT_H_

#include <braft/node.h>  // NodeImpl
#include <braft/node_manager.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "dingofs/heartbeat.pb.h"
#include "metaserver/common/types.h"
#include "metaserver/copyset/copyset_node_manager.h"
#include "utils/wait_interval.h"

namespace dingofs {
namespace metaserver {

class ResourceCollector;

/**
 * heartbeat subsystem option
 */
struct HeartbeatOptions {
  MetaServerID metaserverId;
  std::string metaserverToken;
  std::string storeUri;
  std::string mdsListenAddr;
  std::string ip;
  uint32_t port;
  uint32_t intervalSec;
  uint32_t timeout;
  copyset::CopysetNodeManager* copysetNodeManager;
  ResourceCollector* resourceCollector;
  std::shared_ptr<fs::LocalFileSystem> fs;
};

class HeartbeatTaskExecutor;

/**
 * heartbeat subsystem
 */
class Heartbeat {
 public:
  Heartbeat() = default;
  ~Heartbeat() = default;

  /**
   * @brief init heartbeat subsystem
   * @param[in] options
   * @return 0:success; not 0: fail
   */
  int Init(const HeartbeatOptions& options);

  /**
   * @brief clean heartbeat subsystem
   * @return 0:success; not 0: fail
   */
  int Fini();

  /**
   * @brief run heartbeat subsystem
   * @return 0:success; not 0: fail
   */
  int Run();

 private:
  /**
   * @brief stop heartbeat subsystem
   * @return 0:success; not 0: fail
   */
  int Stop();

  /*
   * heartbeat work thread
   */
  void HeartbeatWorker();

  void BuildCopysetInfo(pb::mds::heartbeat::CopySetInfo* info,
                        copyset::CopysetNode* copyset);

  int BuildRequest(pb::mds::heartbeat::MetaServerHeartbeatRequest* request);

  int SendHeartbeat(
      const pb::mds::heartbeat::MetaServerHeartbeatRequest& request,
      pb::mds::heartbeat::MetaServerHeartbeatResponse* response);

  /*
   * print HeartbeatRequest to log
   */
  void DumpHeartbeatRequest(
      const pb::mds::heartbeat::MetaServerHeartbeatRequest& request);

  /*
   * print HeartbeatResponse to log
   */
  void DumpHeartbeatResponse(
      const pb::mds::heartbeat::MetaServerHeartbeatResponse& response);

  bool GetMetaserverSpaceStatus(
      pb::mds::heartbeat::MetaServerSpaceStatus* status, uint64_t ncopysets);

  friend class HeartbeatTest;

  utils::Thread hbThread_;

  std::atomic<bool> toStop_;

  utils::WaitInterval waitInterval_;

  copyset::CopysetNodeManager* copysetMan_;

  // metaserver store path
  std::string storePath_;

  HeartbeatOptions options_;

  // MDS addr list
  std::vector<std::string> mdsEps_;

  // index of current service mds
  int inServiceIndex_;

  // MetaServer addr
  butil::EndPoint msEp_;

  // heartbeat subsystem init time, use unix time
  uint64_t startUpTime_;

  std::unique_ptr<HeartbeatTaskExecutor> taskExecutor_;
};

// execute tasks from heartbeat response
class HeartbeatTaskExecutor {
 public:
  HeartbeatTaskExecutor(copyset::CopysetNodeManager* mgr,
                        const butil::EndPoint& endpoint);

  void ExecTasks(
      const pb::mds::heartbeat::MetaServerHeartbeatResponse& response);

 private:
  void ExecOneTask(const pb::mds::heartbeat::CopySetConf& conf);

  void DoTransferLeader(copyset::CopysetNode* node,
                        const pb::mds::heartbeat::CopySetConf& conf);
  void DoAddPeer(copyset::CopysetNode* node,
                 const pb::mds::heartbeat::CopySetConf& conf);
  void DoRemovePeer(copyset::CopysetNode* node,
                    const pb::mds::heartbeat::CopySetConf& conf);
  void DoChangePeer(copyset::CopysetNode* node,
                    const pb::mds::heartbeat::CopySetConf& conf);
  void DoPurgeCopyset(PoolId poolid, CopysetId copysetid);

  bool NeedPurge(const pb::mds::heartbeat::CopySetConf& conf);

  copyset::CopysetNodeManager* copysetMgr_;
  butil::EndPoint ep_;
};

}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_HEARTBEAT_H_
