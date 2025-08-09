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
 * Created Date: 2021-05-19
 * Author: chenwei
 */

#ifndef DINGOFS_SRC_METASERVER_METASERVER_H_
#define DINGOFS_SRC_METASERVER_METASERVER_H_

#include <brpc/server.h>

#include <memory>
#include <string>

#include "fs/local_filesystem.h"
#include "metaserver/copyset/config.h"
#include "metaserver/copyset/copyset_service.h"
#include "metaserver/copyset/raft_cli_service2.h"
#include "metaserver/heartbeat.h"
#include "metaserver/inflight_throttle.h"
#include "metaserver/metaserver_service.h"
#include "metaserver/partition_clean_manager.h"
#include "metaserver/recycle_manager.h"
#include "metaserver/register.h"
#include "metaserver/resource_statistic.h"
#include "stub/rpcclient/base_client.h"
#include "stub/rpcclient/mds_client.h"
#include "stub/rpcclient/metaserver_client.h"
#include "utils/configuration.h"

namespace dingofs {
namespace metaserver {

struct MetaserverOptions {
  std::string ip;
  int port;
  std::string externalIp;
  int externalPort;
  int bthreadWorkerCount = -1;
  bool enableExternalServer;
};

class Metaserver {
 public:
  void InitOptions(std::shared_ptr<utils::Configuration> conf);
  void Init();
  void Run();
  void Stop();

 private:
  void InitStorage();
  void InitCopysetNodeOptions();
  void InitCopysetNodeManager();
  void InitLocalFileSystem();
  void InitInflightThrottle();
  void InitHeartbeatOptions();
  void InitResourceCollector();
  void InitHeartbeat();
  void InitMetaClient();
  void InitRegisterOptions();
  void InitBRaftFlags(const std::shared_ptr<utils::Configuration>& conf);
  void InitPartitionOptionFromConf(PartitionCleanOption* partion_clean_option);
  void InitRecycleManagerOption(RecycleManagerOption* recycleManagerOption);
  void GetMetaserverDataByLoadOrRegister();
  int PersistMetaserverMeta(std::string path,
                            pb::metaserver::MetaServerMetadata* metadata);
  int LoadMetaserverMeta(const std::string& metaFilePath,
                         pb::metaserver::MetaServerMetadata* metadata);
  int LoadDataFromLocalFile(std::shared_ptr<fs::LocalFileSystem> fs,
                            const std::string& localPath, std::string* data);
  int PersistDataToLocalFile(std::shared_ptr<fs::LocalFileSystem> fs,
                             const std::string& localPath,
                             const std::string& data);

 private:
  // metaserver configuration items
  std::shared_ptr<utils::Configuration> conf_;
  // initialized or not
  bool inited_ = false;
  // running as the main MDS or not
  bool running_ = false;

  std::shared_ptr<stub::rpcclient::MdsClient> mdsClient_;
  std::shared_ptr<stub::rpcclient::MetaServerClient> metaClient_;
  stub::rpcclient::MDSBaseClient* mdsBase_;
  stub::common::MdsOption mdsOptions_;
  MetaserverOptions options_;
  pb::metaserver::MetaServerMetadata metadata_;
  std::string metaFilePath_;

  std::unique_ptr<brpc::Server> server_;
  std::unique_ptr<brpc::Server> externalServer_;

  std::unique_ptr<MetaServerServiceImpl> metaService_;
  std::unique_ptr<copyset::CopysetServiceImpl> copysetService_;
  std::unique_ptr<copyset::RaftCliService2> raftCliService2_;

  HeartbeatOptions heartbeatOptions_;
  Heartbeat heartbeat_;

  std::unique_ptr<ResourceCollector> resourceCollector_;

  copyset::CopysetNodeOptions copysetNodeOptions_;
  copyset::CopysetNodeManager* copysetNodeManager_;

  RegisterOptions registerOptions_;

  std::unique_ptr<InflightThrottle> inflightThrottle_;
  std::shared_ptr<dingofs::fs::LocalFileSystem> localFileSystem_;
};
}  // namespace metaserver
}  // namespace dingofs

#endif  // DINGOFS_SRC_METASERVER_METASERVER_H_
